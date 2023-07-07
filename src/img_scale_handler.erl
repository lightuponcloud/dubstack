%%
%% Accepts HTTP requests and scales requested images.
%%
-module(img_scale_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_scale/2, allowed_methods/2,
    is_authorized/2, forbidden/2, resource_exists/2, previously_existed/2]).

-include("general.hrl").
-include("riak.hrl").
-include("entities.hrl").
-include("media.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {[
	{{<<"image">>, <<"jpeg">>, '*'}, to_scale}
    ], Req, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

to_scale(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),
    ObjectKey = proplists:get_value(object_key, State),
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey),
    Width =
	case proplists:get_value(width, State) of
	    undefined -> ?DEFAULT_IMAGE_WIDTH;
	    W ->
		case W =< 0 of
		    true -> ?DEFAULT_IMAGE_WIDTH;
		    false -> W
		end
	end,
    Height =
	case proplists:get_value(height, State) of
	    undefined -> Width;
	    H ->
		case H =< 0 of
		    true -> ?DEFAULT_IMAGE_WIDTH;
		    false -> H
		end
	end,
    CropFlag = proplists:get_value(crop, State),
    T0 = utils:timestamp(),
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	{error, Reason} ->
	    lager:error("[img_scale_handler] head_object failed ~p/~p: ~p",
			[BucketId, PrefixedObjectKey, Reason]),
	    {<<>>, Req0, []};
	not_found -> {<<>>, Req0, []};
	Metadata0 ->
	    TotalBytes =
		case proplists:get_value("x-amz-meta-bytes", Metadata0) of
		    undefined -> undefined;
		    V -> utils:to_integer(V)
		end,
	    case TotalBytes =:= undefined orelse TotalBytes > ?MAXIMUM_IMAGE_SIZE_BYTES of
		true ->
		    %% In case image object size is bigger than the limit, return empty response.
		    {<<>>, Req0, []};
		false ->
		    scale_response(Req0, BucketId, Metadata0, Width, Height, CropFlag, T0)
	    end
    end.

%%
%% Receives stream from httpc and passes it to cowboy
%%
receive_streamed_body(RequestId0, Pid0, BucketId, NextObjectKeys0, Acc) ->
    httpc:stream_next(Pid0),
    receive
	{http, {RequestId0, stream, BinBodyPart}} ->
	    case (byte_size(Acc) + byte_size(BinBodyPart)) >= ?MAXIMUM_IMAGE_SIZE_BYTES of
		true ->
		    httpc:cancel_request(RequestId0),
		    << Acc/binary, BinBodyPart/binary >>;
		false ->
		    receive_streamed_body(RequestId0, Pid0, BucketId, NextObjectKeys0,
					  << Acc/binary, BinBodyPart/binary >>)
	    end;
	{http, {RequestId0, stream_end, _Headers0}} ->
	    case NextObjectKeys0 of
		[] -> Acc;
		[CurrentObjectKey|NextObjectKeys1] ->
		    %% stream next chunk
		    case riak_api:get_object(BucketId, CurrentObjectKey, stream) of
			not_found ->
			    lager:error("[img_scale_handler] part not found: ~p/~p", [BucketId, CurrentObjectKey]),
			    <<>>;
			{ok, RequestId1} ->
			    receive
				{http, {RequestId1, stream_start, _Headers1, Pid1}} ->
				    receive_streamed_body(RequestId1, Pid1, BucketId, NextObjectKeys1, Acc);
				{http, Msg} -> lager:error("[img_scale_handler] stream error: ~p", [Msg])
			    end
		    end
	    end;
	{http, Msg} ->
	    lager:error("[img_scale_handler] cant receive stream body: ~p", [Msg]),
	    <<>>
    end.

%%
%% Download image or first couple of megabytes of video from object storage to pepare preview
%% In case of image: I assume it cannot be bigger than MAXIMUM_IMAGE_SIZE_BYTES.
%%
get_binary_data(BucketId, Prefix, StartByte, EndByte) ->
    MaxKeys = ?FILE_MAXIMUM_SIZE div ?FILE_UPLOAD_CHUNK_SIZE,
    PartNumStart = (StartByte div ?FILE_UPLOAD_CHUNK_SIZE) + 1,
    PartNumEnd = (EndByte div ?FILE_UPLOAD_CHUNK_SIZE) + 1,
    case riak_api:list_objects(BucketId, [{max_keys, MaxKeys}, {prefix, Prefix ++ "/"}]) of
	not_found -> <<>>;
	RiakResponse0 ->
	    Contents = proplists:get_value(contents, RiakResponse0),
	    %% We take into account 'range' header, by taking all parts from specified one
	    List0 = lists:filtermap(
		fun(K) ->
		    ObjectKey1 = proplists:get_value(key, K),
		    Tokens = lists:last(string:tokens(ObjectKey1, "/")),
		    [N,_] = string:tokens(Tokens, "_"),
		    case utils:to_integer(N) of
			I when I >= PartNumStart, I =< PartNumEnd -> {true, ObjectKey1};
			_ -> false
		    end
		end, Contents),
	    List1 = lists:sort(
		fun(K1, K2) ->
		    T1 = lists:last(string:tokens(K1, "/")),
		    [N1,_] = string:tokens(T1, "_"),
		    T2 = lists:last(string:tokens(K2, "/")),
		    [N2,_] = string:tokens(T2, "_"),
		    utils:to_integer(N1) < utils:to_integer(N2)
		end, List0),
	    case List1 of
		 [] -> <<>>;
		 [PrefixedObjectKey | NextKeys] ->
		    case riak_api:get_object(BucketId, PrefixedObjectKey, stream) of
			not_found -> <<>>;
			{ok, RequestId} ->
			    receive
				{http, {RequestId, stream_start, _Headers, Pid}} ->
				    receive_streamed_body(RequestId, Pid, BucketId, NextKeys, <<>>);
				{http, Msg} ->
				    lager:error("[img_scale_handler] error starting stream: ~p", [Msg]),
				    <<>>
			    end
		    end
	    end
    end.

serve_img(Req0, BucketId, Prefix, CachedKey, Width, Height, CropFlag, T0) ->
    BinaryData = get_binary_data(BucketId, Prefix, 0, ?MAXIMUM_IMAGE_SIZE_BYTES),
    Watermark =
	case riak_api:head_object(BucketId, ?WATERMARK_OBJECT_KEY) of
	    {error, Reason1} ->
		lager:error("[img_scale_handler] head_object failed ~p/~p: ~p",
			    [BucketId, ?WATERMARK_OBJECT_KEY, Reason1]),
		[];
	    not_found -> [];
	    WatermarkResponse ->
		WatermarkGUID = proplists:get_value("x-amz-meta-guid", WatermarkResponse),
		WatermarkUploadId = proplists:get_value("x-amz-meta-upload-id", WatermarkResponse),
		case riak_api:get_object(BucketId, WatermarkGUID, WatermarkUploadId) of
		    not_found -> [];
		    {error, _} -> [];
		    WatermarkBinaryData -> [{watermark, WatermarkBinaryData}]
		end
	end,
    Reply0 = img:scale([
	{from, BinaryData},
	{to, jpeg},
	{crop, CropFlag},
	{scale_width, Width},
	{scale_height, Height}
    ] ++ Watermark),
    case Reply0 of
	{error, Reason2} -> js_handler:bad_request(Req0, Reason2);
	_ ->
	    Options = [{acl, public_read}],
	    Response = riak_api:put_object(BucketId, utils:dirname(Prefix), CachedKey, Reply0, Options),
	    case Response of
		{error, Reason3} ->
		    lager:error("[img_scale_handler] Can't put object ~p/~p/~p: ~p",
				[BucketId, utils:dirname(Prefix), CachedKey, Reason3]);
		_ -> ok
	    end,
	    T1 = utils:timestamp(),
	    Req1 = cowboy_req:reply(200, #{
		<<"elapsed-time">> => io_lib:format("~.2f", [utils:to_float(T1-T0)/1000])
	    }, Reply0, Req0),
	    {ok, Req1, []}
    end.

%%
%% Returns thumbnail as binary.
%%
%% Cached images are stored as
%% ~object/file-GUID/upload-GUID_WxH.ext, where W and H are width and height
%%
scale_response(Req0, BucketId, Metadata, Width, Height, CropFlag, T0) ->
    GUID = proplists:get_value("x-amz-meta-guid", Metadata),
    UploadId = proplists:get_value("x-amz-meta-upload-id", Metadata),
    %% Old GUID, old bucket id and upload id are needed for 
    %% determining URI of the original object, before it was copied
    OldGUID = proplists:get_value("x-amz-meta-copy-from-guid", Metadata),
    OldBucketId =
	case proplists:get_value("x-amz-meta-copy-from-bucket-id", Metadata) of
	    undefined -> BucketId;
	    B -> B
	end,
    OldUploadId =
	case proplists:get_value("x-amz-meta-copy-from-upload-id", Metadata) of
	    undefined -> UploadId;
	    UID -> UID
	end,
    {RealBucketId, RealGUID, RealUploadId, RealPrefix0} =
	case OldGUID =/= undefined andalso OldGUID =/= GUID of
	    true ->
		RealPrefix1 = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, OldGUID),
		{OldBucketId, OldGUID, OldUploadId, utils:prefixed_object_key(RealPrefix1, OldUploadId)};
	    false ->
		PrefixedGUID0 = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
		{BucketId, GUID, UploadId, utils:prefixed_object_key(PrefixedGUID0, UploadId)}
	end,
    %% First check if cached image exists already.
    PrefixedGUID1 = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, RealGUID),
    CachedKey = RealUploadId ++ "_" ++ erlang:integer_to_list(Width) ++ "x" ++ erlang:integer_to_list(Height),
    PrefixedCachedKey = utils:prefixed_object_key(PrefixedGUID1, CachedKey),
    case riak_api:get_object(RealBucketId, PrefixedCachedKey) of
	{error, Reason} ->
            %% Miss
	    lager:error("[img_scale_handler] get_object failed ~p/~p: ~p",
			[RealBucketId, PrefixedCachedKey, Reason]),
	    serve_img(Req0, RealBucketId, RealPrefix0, CachedKey, Width, Height, CropFlag, T0);
	not_found ->
            %% Miss
	    serve_img(Req0, RealBucketId, RealPrefix0, CachedKey, Width, Height, CropFlag, T0);
	RiakResponse ->
            %% Return cached image
	    {proplists:get_value(content, RiakResponse), Req0, []}
    end.

%%
%% Checks if provided token is correct ( Token is optional here ).
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, _State) ->
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    %% Extracts token from request headers and looks it up in "security" bucket
    case utils:get_token(Req0) of
	undefined ->
	    %% Check session id
	    Settings = #general_settings{},
	    SessionCookieName = Settings#general_settings.session_cookie_name,
	    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
	    case login_handler:check_session_id(SessionID0) of
		false ->
		    case utils:is_public_bucket_id(BucketId) of
			true ->
			    case utils:is_valid_bucket_id(BucketId, undefined) of
				true -> {true, Req0, [{bucket_id, BucketId}]};
				false -> js_handler:unauthorized(Req0, 27)
			    end;
			false -> js_handler:unauthorized(Req0, 28)
		    end;
		{error, Number} -> js_handler:unauthorized(Req0, Number);
		User ->
		    case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
			true -> {true, Req0, [{bucket_id, BucketId}, {user, User}]};
			false -> js_handler:unauthorized(Req0, 27)
		    end
	    end;
	Token ->
	    case login_handler:check_token(Token) of
		not_found -> js_handler:unauthorized(Req0, 28);
		expired -> js_handler:unauthorized(Req0, 28);
		User ->
		    case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
			true -> {true, Req0, [{bucket_id, BucketId}, {user, User}]};
			false -> js_handler:unauthorized(Req0, 27)
		    end
	    end
    end.

%%
%% Checks if provided token ( or access token ) is valid.
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, State) ->
    User = proplists:get_value(user, State),
    TenantId =
	case User of
	    undefined -> undefined;
	    _ -> User#user.tenant_id
	end,
    BucketId = proplists:get_value(bucket_id, State),
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix = list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, ParsedQs)),
    ObjectKey0 =
	case proplists:get_value(<<"object_key">>, ParsedQs) of
	    undefined -> {error, 8};
	    ObjectKey1 -> unicode:characters_to_list(ObjectKey1)
	end,
    Width =
	case proplists:get_value(<<"w">>, ParsedQs) of
	    undefined -> undefined;
	    W -> try utils:to_number(W) catch error:badarg -> undefined end
	end,
    Height =
	case proplists:get_value(<<"h">>, ParsedQs) of
	    undefined -> undefined;
	    H -> try utils:to_number(H) catch error:badarg -> undefined end
	end,
    IsDummyReq =
	case proplists:get_value(<<"dummy">>, ParsedQs) of
	    undefined -> false;
	    <<"1">> -> true
	end,
    %% The following flag can be used to turn off image cropping
    CropFlag =
	case proplists:get_value(<<"crop">>, ParsedQs) of
	    <<"0">> -> false;
	    _ -> true
	end,
    case lists:keyfind(error, 1, [Prefix, ObjectKey0]) of
	{error, Number} -> js_handler:forbidden(Req0, Number, stop);
	false ->
	    IsRestricted = utils:is_restricted_bucket_id(BucketId),
	    IsPublic = utils:is_public_bucket_id(BucketId),
	    UserBelongsToGroup =
		case User =:= undefined orelse IsRestricted orelse IsPublic of
		    true -> undefined;
		    false -> lists:any(
			    fun(Group) -> utils:is_bucket_belongs_to_group(BucketId, TenantId, Group#group.id) end,
			    User#user.groups)
		end,
	    case UserBelongsToGroup of
		false ->
		    PUser = admin_users_handler:user_to_proplist(User),
		    js_handler:forbidden(Req0, 37, proplists:get_value(groups, PUser), stop);
		_ ->
		    {false, Req0, [
			{bucket_id, BucketId},
			{prefix, Prefix},
			{width, Width},
			{height, Height},
			{is_dummy, IsDummyReq},
			{crop, CropFlag},
			{object_key, ObjectKey0}
		    ]}
	    end
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    {true, Req0, State}.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
