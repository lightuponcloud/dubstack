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
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	not_found -> {<<>>, Req0, []};
	RiakResponse0 ->
	    TotalBytes =
		case proplists:get_value("x-amz-meta-bytes", RiakResponse0) of
		    undefined -> undefined;
		    V -> utils:to_integer(V)
		end,
	    GUID = proplists:get_value("x-amz-meta-guid", RiakResponse0),
	    UploadId = proplists:get_value("x-amz-meta-upload-id", RiakResponse0),
	    case TotalBytes =:= undefined orelse TotalBytes > ?MAXIMUM_IMAGE_SIZE_BYTES of
		true ->
		    %% In case image object size is bigger than the limit, return empty response.
		    {<<>>, Req0, []};
		false ->
		    scale_response(Req0, BucketId, GUID, UploadId, Width, Height, CropFlag)
	    end
    end.

%%
%% Returns thumbnail as binary.
%%
%% Cached images are stored as
%% ~object/file-GUID/upload-GUID_WxH.ext, where W and H are width and height
%%
scale_response(Req0, BucketId, GUID, UploadId, Width, Height, CropFlag) ->
    %% First check if cached image exists already.
    PrefixedGUID = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
    CachedKey = UploadId ++ "_" ++ erlang:integer_to_list(Width) ++ "x" ++ erlang:integer_to_list(Height),
    PrefixedCachedKey = utils:prefixed_object_key(PrefixedGUID, CachedKey),
    case riak_api:get_object(BucketId, PrefixedCachedKey) of
	not_found ->
	    BinaryData = riak_api:get_object(BucketId, GUID, UploadId),
	    Watermark =
		case riak_api:head_object(BucketId, ?WATERMARK_OBJECT_KEY) of
		    not_found -> [];
		    WatermarkResponse ->
			WatermarkGUID = proplists:get_value("x-amz-meta-guid", WatermarkResponse),
			WatermarkUploadId = proplists:get_value("x-amz-meta-upload-id", WatermarkResponse),
			WatermarkBinaryData = riak_api:get_object(BucketId, WatermarkGUID, WatermarkUploadId),
			[{watermark, WatermarkBinaryData}]
		end,
	    Reply0 = img:scale([
		{from, BinaryData},
		{to, jpeg},
		{crop, CropFlag},
		{scale_width, Width},
		{scale_height, Height}
	    ] ++ Watermark),
	    case Reply0 of
		{error, Reason} -> js_handler:bad_request(Req0, Reason);
		_ ->
		    Options = [{acl, public_read}],
		    riak_api:put_object(BucketId, PrefixedGUID, CachedKey, Reply0, Options),
		    {Reply0, Req0, []}
	    end;
	RiakResponse ->
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
	{error, Number} -> js_handler:forbidden(Req0, Number);
	false ->
	    UserBelongsToGroup =
		case User =:= undefined orelse utils:is_public_bucket_id(BucketId) of
		    true -> undefined;
		    false -> lists:any(
			    fun(Group) -> utils:is_bucket_belongs_to_group(BucketId, TenantId, Group#group.id) end,
			    User#user.groups)
		end,
	    case UserBelongsToGroup of
		false ->
		    PUser = admin_users_handler:user_to_proplist(User),
		    js_handler:forbidden(Req0, 37, proplists:get_value(groups, PUser));
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
