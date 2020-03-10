%%
%% Allows to download objects from Riak CS, after authentication.
%%
-module(download_handler).

-export([init/2, real_path/2]).

-include("general.hrl").
-include("riak.hrl").
-include("entities.hrl").
-include("log.hrl").


real_path(BucketId, Metadata0) ->
    GUID = proplists:get_value("x-amz-meta-guid", Metadata0),
    %% Old GUID and old bucket id are needed for URI to original object, before it was copied
    OldGUID = proplists:get_value("x-amz-meta-copy-from-guid", Metadata0),
    OldBucketId =
	case proplists:get_value("x-amz-meta-copy-from-bucket-id", Metadata0) of
	    undefined -> BucketId;
	    B -> B
	end,
    ModifiedTime0 = proplists:get_value("x-amz-meta-modified-utc", Metadata0),
    case GUID of
	undefined -> ?WARN("[download_handler] GUID undefined in metadata: ~p~n", [Metadata0]);
	_ -> ok
    end,
    RealPrefix0 = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
    RealPath0 = utils:prefixed_object_key(RealPrefix0, ModifiedTime0),
    case OldGUID =/= undefined andalso OldGUID =/= GUID of
	true ->
	    RealPrefix1 = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, OldGUID),
	    RealPath1 = utils:prefixed_object_key(RealPrefix1, ModifiedTime0),
	    {OldBucketId, RealPath1};
	false -> {OldBucketId, RealPath0}
    end.

%%
%% Check if user has access to provided bucket name,
%% then check if object exists.
%% Returns object's real path.
%%
validate_request(BucketId, User, PrefixedObjectKey) ->
    case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
	true ->
	    UserBelongsToGroup = lists:any(fun(Group) ->
		utils:is_bucket_belongs_to_group(BucketId, User#user.tenant_id, Group#group.id) end,
		User#user.groups),
	    case UserBelongsToGroup of
		false -> {error, 37};
		true ->
		    case riak_api:head_object(BucketId, PrefixedObjectKey) of
			not_found -> not_found;
			Metadata -> validate_request(BucketId, Metadata)
		    end
	    end;
	false -> {error, 7}
    end.
validate_request(BucketId, Metadata0) ->
    case proplists:get_value("x-amz-meta-is-deleted", Metadata0) of
	"true" -> not_found;
	_ ->
	    {OldBucketId, RealPath} = real_path(BucketId, Metadata0),

	    ContentType = proplists:get_value(content_type, Metadata0),
	    Bytes = proplists:get_value("x-amz-meta-bytes", Metadata0),
	    OrigName0 = erlang:list_to_binary(proplists:get_value("x-amz-meta-orig-filename", Metadata0)),
	    OrigName1 = utils:unhex(OrigName0),
	    {OldBucketId, RealPath, ContentType, OrigName1, erlang:list_to_binary(Bytes)}
    end.

%%
%% Checks if client has access to the system.
%%
%% It uses authorization token HTTP header, if provided.
%% Otherwise it checks session cookie.
%%
check_privileges(Req0) ->
    %% Extracts token from request headers and looks it up in "security" bucket
    case utils:get_token(Req0) of
	undefined -> 
	    Settings = #general_settings{},
	    SessionCookieName = Settings#general_settings.session_cookie_name,
	    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
	    case login_handler:check_session_id(SessionID0) of
		false -> {error, 28};
		{error, Code} -> {config_error, Code};
		User -> User
	    end;
	Token -> 
	    case login_handler:check_token(Token) of
		not_found -> {error, 28};
		expired -> {error, 38};
		User -> User
	    end
    end.

%%
%% Receives stream from httpc and passes it to cowboy
%%
receive_streamed_body(Req0, RequestId, Pid) ->
    httpc:stream_next(Pid),
    receive
	{http, {RequestId, stream, BinBodyPart}} ->
	    cowboy_req:stream_body(BinBodyPart, nofin, Req0),
	    receive_streamed_body(Req0, RequestId, Pid);
	{http, {RequestId, stream_end, _Headers}} ->
	    cowboy_req:stream_body(<<>>, fin, Req0);
	{http, Msg} ->
	    ?ERROR("[download_handler] error receiving stream body: ~p", [Msg]),
	    cowboy_req:stream_body(<<>>, fin, Req0)
    end.

%%
%% Checks if Range request header is valid.
%%
validate_range(Req0) ->
    Value = cowboy_req:header(<<"range">>, Req0),
    {StartByte0, EndByte0} =
	case Value of
	    undefined -> {undefined, undefined};
	    Value ->
		{bytes, [{StartByte1, EndByte1}]} = cow_http_hd:parse_range(Value),
		{StartByte1, EndByte1}
	end,
    case StartByte0 =/= undefined andalso EndByte0 =/= undefined of
	true ->
	    case StartByte0 > EndByte0 of
		true -> undefined;
		false -> Value
	    end;
	false -> Value
    end.


init(Req0, Opts) ->
    cowboy_req:cast({set_options, #{idle_timeout => infinity}}, Req0),
    case check_privileges(Req0) of
	{error, Number} ->
	    Req1 = cowboy_req:reply(403, #{
		<<"content-type">> => <<"application/json">>
	    }, jsx:encode([{error, Number}]), Req0),
	    {ok, Req1, []};
	{config_error, Code} ->
	    Req1 = cowboy_req:reply(500, #{
		<<"content-type">> => <<"application/json">>
	    }, jsx:encode([{error, erlang:list_to_binary(Code)}]), Req0),
	    {ok, Req1, []};
	User ->
	    PathInfo = cowboy_req:path_info(Req0),
	    BucketId =
		case lists:nth(1, PathInfo) of
		    undefined -> undefined;
		    <<>> -> undefined;
		    BV -> erlang:binary_to_list(BV)
		end,
	    Path0 = lists:nthtail(1, PathInfo),
	    Path1 = erlang:list_to_binary(utils:join_list_with_separator(Path0, <<"/">>, [])),
	    PrefixedObjectKey = erlang:binary_to_list(Path1),
	    case validate_request(BucketId, User, PrefixedObjectKey) of
		not_found ->
		    Req1 = cowboy_req:reply(404, #{
			<<"content-type">> => <<"application/json">>
		    }, Req0),
		    {ok, Req1, []};
		{error, Number} ->
		    Req1 = cowboy_req:reply(400, #{
			<<"content-type">> => <<"application/json">>
		    }, jsx:encode([{error, Number}]), Req0),
		    {ok, Req1, []};
		{OldBucketId, RealPath, ContentType, OrigName, Bytes} ->
		    Range = validate_range(Req0),
		    ContentDisposition = << <<"attachment;filename=\"">>/binary, OrigName/binary, <<"\"">>/binary >>,
		    Headers0 = #{
			<<"content-type">> => ContentType,
			<<"content-disposition">> => ContentDisposition,
			<<"content-length">> => Bytes
		    },
		    Headers1 =
			case Range of
			    undefined -> Headers0;
			    _ -> maps:put(<<"range">>, Range, Headers0)
			end,
		    Req1 = cowboy_req:stream_reply(200, Headers1, Req0),
		    case riak_api:get_object(OldBucketId, RealPath, stream) of
			not_found ->
        		    Req1 = cowboy_req:reply(404, #{
            			<<"content-type">> => <<"text/html">>
        		    }, <<"404: Not found">>, Req0),
			    {ok, Req1, []};
			{ok, RequestId} ->
			    receive
				{http, {RequestId, stream_start, _Headers, Pid}}  ->
				    receive_streamed_body(Req1, RequestId, Pid);
				{http, Msg} ->
				    ?ERROR("[download_handler] stream error: ~p", [Msg])
			    end,
			    {ok, Req1, Opts}
		    end
	    end
    end.
