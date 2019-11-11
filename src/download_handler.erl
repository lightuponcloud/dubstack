%%
%% Allows to download objects from Riak CS, after authentication.
%%
-module(download_handler).

-export([init/2]).

-include("general.hrl").
-include("riak.hrl").
-include("user.hrl").
-include("log.hrl").

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
			not_found -> {error, 17};
			RiakResponse0 ->
			    ModifiedTime = proplists:get_value("x-amz-meta-modified-utc", RiakResponse0),
			    GUID = proplists:get_value("x-amz-meta-guid", RiakResponse0),
			    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
			    RealKey = utils:format_timestamp(utils:to_integer(ModifiedTime)),
			    RealPath = utils:prefixed_object_key(RealPrefix, RealKey),
			    ContentType = proplists:get_value(content_type, RiakResponse0),

			    OrigName0 = proplists:get_value("x-amz-meta-orig-filename", RiakResponse0),
			    OrigName1 = erlang:list_to_binary(OrigName0),
		
			    Bytes = proplists:get_value("x-amz-meta-bytes", RiakResponse0),
			    {RealPath, ContentType, OrigName1, Bytes}
		    end
	    end;
	false -> {error, 7}
    end.

%%
%% Checks if client has access to the system.
%%
%% It uses authorization token HTTP header, if provided.
%% Otherwise it checks session cookie.
%%
check_token(Req0) ->
    case utils:check_token(Req0) of
	undefined -> 
	    Settings = #general_settings{},
	    SessionCookieName = Settings#general_settings.session_cookie_name,
	    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
	    case login_handler:check_session_id(SessionID0) of
		false -> {error, 28};
		{error, Code} -> {config_error, Code};
		User -> User
	    end;
	not_found -> {error, 28};
	expired -> {error, 38};
	User -> User
    end.

%%
%% Receives stream from httpc and passes it to cowboy
%%
receive_streamed_body(Req0, RequestId) ->
    receive
	{http, {RequestId, stream, BinBodyPart}} ->
	    cowboy_req:stream_body(BinBodyPart, nofin, Req0),
	    receive_streamed_body(Req0, RequestId);
	{http, {RequestId, stream_end, _Headers}} ->
	    cowboy_req:stream_body(<<>>, fin, Req0);
	{http, Msg} ->
	    ?ERROR("[download_handler] stream error: ~p", [Msg]),
	    cowboy_req:stream_body(<<>>, fin, Req0)
    end.

receive_streamed_body(Req0, RequestId, Pid) ->
    httpc:stream_next(Pid),
    receive
	{http, {RequestId, stream, BinBodyPart}} ->
	    cowboy_req:stream_body(BinBodyPart, nofin, Req0),
	    receive_streamed_body(Req0, RequestId, Pid);
	{http, {RequestId, stream_end, _Headers}} ->
	    cowboy_req:stream_body(<<>>, fin, Req0);
	{http, Msg} ->
	    ?ERROR("[download_handler] stream error: ~p", [Msg]),
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
    case check_token(Req0) of
	{error, Number} -> js_handler:forbidden(Req0, Number);
	{config_error, Code} -> js_handler:incorrect_configuration(Req0, Code);
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
		{error, Number} -> js_handler:bad_request(Req0, Number);
		{RealPath, ContentType, OrigName, Bytes} ->
		    Range = validate_range(Req0),

		    ContentDisposition = << <<"attachment;filename=">>/binary, OrigName/binary >>,
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
		    {ok, RequestId} = riak_api:get_object(BucketId, RealPath, stream),
		    receive
			{http, {RequestId, stream_start, _Headers}} ->
			    receive_streamed_body(Req1, RequestId);
			{http, {RequestId, stream_start, _Headers, Pid}}  ->
			    receive_streamed_body(Req1, RequestId, Pid);
			{http, Msg} ->
			    ?ERROR("[download_handler] stream error: ~p", [Msg])
		    end,
		    {ok, Req1, Opts}
	    end
    end.
