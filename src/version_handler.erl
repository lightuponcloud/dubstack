%%
%% Version handler returns info on provided bucket DVV ( Dotted version Vector )
%%
%% GET application/json
%%	Returns version of DB in JSON format.
%%
%% GET application/octet-stream
%%	Allows to download SQLite DB
%%
-module(version_handler).
-behavior(cowboy_handler).

-export([init/2]).

-include("riak.hrl").
-include("entities.hrl").
-include("general.hrl").
-include("action_log.hrl").
-include("log.hrl").

%%
%% Checks if client has access to the system.
%%
%% It uses authorization token HTTP header.
%%
check_privileges(Req0, BucketId) ->
    %% Extracts token from request headers and looks it up in "security" bucket
    case utils:get_token(Req0) of
	undefined ->
	    case utils:is_public_bucket_id(BucketId) of
		true -> undefined;
		false -> {error, 28}
	    end;
	Token ->
	    case login_handler:check_token(Token) of
		not_found -> {error, 28};
		expired -> {error, 38};
		User -> check_user_access(BucketId, User)
	    end
    end.

%%
%% Check if user has access to the bucket.
%%
check_user_access(BucketId, User) ->
    case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
	true ->
	    UserBelongsToGroup =
		case utils:is_public_bucket_id(BucketId) of
		    true -> true;  %% anyone can download from public bucket
		    false -> lists:any(fun(Group) ->
				utils:is_bucket_belongs_to_group(BucketId, User#user.tenant_id, Group#group.id) end,
				User#user.groups)
		end,
	    case UserBelongsToGroup of
		false -> {error, 37};
		true -> User
	    end;
	false -> {error, 7}
    end.


%%
%% Checks if uploaded file is less in size than 5MB ( unlikely SQLite db can be bigger ).
%% Make sure content-range header matches size of uploaded data
%%
validate_data_size(DataSize) when DataSize =:= 0 -> {error, 2};
validate_data_size(DataSize) when DataSize > ?DB_VERSION_MAXIMUM_SIZE -> {error, 24};
validate_data_size(_DataSize) -> true.


%%
%% Receives stream from httpc and passes it to cowboy
%%
receive_streamed_body(Req0, RequestId0, Pid0) ->
    httpc:stream_next(Pid0),
    receive
	{http, {RequestId0, stream, BinBodyPart}} ->
	    cowboy_req:stream_body(BinBodyPart, nofin, Req0),
	    receive_streamed_body(Req0, RequestId0, Pid0);
	{http, {RequestId0, stream_end, _Headers0}} ->
	    cowboy_req:stream_body(<<>>, fin, Req0);
	{http, Msg} ->
	    ?ERROR("[version_handler] error receiving stream body: ~p", [Msg]),
	    cowboy_req:stream_body(<<>>, fin, Req0)
    end.


%%
%% Downloads .luc SQLite DB from Riak CS and returns it to client.
%%
stream_db(Req0, BucketId0, Bytes, StartByte0) ->
    BucketId1 = utils:to_binary(BucketId0),
    OrigName = << BucketId1/binary, <<".luc">>/binary>>,
    ContentDisposition = << <<"attachment;filename=\"">>/binary, OrigName/binary, <<"\"">>/binary >>,
    StartByte1 = erlang:integer_to_binary(StartByte0),
    Headers0 = #{
	<<"content-type">> => << "application/octet-stream" >>,
	<<"content-disposition">> => ContentDisposition,
	<<"content-length">> => erlang:integer_to_binary(Bytes),
	<<"range">> => << "bytes=", StartByte1/binary, "-" >>
    },
    Req1 = cowboy_req:stream_reply(200, Headers0, Req0),
    case riak_api:get_object(BucketId0, ?DB_VERSION_KEY, stream) of
	not_found ->
	    Req2 = cowboy_req:reply(404, #{
		<<"content-type">> => <<"application/json">>
	    }, Req0),
	    {ok, Req2, []};
	{ok, RequestId} ->
	    receive
	    {http, {RequestId, stream_start, _Headers, Pid}} ->
		receive_streamed_body(Req1, RequestId, Pid);
		{http, Msg} -> ?ERROR("[version_handler] error starting stream: ~p", [Msg])
	    end,
	    {ok, Req1, []}
    end.

handle_post(Req0, BucketId) ->
    {FieldValues, Req1} = upload_handler:acc_multipart(Req0, []),
    Version0 =
	try
	    upload_handler:validate_version(proplists:get_value(version, FieldValues))
	catch error:_ ->
	    {error, 22}
	end,
    Blob = proplists:get_value(blob, FieldValues),
    Md5 = upload_handler:validate_md5(proplists:get_value(md5, FieldValues)),
    DataSizeOk =
	case Blob of
	    undefined -> {error, 2};
	    _ -> validate_data_size(byte_size(Blob))
	end,
    case lists:keyfind(error, 1, [Version0, DataSizeOk, Md5]) of
	{error, Number} -> js_handler:bad_request_ok(Req1, Number);
	false ->
	    %% TODO: check if object is locked using sqlite_server
	    Version1 = base64:encode(jsx:encode(Version0)),
	    Checksum = utils:hex(riak_crypto:md5(Blob)),
	    Meta = [{acl, public_read}, {meta, [
		{"version", Version1},
		{"bytes", byte_size(Blob)},
		{"md5", Checksum}
	    ]}],
	    %% TODO: add user id ( just in case )
	    case riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, Meta) of
		ok ->
		    Req2 = cowboy_req:reply(200, #{
			<<"content-type">> => <<"application/json">>
		    }, jsx:encode([
			{md5, Md5}
		    ]), Req0),
		    {ok, Req2, []};
		_ -> js_handler:too_many(Req0)
	    end
    end.

%%
%% HEAD HTTP request returns DVV in JSON format
%%
%% GET request returns SQLite DB itself.
%%
%% POST request uploads a new version of SQLite DB.
%%
response(Req0, <<"HEAD">>, BucketId) ->
    case riak_api:head_object(BucketId, ?DB_VERSION_KEY) of
	not_found -> js_handler:not_found_ok(Req0);
	Meta ->
	    DbMeta = list_handler:parse_object_record(Meta, []),
	    Version = proplists:get_value("version", DbMeta),
	    Req1 = cowboy_req:reply(200, #{
		<<"DVV">> => utils:to_binary(Version),
		<<"content-type">> => <<"application/json">>
	    }, <<>>, Req0),
	    {ok, Req1, []}
    end;

response(Req0, <<"GET">>, BucketId) ->
    case riak_api:head_object(BucketId, ?DB_VERSION_KEY) of
	not_found -> js_handler:not_found_ok(Req0);
	Meta ->
	    Bytes =
		case proplists:get_value("x-amz-meta-bytes", Meta) of
		    undefined -> 0;  %% It might be undefined in case of corrupted metadata
		    B -> utils:to_integer(B)
		end,
	    case download_handler:validate_range(cowboy_req:header(<<"range">>, Req0), Bytes) of
		undefined -> stream_db(Req0, BucketId, Bytes, 0);
		{error, Number} -> js_handler:bad_request_ok(Req0, Number);
		{StartByte, _EndByte} -> stream_db(Req0, BucketId, Bytes, StartByte)
	    end
    end;

response(Req0, <<"POST">>, BucketId) ->
    handle_post(Req0, BucketId);

response(Req0, _Method, _BucketId) ->
    js_handler:bad_request_ok(Req0, 17).

init(Req0, _Opts) ->
    cowboy_req:cast({set_options, #{idle_timeout => infinity}}, Req0),
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    case check_privileges(Req0, BucketId) of
	{error, Number} ->
	    Req1 = cowboy_req:reply(403, #{
		<<"content-type">> => <<"application/json">>
	    }, jsx:encode([{error, Number}]), Req0),
	    {ok, Req1, []};
	_User ->
	    Method = cowboy_req:method(Req0),
	    response(Req0, Method, BucketId)
    end.
