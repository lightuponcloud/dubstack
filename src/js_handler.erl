%%
%% Provides javascript library with required context variables.
%%
-module(js_handler).

-export([init/2, bad_request/2, forbidden/2, unauthorized/2, not_found/1, too_many/1,
	 redirect_to_login/1, incorrect_configuration/2]).

-include("general.hrl").
-include("riak.hrl").

%%
%% Parse IETF language tag
%%
%% Ther's room for improvement, as it uses only 2-letter language code.
%% No variants supported. Old format not supported as well.
%%
-spec parse_language_tag(binary()) -> string().

parse_language_tag(undefined) -> ?DEFAULT_LANGUAGE_TAG;
parse_language_tag(<<>>) -> ?DEFAULT_LANGUAGE_TAG;
parse_language_tag(Code0) when erlang:is_binary(Code0) ->
    Bits0 = binary:split(Code0, <<",">>, [global]),
    case length(Bits0) of
	0 -> ?DEFAULT_LANGUAGE_TAG;
	_ ->
	    Bits1 = binary:split(lists:nth(1, Bits0), <<"-">>, [global]),
	    case length(Bits1) =:= 2 of
		false -> ?DEFAULT_LANGUAGE_TAG;
		true ->
		    Code1 = lists:nth(1, Bits1),
		    case byte_size(Code1) =:= 2 of
			true -> erlang:binary_to_list(Code1);
			false -> ?DEFAULT_LANGUAGE_TAG
		    end
	    end
    end.

decode_messages_json(Path, DefaultPath) ->
    case file:read_file(Path) of
	{ok, Content0} ->
	    case jsx:is_json(Content0) of
		{error, badarg} -> "{}";
		false -> "{}";
		true -> jsx:decode(Content0)
	    end;
	{error, _Error} ->
	    case file:read_file(DefaultPath) of
		{ok, Content1} ->
		    case jsx:is_json(Content1) of
			{error, badarg} -> "{}";
			false -> "{}";
			true -> jsx:decode(Content1)
		    end
	    end
    end.

init(Req0, Opts) ->
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    Value0 -> binary_to_list(Value0)
	end,
    Settings = #general_settings{},
    SessionCookieName = Settings#general_settings.session_cookie_name,
    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
    Token =
	case login_handler:check_session_id(SessionID0) of
	    false -> <<>>;
	    {error, _} -> <<>>;
	    _User -> SessionID0
	end,
    %% Since ther's no way to detect browser language preference, Accept-Language should be used instead
    LanguageCode = parse_language_tag(cowboy_req:header(<<"accept-language">>, Req0)),
    %% Load messages from catalog, stored in JSON file in filesystem
    EbinDir = filename:dirname(code:which(js_handler)),
    AppDir = filename:dirname(EbinDir),
    MessagesFilePath = filename:join([AppDir, "priv", lists:flatten([LanguageCode, ".json"])]),
    DefaultMessagesFilePath = filename:join([AppDir, "priv", "en.json"]),
    JSONMessages = decode_messages_json(MessagesFilePath, DefaultMessagesFilePath),
    {ok, Body} = jquery_riak_js_dtl:render([
	{messages, JSONMessages},
	{root_path, Settings#general_settings.root_path},
	{bucket_id, BucketId},
	{token, Token},
	{chunk_size, ?FILE_UPLOAD_CHUNK_SIZE}
    ]),
    Req1 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/html">>}, Body, Req0),
    {ok, Req1, Opts}.

bad_request(Req0, MsgCode) when erlang:is_integer(MsgCode) orelse erlang:is_list(MsgCode) ->
    Req1 = cowboy_req:reply(400, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode([{error, MsgCode}]), Req0),
    {true, Req1, []}.

forbidden(Req0, MsgCode) when erlang:is_integer(MsgCode) ->
    Req1 = cowboy_req:reply(403, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode([{error, MsgCode}]), Req0),
    {true, Req1, []}.

unauthorized(Req0, MsgCode) when erlang:is_integer(MsgCode) ->
    Req1 = cowboy_req:reply(401, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode([{error, MsgCode}]), Req0),
    {{false, <<"Token">>}, Req1, []}.

too_many(Req0) ->
    Req1 = cowboy_req:reply(429, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode([{error, 33}]), Req0),
    {true, Req1, []}.

not_found(Req0) ->
    Req1 = cowboy_req:reply(404, #{
	<<"content-type">> => <<"application/json">>
    }, <<>>, Req0),
    {true, Req1, []}.

incorrect_configuration(Req0, MsgCode) when erlang:is_list(MsgCode) ->
    Req1 = cowboy_req:reply(500, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode([{error, erlang:list_to_binary(MsgCode)}]), Req0),
    {true, Req1, []}.

redirect_to_login(Req0) ->
    Settings = #general_settings{},
    Proto = <<"http://">>,
    Domain = utils:to_binary(Settings#general_settings.domain),
    URI = utils:to_binary(Settings#general_settings.root_path),
    Req1 = cowboy_req:reply(302, #{
	<<"Location">> => << Proto/binary, Domain/binary, URI/binary >>
    }, <<>>, Req0),
    {ok, Req1, []}.
