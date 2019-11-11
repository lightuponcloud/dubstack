%%
%% Logs user out.
%%
-module(logout_handler).
-behavior(cowboy_handler).

-export([init/2]).

-include("general.hrl").
-include("riak.hrl").

%%
%% Deletes auth token from DB
%%
-spec revoke_token(binary()) -> ok.

revoke_token(UUID4) when erlang:is_binary(UUID4) ->
    PrefixedToken = utils:prefixed_object_key(?TOKENS_PREFIX, erlang:binary_to_list(UUID4)),
    riak_api:delete_object(?SECURITY_BUCKET_NAME, PrefixedToken).

%%
%% Deletes CSRF token from DB
%%
-spec revoke_csrf_token(binary()) -> ok.

revoke_csrf_token(UUID4) when erlang:is_binary(UUID4) ->
    PrefixedToken = utils:prefixed_object_key(?CSRF_TOKENS_PREFIX, erlang:binary_to_list(UUID4)),
    riak_api:delete_object(?SECURITY_BUCKET_NAME, PrefixedToken).


init(Req0, _Opts) ->
    Settings = #general_settings{},
    SessionCookieName = Settings#general_settings.session_cookie_name,
    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
    case login_handler:check_session_id(SessionID0) of
	false -> js_handler:redirect_to_login(Req0);
	{error, Code} -> js_handler:incorrect_configuration(Req0, Code);
	_User ->
	    CSRFCookieName = Settings#general_settings.csrf_cookie_name,
	    Req1 = cowboy_req:set_resp_cookie(utils:to_binary(SessionCookieName),
		<<"deleted">>, Req0, #{max_age => 0}),
	    Req2 = cowboy_req:set_resp_cookie(utils:to_binary(CSRFCookieName),
		<<"deleted">>, Req1, #{max_age => 0}),
	    revoke_token(SessionID0),
	    #{CSRFCookieName := CSRFToken0} = cowboy_req:match_cookies(
		[{Settings#general_settings.csrf_cookie_name, [], undefined}], Req0),
	    case login_handler:check_csrf_token(CSRFToken0) of
		false -> ok;
		_ ->
		    revoke_csrf_token(CSRFToken0)
	    end,
	    js_handler:redirect_to_login(Req2)
    end.
