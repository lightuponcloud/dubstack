%%
%% Renders first page template if user is logged in.
%% Otherwise displays login page.
%%
-module(first_page_handler).

-export([init/2, allowed_methods/2, login/2, forbidden_response/1]).

-include("general.hrl").
-include("riak.hrl").
-include("user.hrl").


init(Req0, _Opts) ->
    Settings = #general_settings{},
    SessionCookieName = Settings#general_settings.session_cookie_name,
    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
    case login_handler:check_session_id(SessionID0) of
	false ->
	    login(Req0, Settings);
	User ->
	    State = admin_users_handler:user_to_proplist(User) ++ [{token, SessionID0}],
	    first_page(Req0, Settings, State)
    end.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"POST">>], Req, State}.


forbidden_response(Req0) ->
    {ok, Body} = forbidden_dtl:render([]),
    Req1 = cowboy_req:reply(403, #{
	<<"content-type">> => <<"text/html">>
    }, Body, Req0),
    {ok, Req1, []}.


validate_post(Req0, Settings) ->
    %% Same-domain requests should have referer set ( very rarely absent )
    case cowboy_req:header(<<"referer">>, Req0) of
	undefined -> false;
	_ ->
	    {ok, KeyValues, _} = cowboy_req:read_urlencoded_body(Req0),
	    CSRFCookieName = Settings#general_settings.csrf_cookie_name,
	    #{CSRFCookieName := CSRFToken0} = cowboy_req:match_cookies(
		[{Settings#general_settings.csrf_cookie_name, [], undefined}], Req0),
	    IsCSRFValid =
		case login_handler:check_csrf_token(CSRFToken0) of
		    true -> true;
		    false ->
			%% Cookie was not set, check CSRF token from body of POST request
			CSRFToken1 = proplists:get_value(<<"csrf_token">>, KeyValues),
			login_handler:check_csrf_token(CSRFToken1)
		end,
	    case IsCSRFValid of
		false -> false;
		true -> {
			    proplists:get_value(<<"login">>, KeyValues),
			    proplists:get_value(<<"password">>, KeyValues)
		        }
	    end
    end.

set_csrf_token(Req0, Settings) ->
    CSRFCookieName = Settings#general_settings.csrf_cookie_name,
    #{CSRFCookieName := CSRFToken0} = cowboy_req:match_cookies(
	[{Settings#general_settings.csrf_cookie_name, [], undefined}], Req0),
    CSRFToken1 =
	case CSRFToken0 of
	    undefined ->
		login_handler:new_csrf_token();
	    Token0 ->
		case login_handler:check_csrf_token(Token0) of
		    false -> login_handler:new_csrf_token();
		    true -> Token0
		end
	end,
    %% Set Session ID cookie
    %% If HTTPS is used, add secure => true
    CookieDomain = utils:to_binary(Settings#general_settings.domain),
    {cowboy_req:set_resp_cookie(utils:to_binary(CSRFCookieName), CSRFToken1, Req0,
	    #{max_age => ?CSRF_TOKEN_EXPIRATION_TIME,
	      http_only => true,
	      domain => CookieDomain,
	      path => <<"/">>}), CSRFToken1}.


-spec error_response(any(), general_settings) -> any().

error_response(Req0, Settings) ->
    {Req1, CSRFToken0} = set_csrf_token(Req0, Settings),
    {ok, Body} = login_dtl:render([
	{static_root, Settings#general_settings.static_root},
	{root_path, Settings#general_settings.root_path},
	{csrf_token, CSRFToken0},
	{error, "Incorrect Credentials"}
    ]),
    Req2 = cowboy_req:reply(200, #{
	<<"content-type">> => <<"text/html">>
    }, Body, Req1),
    {ok, Req2, []}.


login(Req0, Settings) ->
    case cowboy_req:method(Req0) of
	<<"GET">> ->
	    {Req1, CSRFToken0} = set_csrf_token(Req0, Settings),
	    {ok, Body} = login_dtl:render([
		{static_root, Settings#general_settings.static_root},
		{root_path, Settings#general_settings.root_path},
		{csrf_token, CSRFToken0}
	    ]),
	    Req2 = cowboy_req:reply(200, #{
		<<"content-type">> => <<"text/html">>
	    }, Body, Req1),
	    {ok, Req2, []};
	<<"POST">> ->
	    %% Set CSRF token to reset expiration time
	    case validate_post(Req0, Settings) of
		false -> forbidden_response(Req0);
		{Login, Password} ->
		    case login_handler:check_credentials(Login, Password) of
			false -> error_response(Req0, Settings);
			not_found -> error_response(Req0, Settings);
			User ->
			    UUID4 = login_handler:new_token(Req0, User#user.id, User#user.tenant_id),
			    State = admin_users_handler:user_to_proplist(User) ++ [{token, UUID4}],
			    %% Set Session ID cookie
			    %% If HTTPS is used, add secure => true
			    CookieDomain = utils:to_binary(Settings#general_settings.domain),
			    SessionCookieName = utils:to_binary(Settings#general_settings.session_cookie_name),
			    Req1 = cowboy_req:set_resp_cookie(SessionCookieName,
				UUID4, Req0, #{max_age => ?SESSION_EXPIRATION_TIME,
					       http_only => true,
					       domain => CookieDomain,
					       path => <<"/">>}),
			    first_page(Req1, Settings, State)
		    end
	    end
    end.

first_page(Req0, Settings, State) ->
    BucketId0 =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix0 =
	case list_handler:validate_prefix(BucketId0, proplists:get_value(<<"prefix">>, ParsedQs)) of
	    {error, _} -> not_found;
	    Prefix1 -> Prefix1
	end,
    UserGroups = proplists:get_value(groups, State),
    BucketId1 =
	case BucketId0 =:= undefined of
	    true ->
		case lists:any(fun(_) -> true end, UserGroups) of
		    true ->
			%% Display the first bucket in list
			Group = lists:nth(1, UserGroups),
			GroupId = erlang:binary_to_list(proplists:get_value(id, Group)),
			TenantId = erlang:binary_to_list(proplists:get_value(tenant_id, State)),
			Bits = [?RIAK_BACKEND_PREFIX, TenantId, GroupId, ?RESTRICTED_BUCKET_SUFFIX],
			lists:flatten(utils:join_list_with_separator(Bits, "-", []));
		    false -> undefined
		end;
	    false -> BucketId0
	end,
    case BucketId1 =:= undefined orelse Prefix0 =:= not_found of
	true ->
	    {ok, Body} = not_found_dtl:render([
		{brand_name, Settings#general_settings.brand_name}
	    ]),
	    Req1 = cowboy_req:reply(404, #{
		<<"content-type">> => <<"text/html">>
	    }, Body, Req0),
	    {ok, Req1, []};
	false ->
	    {ok, Body} = index_dtl:render([
		{hex_prefix, Prefix0},
		{bucket_id, BucketId1},
		{brand_name, Settings#general_settings.brand_name},
		{static_root, Settings#general_settings.static_root},
		{root_path, Settings#general_settings.root_path},
		{bucket_suffix, ""},
		{private_suffix, ?PRIVATE_BUCKET_SUFFIX},
		{public_suffix, ?PUBLIC_BUCKET_SUFFIX}
	    ] ++ State),
	    Req1 = cowboy_req:reply(200, #{
		<<"content-type">> => <<"text/html">>
	    }, Body, Req0),
	    {ok, Req1, []}
    end.
