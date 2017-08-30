-module(keystone_api).

-include("riak.hrl").
-include("keystone.hrl").

-export([check_token/1, get_colleagues/2]).

-define(DEFAULT_TIMEOUT, 10000).

%%
%% Returns admin keystone token
%%
-spec get_admin_token(keystone_api_config()) -> string() | atom().

get_admin_token(Config) ->
    URL0 = lists:flatten([Config#keystone_api_config.keystone_url, "/v2.0/tokens"]),
    Hdrs0 = [{utils:to_list(K), utils:to_list(V)} ||
	{K, V} <- [{"Content-type", "application/json"}]],
    Hdrs1 = [{"connection", "close"} | Hdrs0],

    Body = jsx:encode([
	{auth, [
	    {tenantName, utils:to_binary(Config#keystone_api_config.keystone_admin_tenant)},
	    {passwordCredentials, [
		{username, utils:to_binary(Config#keystone_api_config.keystone_admin_username)},
		{password, utils:to_binary(Config#keystone_api_config.keystone_admin_password)}
	    ]}
	]}
    ]),
    Timeout = get_timeout(Config),
    case httpc:request(post, {URL0, Hdrs1, "application/json", Body},
				  [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body0}} ->
	    Body1 = jsx:decode(Body0),
	    Body2 = proplists:get_value(<<"access">>, Body1),
	    case Body2 of
		undefined ->
		    not_found;
		Body3 ->
		    Body4 = proplists:get_value(<<"token">>, Body3),
		    proplists:get_value(<<"id">>, Body4)
	    end;
        _ ->
            not_found
    end.

%%
%% Requests Keystone server and returns username for provided token
%%
-spec check_token(string()) -> string().

check_token(Token) when is_list(Token) ->
    Config = #keystone_api_config{},
    Timeout = get_timeout(Config),
    AdminToken0 = get_admin_token(Config),
    case AdminToken0 of
	not_found -> not_found;
	AdminToken1 ->
	    URL = lists:flatten([Config#keystone_api_config.keystone_url, "/v2.0/tokens/", Token]),
	    case httpc:request(get, {URL, [{"X-Auth-Token", binary_to_list(AdminToken1)}]},
					  [{timeout, Timeout}], [{body_format, binary}]) of
		{ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body0}} ->
		    Body1 = jsx:decode(Body0),
		    Body2 = proplists:get_value(<<"access">>, Body1),
		    case Body2 of
			undefined ->
			    not_found;
			Body3 ->
			    Body4 = proplists:get_value(<<"user">>, Body3),
			    UserId = proplists:get_value(<<"id">>, Body4),
			    UserName = proplists:get_value(<<"username">>, Body4),
			    Body5 = proplists:get_value(<<"token">>, Body3),
			    Body6 = proplists:get_value(<<"tenant">>, Body5),
			    TenantId = proplists:get_value(<<"id">>, Body6),
			    TenantName = proplists:get_value(<<"name">>, Body6),
			    [{user_id, binary_to_list(UserId)},
			     {user_name, binary_to_list(UserName)},
			     {tenant_id, binary_to_list(TenantId)},
			     {tenant_name, binary_to_list(TenantName)}]
		    end;
	    _ ->
		not_found
	    end
    end;
check_token(Token) when Token =:= undefined ->
    not_found.

%% Returns list of User IDs for provided tenant
-spec get_colleagues(string(), string()) -> proplist().

get_colleagues(Token, TenantId) when is_list(Token), is_list(TenantId) ->
    Config = #keystone_api_config{},
    Timeout = get_timeout(Config),
    URL = lists:flatten([Config#keystone_api_config.keystone_url, "/v2.0/users"]),

    case httpc:request(get, {URL, [{"X-Auth-Token", Token}]}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body0}} ->
	    Body1 = jsx:decode(Body0),
	    Body2 = proplists:get_value(<<"users">>, Body1),
	    case Body2 of
		undefined ->
		    not_found;
		Body3 ->
		    [[{user_id, proplists:get_value(<<"id">>, UserRecord)},
		      {user_name, proplists:get_value(<<"description">>, UserRecord)}
		     ] || UserRecord <- Body3, proplists:get_value(<<"tenantId">>, UserRecord) =:= TenantId]
	    end;
        _ ->
            not_found
    end;

get_colleagues(_,_) ->
    not_found.

get_timeout(#keystone_api_config{timeout = undefined}) ->
    ?DEFAULT_TIMEOUT;
get_timeout(#keystone_api_config{timeout = Timeout}) ->
    Timeout.
