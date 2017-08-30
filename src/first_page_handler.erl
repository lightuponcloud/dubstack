-module(first_page_handler).

-export([init/2]).

-include("riak.hrl").
-include("general.hrl").


not_found_response(Req0, Settings) ->
    {ok, Body} = not_found_dtl:render([
	{admin_email, Settings#general_settings.admin_email},
	{static_root, Settings#general_settings.static_root}
    ]),
    Req1 = cowboy_req:reply(404, #{
	<<"content-type">> => <<"text/html">>
    }, Body, Req0),
    {ok, Req1, []}.

init(Req0, Opts) ->
    Token = case cowboy_req:binding(token, Req0) of
	undefined -> undefined;
	TokenValue -> binary_to_list(TokenValue)
    end,
    BucketName = case cowboy_req:binding(bucket_name, Req0) of
	undefined -> undefined;
	BucketNameValue -> binary_to_list(BucketNameValue)
    end,
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix0 =
	case proplists:get_value(<<"prefix">>, ParsedQs) of
	    undefined -> undefined;
	    Prefix1 ->
		case utils:is_valid_hex_prefix(Prefix1) of
		    true ->
			unicode:characters_to_binary(Prefix1);
		    false -> undefined
		end
	end,
    Settings = #general_settings{},
    KeystoneAttrs = keystone_api:check_token(Token),
    UserName = case KeystoneAttrs of
	not_found -> not_found;
	_ -> proplists:get_value(user_name, KeystoneAttrs)
    end,
    TenantName = case KeystoneAttrs of
	not_found -> not_found;
	_ -> proplists:get_value(tenant_name, KeystoneAttrs)
    end,
    case (KeystoneAttrs =:= not_found)
	    orelse (utils:is_valid_bucket_name(BucketName, TenantName) =:= false) of
	true ->
	    not_found_response(Req0, Settings);
	false ->
	    Colleagues = keystone_api:get_colleagues(Token, TenantName),
	    {ok, Body} = index_dtl:render([
		{hex_prefix, Prefix0},
		{bucket_name, BucketName},
		{brand_name, Settings#general_settings.brand_name},
		{static_root, Settings#general_settings.static_root},
		{token, Token},
		{user_name, UserName},
		{tenant_name, TenantName},
		{root_uri, Settings#general_settings.root_uri},
		{colleagues, Colleagues},
		{is_bucket_belongs_to_user,
		    utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName)}
	    ]),

	    Req1 = cowboy_req:reply(200, #{
		<<"content-type">> => <<"text/html">>
	    }, Body, Req0),

	    {ok, Req1, Opts}
    end.
