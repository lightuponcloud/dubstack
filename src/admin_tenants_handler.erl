%%
%% API endpoints for managing tenants
%%
-module(admin_tenants_handler).
-behavior(cowboy_handler).

%% Cowboy callbacks
-export([init/2, content_types_provided/2, content_types_accepted/2,
	 allowed_methods/2, forbidden/2, to_json/2, handle_post/2,
	 to_html/2, patch_resource/2, delete_resource/2, delete_completed/2,
	 resource_exists/2]).

%% Other methods
-export([parse_tenant/1, validate_groups/2, validate_group_id/1,
	 validate_boolean/3, validate_patch/2, get_tenant/1,
	 tenant_from_state/1, tenant_to_proplist/1]).

-include_lib("xmerl/include/xmerl.hrl").
-include("riak.hrl").
-include("user.hrl").
-include("general.hrl").


init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"POST">>, <<"PATCH">>, <<"DELETE">>], Req, State}.

parse_tenant(RootElement) ->
    TenantId = erlcloud_xml:get_text("/tenant/record/id", RootElement),
    TenantName = erlcloud_xml:get_text("/tenant/record/name", RootElement),
    IsTenantEnabled = erlcloud_xml:get_bool("/tenant/record/enabled", RootElement),
    GetGroupAttrs = fun(N) ->
	#group{id = erlcloud_xml:get_text("/group/id", N),
	    name = erlcloud_xml:get_text("/group/name", N)} end,
    Groups = [GetGroupAttrs(I) ||
	I <- xmerl_xpath:string("/tenant/record/groups/group", RootElement)],
    #tenant{
	id = TenantId,
	name = TenantName,
	enabled = IsTenantEnabled,
	groups = Groups
    }.

%%
%% Transforms Tenant record to proplist
%%
-spec tenant_to_proplist(user()) -> proplist().

tenant_to_proplist(Tenant) ->
    [
	{id, erlang:list_to_binary(Tenant#tenant.id)},
	{name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(Tenant#tenant.name)))},
	{enabled, utils:to_binary(Tenant#tenant.enabled)},
	{groups, [
	    [{id, erlang:list_to_binary(G#group.id)},
	    {name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(G#group.name)))},
	    {available_bytes, -1},  %% TODO: to store and display used and available bytes
	    {bucket_id, erlang:list_to_binary(lists:concat([?RIAK_BACKEND_PREFIX, "-", Tenant#tenant.id, "-", G#group.id, "-", ?RESTRICTED_BUCKET_SUFFIX]))}
	    ] || G <- Tenant#tenant.groups]}
    ].

%%
%% Returns details of tenant
%%
-spec get_tenant(string()|undefined) -> tenant().

get_tenant(TenantId0) when erlang:is_list(TenantId0) ->
    case riak_api:head_bucket(?SECURITY_BUCKET_NAME) of
	not_found -> not_found;
	_ ->
	    PrefixedTenantId = utils:prefixed_object_name(?TENANTS_PREFIX, TenantId0),
	    case riak_api:get_object(?SECURITY_BUCKET_NAME, PrefixedTenantId) of
		not_found -> not_found;
		Response ->
		    XMLDocument0 = utils:to_list(proplists:get_value(content, Response)),
		    {RootElement0, _} = xmerl_scan:string(XMLDocument0),
		    parse_tenant(RootElement0)
	    end
    end.

%%
%% Returns callback, corresponding to method
%%
%% (called after 'resource_exists()' )
%%
%% Only called for POST and PATCH methods.
%%
content_types_accepted(Req, State) ->
    case cowboy_req:method(Req) of
	<<"PATCH">> ->
	    {[{{<<"application">>, <<"json">>, []}, patch_resource}], Req, State};
	<<"POST">> ->
	    {[{{<<"application">>, <<"json">>, []}, handle_post}], Req, State}
    end.

%%
%% Returns callback 'to_json()'
%% ( called after 'forbidden()' )
%%
%% Only called for GET method.
%%
content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json},
	{{<<"text">>, <<"html">>, '*'}, to_html}
    ], Req, State}.

get_tenants_list(TenantList0, Marker0) ->
    RiakResponse = riak_api:list_objects(?SECURITY_BUCKET_NAME, [{prefix, ?TENANTS_PREFIX}, {marker, Marker0}]),
    case RiakResponse of
	not_found -> [];
	_ ->
	    Contents = proplists:get_value(contents, RiakResponse),
	    Marker1 = proplists:get_value(next_marker, RiakResponse),
	    TenantIds = [proplists:get_value(key, R) || R <- Contents],
	    TenantList1 = [get_tenant(filename:basename(Id)) || Id <- TenantIds,
		string:sub_string(Id, length(Id), length(Id)) =/= "/"],
	    case Marker1 of
		undefined -> TenantList0 ++ TenantList1;
		[] -> TenantList0 ++ TenantList1;
		NextMarker ->
		    get_tenants_list(TenantList0 ++ TenantList1, NextMarker)
	    end
    end.

%%
%% Returns JSON list of tenants
%%
to_json(Req0, State) ->
    case proplists:get_value(session_id, State) of
	undefined ->
	    %% Token should have been specified
	    User = proplists:get_value(user, State),
	    case User#user.staff of
		true -> {jsx:encode(
			    [tenant_to_proplist(T) || T <- get_tenants_list([], undefined)]),
			    Req0, State};
		false ->
		    %% Only staff is allowed to access this API endpoint
		    Req1 = cowboy_req:reply(403, #{
			<<"content-type">> => <<"application/json">>
			}, <<>>, Req0),
		    {<<>>, Req1, []}
	    end;
	_ ->
	    %% REST API accepts only bearer Tokens
	    Req1 = cowboy_req:reply(403, #{
		<<"content-type">> => <<"application/json">>
		}, <<>>, Req0),
	    {<<>>, Req1, []}
    end.

%%
%% Checks session ID and displays tenants list on HTML page
%%
to_html(Req0, State) ->
    Settings = #general_settings{},
    SessionId = proplists:get_value(session_id, State),
    case login_handler:check_session_id(SessionId) of
	false -> js_handler:redirect_to_login(Req0);
	User ->
	    case User#user.staff of
		false -> js_handler:redirect_to_login(Req0);
		true ->
		    TenantsList = [tenant_to_proplist(T) || T <- get_tenants_list([], undefined)],
		    State1 = admin_users_handler:user_to_proplist(User),
		    {ok, Body} = admin_tenants_dtl:render([
			{brand_name, Settings#general_settings.brand_name},
			{static_root, Settings#general_settings.static_root},
			{root_path, Settings#general_settings.root_path},
			{tenants, TenantsList},
			{tenants_count, length(TenantsList)}
		    ] ++ State1),
		    Req1 = cowboy_req:reply(200, #{
			<<"content-type">> => <<"text/html">>
		    }, Body, Req0),
		    {ok, Req1, []}
	    end
    end.

%%
%% Returns Tenant record or false
%%
tenant_from_state(Req0) ->
    case cowboy_req:binding(tenant_id, Req0) of
	undefined -> undefined;
	TenantId ->
	    case admin_tenants_handler:get_tenant(erlang:binary_to_list(TenantId)) of
		not_found -> undefined;
		Tenant -> Tenant
	    end
    end.

%%
%% Checks if provided token is correct.
%% Allows request in case ANONYMOUS_USER_CREATION is set to true
%%
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, _State) ->
    Settings = #general_settings{},
    case ?ANONYMOUS_USER_CREATION of
	true ->
	    User0 = #user{
		id = admin,
		name = "Administrator",
		tenant_id = "nonexistent",
		tenant_name = "Non-existent",
		tenant_enabled = true,
		login = Settings#general_settings.admin_email,
		enabled = true,
		staff = true
	    },
	    {false, Req0, [{user, User0}]};
	false ->
	    case utils:check_token(Req0) of
		not_found -> {true, Req0, []};
		expired -> {true, Req0, []};
		undefined ->
		    SessionCookieName = Settings#general_settings.session_cookie_name,
		    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
		    %% Response depends on content type
		    {false, Req0, [{session_id, SessionID0}]};
		User1 ->
		    case User1#user.staff of
			true -> {false, Req0, [{user, User1}]};
			false -> {true, Req0, []}
		    end
	    end
    end.

%%
%% Validates tenant and user IDs, if they are specified
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    case tenant_from_state(Req0) of
	undefined -> {true, Req0, State};
	Tenant ->
	    case admin_users_handler:user_from_state(Req0) of
		undefined -> {true, Req0, State ++ [{path_tenant, Tenant}]};
		not_found -> {false, Req0, []};
		User -> {true, Req0, State ++ [{path_tenant, Tenant}, {path_user, User}]}
	    end
    end.

%%
%% Checks if provided tenant id is specified and do not exceed length limit
%%
validate_tenant_id([]) ->
    {error, {tenant_id, <<"Tenant name error. Its latin representation should not be empty.">>}};
validate_tenant_id(TenantId0) when erlang:is_list(TenantId0) ->
    case utils:alphanumeric(TenantId0) of
	<<>> -> {error, {tenant_id, <<"Tenant name error. Its latin representation should not be empty.">>}};
	TenantId1 ->
	    case byte_size(TenantId1) > ?MAXIMUM_TENANT_NAME of
		true ->
		    {error, {tenant_id, erlang:list_to_binary([<<"Please choose a shorter tenant name. ">>,
				<<"Note, its latin representation should not exceed ">>,
				utils:to_binary(?MAXIMUM_TENANT_NAME), <<" characters.">>])}};
		false -> unicode:characters_to_list(TenantId1)
	    end
    end.

validate_tenant_name(undefined, required) ->
    {error, {name, <<"Tenant Name should be specified.">>}};
validate_tenant_name(undefined, not_required) ->
    {undefined, undefined};
validate_tenant_name(<<>>, _IsTenantRequired) ->
    {error, {name, <<"Tenant Name should be specified.">>}};
validate_tenant_name(TenantName0, _IsTenantRequired) when erlang:is_binary(TenantName0) ->
    TenantNameLength = length(unicode:characters_to_list(TenantName0)),
    %% We don't expect names, longer than 254 characters
    case TenantNameLength > 254 orelse TenantNameLength =:= 0 of
	true -> {error, {name, erlang:list_to_binary([<<"Tenant name should be not empty. Its length ">>,
			<<"should not exceed 254 characters.">>])}};
	false ->
	    TenantId0 = utils:slugify_object_name(TenantName0),
	    case validate_tenant_id(TenantId0) of
		{error, Reason} -> {error, Reason};
		TenantId1 -> {TenantId1, utils:hex(utils:trim_spaces(TenantName0))}
	    end
    end.

-spec validate_group_id(binary()) -> list().

validate_group_id(<<>>) ->
    validate_group_id([]);
validate_group_id([]) ->
    {error, "Group name error. Its latin representation should not be empty."};
validate_group_id(GroupId0) when erlang:is_binary(GroupId0) ->
    GroupIdLength = byte_size(GroupId0),
    case GroupIdLength > ?MAXIMUM_GROUP_NAME of
	true ->
	    {error, lists:flatten(["Please choose a shorter group name. ",
			"Its latin representation should not exceed ",
			utils:to_list(?MAXIMUM_GROUP_NAME), " characters. '",
			unicode:characters_to_list(GroupId0), "' has ",
			utils:to_list(GroupIdLength), " characters."])};
	false -> unicode:characters_to_list(GroupId0)
    end.

%%
%% Checks if length of group do not exceed certain length,
%% generates ID and returns: {ID, trimmed group name}
%%
-spec validate_group_name(binary(), list()) -> tuple().

validate_group_name(GroupName0, TenantGroups)
	when erlang:is_binary(GroupName0), erlang:is_list(TenantGroups) ->
    GroupId0 = utils:slugify_object_name(GroupName0),
    case validate_group_id(utils:alphanumeric(GroupId0)) of
	{error, Reason} -> {error, Reason};
	GroupId1 ->
	    case TenantGroups of
		[] -> {GroupId1, utils:trim_spaces(GroupName0)};
		_ ->
		    case lists:member(GroupId1, [G#group.id || G <- TenantGroups]) of
			true -> {GroupId1, utils:trim_spaces(GroupName0)};
			false -> {error, lists:flatten(["Group ", unicode:characters_to_list(GroupName0),
							" is not defined for provided tenant."])}
		    end
	    end
    end.

%%
%% Checks if every group in list meets two conditions:
%%	- length of group is less than or equal to ?MAXIMUM_GROUP_NAME
%%	- group name consists of printable characters
%%	  ( those characters that can be used for "slug" )
%%	- ther'a no duplicate groups
%%
validate_groups(undefined, _TenantGroups) -> [];
validate_groups(<<>>, _TenantGroups) -> {error, {groups, <<"At least one group must be specified.">>}};
validate_groups(Groups0, TenantGroups) when erlang:is_binary(Groups0) ->
    Groups1 = [validate_group_name(T, TenantGroups)
	       || T <- binary:split(Groups0, <<",">>, [global]),
	       erlang:byte_size(T) > 0],
    Errors = [element(2, E) || E <- Groups1, element(1, E) =:= error],
    case length(Errors) > 0 of
	true ->
	    {error, {groups, unicode:characters_to_binary(lists:flatten(
		utils:join_list_with_separator(Errors, " ", [])))}};
	false ->
	    case utils:has_duplicates([element(1, I) || I <- Groups1]) of
		true -> {error, {groups, <<"Provided list of groups has duplicate names.">>}};
		false ->
		    [#group{
			id = element(1, G),
			name = utils:hex(element(2, G))
		    } || G <- Groups1]
	    end
    end.

%%
%% Checks if "boolean" flag is specified correctly.
%%
-spec validate_boolean(binary()|atom()|undefined, atom(), boolean()) -> boolean()|tuple().

validate_boolean(undefined, Field, Default)
	when erlang:is_atom(Field), erlang:is_boolean(Default) ->
    Default;
validate_boolean(true, Field, Default)
	when erlang:is_atom(Field), erlang:is_boolean(Default) -> true;
validate_boolean(false, Field, Default)
	when erlang:is_atom(Field), erlang:is_boolean(Default) -> false;
validate_boolean(Flag0, Field, Default)
	when erlang:is_binary(Flag0), erlang:is_atom(Field), erlang:is_boolean(Default) ->
    case byte_size(Flag0) > 5 of
	true -> {error, {Field, <<"Incorrect boolean value.">>}};
	false ->
	    Flag1 = utils:to_atom(Flag0),
	    case Flag1 =:= true orelse Flag1 =:= false of
		true -> Flag1;
		false -> {error, <<"Incorrect boolean value">>}
	    end
    end.

%%
%% Checks the following
%%	- tenant name is correct
%%	- "enabled" flag is boolean.
%%	- at least one group name is specified
%%
validate_post(Body) ->
    case jsx:is_json(Body) of
	{error,badarg} -> {error, <<"JSON parsing error.">>};
	false -> {error, <<"JSON parsing error.">>};
	true ->
	    FieldValues = jsx:decode(Body),
	    TenantName0 = validate_tenant_name(proplists:get_value(<<"name">>, FieldValues), required),
	    Groups0 = validate_groups(proplists:get_value(<<"groups">>, FieldValues), []),
	    IsEnabled0 = validate_boolean(proplists:get_value(<<"enabled">>, FieldValues), enabled, true),

	    Errors = [element(2, F) || F <- [TenantName0, Groups0, IsEnabled0], element(1, F) =:= error],
	    case length(Errors) > 0 of
		true ->
		    {error, Errors};
		false ->
		    {TenantId0, TenantName1} = TenantName0,
		    #tenant{
			id = TenantId0,
			name = TenantName1,
			enabled = IsEnabled0,
			groups = Groups0
		    }
	    end
    end.

validate_patch(not_found, _FieldValues) ->
    {error, {tenant, <<"Tenant not found.">>}};
validate_patch(Tenant, Body) ->
    case jsx:is_json(Body) of
	{error,badarg} -> {error, <<"JSON parsing error.">>};
	false -> {error, <<"JSON parsing error.">>};
	true ->
	    FieldValues = jsx:decode(Body),
	    TenantName0 = validate_tenant_name(proplists:get_value(<<"name">>, FieldValues), not_required),
	    Groups0 = validate_groups(proplists:get_value(<<"groups">>, FieldValues), []),
	    IsEnabled0 = validate_boolean(proplists:get_value(<<"enabled">>, FieldValues), enabled, Tenant#tenant.enabled),

	    Errors = [element(2, F) || F <- [TenantName0, Groups0, IsEnabled0], element(1, F) =:= error],
	    case length(Errors) > 0 of
		true ->
		    {error, Errors};
		false ->
		    {_TenantId0, TenantName1} = TenantName0,
		    TenantName2 =
			case TenantName1 of
			    undefined -> Tenant#tenant.name;
			    _ -> TenantName1
			end,
		    Groups1 = case Groups0 of [] -> Tenant#tenant.groups; _ -> Groups0 end,
		    #tenant{
			id = Tenant#tenant.id,
			name = TenantName2,
			enabled = IsEnabled0,
			groups = Groups1
		    }
	    end
    end.

%%
%% Creates a new "tenant" ( aka "project" )
%% and returns what Cowboy framework expects.
%%
%% TenantId -- slugified tenant name
%%
%% Groups -- list of group records
%%
-spec new_tenant(any(), tenant()) -> any().

new_tenant(Req0, Tenant0) ->
    case riak_api:head_bucket(?SECURITY_BUCKET_NAME) of
	not_found ->
	    riak_api:create_bucket(?SECURITY_BUCKET_NAME);
	_ -> ok
    end,
    PrefixedTenantId = utils:prefixed_object_name(?TENANTS_PREFIX, Tenant0#tenant.id),
    ExistingTenantObject = riak_api:head_object(?SECURITY_BUCKET_NAME, PrefixedTenantId),
    case ExistingTenantObject of
	not_found ->
	    Groups = [{group, [
		      {id, [G#group.id]},
		      {name, [G#group.name]}
	    ]} || G <- Tenant0#tenant.groups],
	    Tenant1 = {record, [
		{id, [Tenant0#tenant.id]},
		{name, [Tenant0#tenant.name]},
		{enabled, [utils:to_list(Tenant0#tenant.enabled)]},
		{groups, Groups}
	    ]},
	    RootElement0 = #xmlElement{name=tenant, content=[Tenant1]},
	    XMLDocument0 = xmerl:export_simple([RootElement0], xmerl_xml),
	    riak_api:put_object(?SECURITY_BUCKET_NAME, ?TENANTS_PREFIX, Tenant0#tenant.id,
		unicode:characters_to_binary(XMLDocument0), [{acl, private}]),
	    Req1 = cowboy_req:set_resp_body(jsx:encode([
		{id, list_to_binary(Tenant0#tenant.id)},
		{name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(Tenant0#tenant.name)))},
		{enabled, utils:to_binary(Tenant0#tenant.enabled)},
		{groups, [
		    [{id, list_to_binary(G#group.id)},
		    {name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(G#group.name)))}]
		    || G <- Tenant0#tenant.groups]}
		]), Req0),
	    {true, Req1, []};
	_ ->
	    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, <<"Tenant exists.">>}]), Req0),
	    {true, Req1, []}
    end.

%%
%% Change existing tenant
%%
-spec edit_tenant(any(), tenant()) -> any().

edit_tenant(Req0, Tenant) ->
    EditedTenant = {record, [
	{id, [utils:to_list(Tenant#tenant.id)]},
	{name, [Tenant#tenant.name]},
	{enabled, [utils:to_list(Tenant#tenant.enabled)]},
	{groups, [{group, [
		{id, [G#group.id]},
		{name, [G#group.name]}
	    ]} || G <- Tenant#tenant.groups]}
	]},
    RootElement0 = #xmlElement{name=tenant, content=[EditedTenant]},
    XMLDocument0 = xmerl:export_simple([RootElement0], xmerl_xml),
    riak_api:put_object(?SECURITY_BUCKET_NAME, ?TENANTS_PREFIX, Tenant#tenant.id,
	unicode:characters_to_binary(XMLDocument0), [{acl, private}]),

    Req1 = cowboy_req:set_resp_body(jsx:encode([
	{id, list_to_binary(Tenant#tenant.id)},
	{name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(Tenant#tenant.name)))},
	{enabled, Tenant#tenant.enabled},
	{groups, [
	    [{id, list_to_binary(G#group.id)},
	    {name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(G#group.name)))}]
	    || G <- Tenant#tenant.groups]}
    ]), Req0),
    {true, Req1, []}.

%%
%% Creates Tenant record
%%
%% Example of expected request data format:
%%
%% {
%%	"name": "Іван Франко",
%%	"enabled": "true",
%%	"groups": "Writers, PhDs"
%% }
%%
handle_post(Req0, _State) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case validate_post(Body) of
	{error, Reasons} ->
	    Req2 = cowboy_req:set_resp_body(jsx:encode([{errors, Reasons}]), Req1),
	    {true, Req2, []};
	Tenant ->
	    case get_tenant(Tenant#tenant.id) of
		not_found ->
		    new_tenant(Req1, Tenant);
		_ ->
		    Req3 = cowboy_req:set_resp_body(jsx:encode([
			{errors, <<"Tenant with this name exists.">>}]), Req1),
		    {true, Req3, []}
	    end
    end.

%%
%% Allows to edit Tenant
%%
patch_resource(Req0, _State) ->
    case cowboy_req:binding(tenant_id, Req0) of
	undefined ->
	    Req1 = cowboy_req:set_resp_body(jsx:encode([
		{error, <<"Tenant ID must be specified in URL.">>}]), Req0),
	    {true, Req1, []};
	TenantId1 ->
	    TenantId2 = erlang:binary_to_list(TenantId1),
	    {ok, Body, Req1} = cowboy_req:read_body(Req0),
	    case validate_patch(get_tenant(TenantId2), Body) of
		{error, Reasons} ->
		    Req2 = cowboy_req:set_resp_body(jsx:encode([{errors, Reasons}]), Req1),
		    {true, Req2, []};
		Tenant ->
		    edit_tenant(Req1, Tenant)
	    end
    end.

delete_resource(Req, _State) ->
    case cowboy_req:binding(tenant_id, Req) of
	undefined ->
	    Req1 = cowboy_req:set_resp_body(jsx:encode([
		{error, <<"Tenant ID must be specified in URL.">>}]), Req),
	    {true, Req1, []};
	TenantId1 ->
	    TenantId2 = erlang:binary_to_list(TenantId1),
	    PrefixedTenantId = utils:prefixed_object_name(?TENANTS_PREFIX, TenantId2),
	    riak_api:delete_object(?SECURITY_BUCKET_NAME, PrefixedTenantId)
    end.

delete_completed(Req0, State) ->
    Req1 = cowboy_req:set_resp_body("{\"status\": \"ok\"}", Req0),
    {true, Req1, State}.
