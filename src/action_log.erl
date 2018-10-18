%%
%% Stores information on performed actions.
%%
-module(action_log).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2, forbidden/2,
    resource_exists/2, previously_existed/2, add_record/3, strip_hidden_part/1]).

-include_lib("xmerl/include/xmerl.hrl").

-include("riak.hrl").
-include("user.hrl").
-include("action_log.hrl").

-import(xmerl_xs, 
    [ xslapply/2, value_of/1, select/2, built_in_rules/2 ]).

%%
%% Adds action log record to XML document, stored in Riak CS
%%
-spec add_record(string(), string(), #riak_action_log_record{}) -> proplist().

add_record(BucketId, Prefix, Record0) ->
    Record1 = {record, [
	{action, [Record0#riak_action_log_record.action]},
	{details, [Record0#riak_action_log_record.details]},
	{user_name, [Record0#riak_action_log_record.user_name]},
	{tenant_name, [Record0#riak_action_log_record.tenant_name]},
	{timestamp, [Record0#riak_action_log_record.timestamp]}
	]},
    PrefixedActionLogFilename = utils:prefixed_object_name(
	Prefix, ?RIAK_ACTION_LOG_FILENAME),

    ExistingObject0 = riak_api:head_object(BucketId,
	PrefixedActionLogFilename),
    Options = [{acl, public_read}],  % TODO: public_read
    case ExistingObject0 of
	not_found ->
	    %% Create .riak_action_log.xml
	    RootElement0 = #xmlElement{name=action_log, content=[Record1]},
	    XMLDocument0 = xmerl:export_simple([RootElement0], xmerl_xml),
	    riak_api:put_object(BucketId, Prefix, ?RIAK_ACTION_LOG_FILENAME,
		unicode:characters_to_binary(XMLDocument0), Options);
	_ ->
	    ExistingObject1 = riak_api:get_object(BucketId, PrefixedActionLogFilename),
	    XMLDocument1 = utils:to_list(proplists:get_value(content, ExistingObject1)),
	    {RootElement1, _} = xmerl_scan:string(XMLDocument1),
	    #xmlElement{content=Content} = RootElement1,
	    NewContent = [Record1]++Content,
	    NewRootElement = RootElement1#xmlElement{content=NewContent},
	    XMLDocument2 = xmerl:export_simple([NewRootElement], xmerl_xml),

	    riak_api:put_object(BucketId, Prefix, ?RIAK_ACTION_LOG_FILENAME,
		unicode:characters_to_binary(XMLDocument2), Options)
    end.

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

%%
%% Helps to transform action log record from XML to JSON
%%
template(E = #xmlElement{name=action_log}) ->
    xslapply(fun template/1, E);
template(E = #xmlElement{name=record}) ->
    Details = lists:flatten(xslapply(fun template/1, select("details", E))),
    UserName = lists:flatten(xslapply(fun template/1, select("user_name", E))),
    TenantName = lists:flatten(xslapply(fun template/1, select("tenant_name", E))),
    [
      {action, erlang:list_to_binary(lists:flatten(xslapply(fun template/1, select("action", E))))},
      {details, unicode:characters_to_binary(Details)},
      {user_name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(UserName)))},
      {tenant_name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(TenantName)))},
      {timestamp, erlang:list_to_binary(lists:flatten(xslapply(fun template/1, select("timestamp", E))))}
    ];
template(E) -> built_in_rules(fun template/1, E).

to_json(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),

    PrefixedActionLogFilename = utils:prefixed_object_name(
	Prefix, ?RIAK_ACTION_LOG_FILENAME),
    ExistingObject0 = riak_api:head_object(BucketId,
	PrefixedActionLogFilename),
    case ExistingObject0 of
	not_found ->
	    {["[]"], Req0, State};
	_ ->
	    ExistingObject1 = riak_api:get_object(BucketId, PrefixedActionLogFilename),
	    XMLContent0 = utils:to_list(proplists:get_value(content, ExistingObject1)),
	    {XMLContent1, _} = xmerl_scan:string(XMLContent0),

	    %% filter out "\n" characters and serialize records to JSON
	    Output = jsx:encode([R || R <- template(XMLContent1),
		proplists:is_defined(action, R) =:= true]),
	    {Output, Req0, State}
    end.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, _State) ->
    case utils:check_token(Req0) of
	not_found -> {true, Req0, []};
	expired -> {true, Req0, []};
	User -> {false, Req0, User}
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    BucketId = erlang:binary_to_list(cowboy_req:binding(bucket_id, Req0)),
    Groups = State#user.groups,
    TenantId = State#user.tenant_id,
    UserBelongsToGroup = lists:any(fun(Group) ->
	utils:is_bucket_belongs_to_group(BucketId, TenantId, Group#group.id) end,
	Groups),
    case UserBelongsToGroup of
	true ->
	    ParsedQs = cowboy_req:parse_qs(Req0),
	    case list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, ParsedQs)) of
		{error, Number} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req0),
		    {false, Req1, []};
		Prefix ->
		    {true, Req0, [{user, State},
			  {bucket_id, BucketId},
			  {prefix, Prefix}]}
	    end;
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.


%%
%% Removes stuff user is not supposed to see.
%%
-spec strip_hidden_part(string()) -> string()|undefined.

strip_hidden_part(Name) when erlang:is_list(Name) ->
    case utils:is_hidden_object({key, Name}) of
	true -> undefined;
	false ->
	    case string:str(Name, "-deleted-") of
		0 -> Name;
		_ -> undefined
	    end
    end.
