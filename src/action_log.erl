-module(action_log).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2, forbidden/2,
	 resource_exists/2, previously_existed/2, add_record/3]).

-include_lib("xmerl/include/xmerl.hrl").

-include("riak.hrl").
-include("action_log.hrl").

-import(xmerl_xs, 
    [ xslapply/2, value_of/1, select/2, built_in_rules/2 ]).

%%
%% Adds action log record to XML document, stored in Riak CS
%%
-spec add_record(string(), string(), #riak_action_log_record{}) -> proplist().

add_record(BucketName, Prefix, Record0) ->
    Record1 = {record, [
	{action, [Record0#riak_action_log_record.action]},
	{details, [Record0#riak_action_log_record.details]},
	{user_name, [Record0#riak_action_log_record.user_name]},
	{tenant_name, [Record0#riak_action_log_record.tenant_name]},
	{timestamp, [Record0#riak_action_log_record.timestamp]}
	]},
    PrefixedActionLogFilename = utils:prefixed_object_name(
	Prefix, ?RIAK_ACTION_LOG_FILENAME),

    ExistingObject0 = riak_api:head_object(BucketName,
	PrefixedActionLogFilename),
    Options = [{acl, public_read}],  % TODO: public_read
    case ExistingObject0 of
	not_found ->
	    %% Create .riak_action_log.xml
	    RootElement0 = #xmlElement{name=action_log, content=[Record1]},
	    XMLDocument0 = xmerl:export_simple([RootElement0], xmerl_xml),
	    riak_api:put_object(BucketName, Prefix, ?RIAK_ACTION_LOG_FILENAME,
		unicode:characters_to_binary(XMLDocument0), Options);
	_ ->
	    ExistingObject1 = riak_api:get_object(BucketName, PrefixedActionLogFilename),
	    XMLDocument1 = utils:to_list(proplists:get_value(content, ExistingObject1)),
	    {RootElement1, _} = xmerl_scan:string(XMLDocument1),
	    #xmlElement{content=Content} = RootElement1,
	    NewContent = [Record1]++Content,
	    NewRootElement = RootElement1#xmlElement{content=NewContent},
	    XMLDocument2 = xmerl:export_simple([NewRootElement], xmerl_xml),

	    riak_api:put_object(BucketName, Prefix, ?RIAK_ACTION_LOG_FILENAME,
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
    [
      {action, lists:flatten(xslapply(fun template/1, select("action", E)))},
      {details, lists:flatten(xslapply(fun template/1, select("details", E)))},
      {user_name, lists:flatten(xslapply(fun template/1, select("user_name", E)))},
      {tenant_name, lists:flatten(xslapply(fun template/1, select("tenant_name", E)))},
      {timestamp, lists:flatten(xslapply(fun template/1, select("timestamp", E)))}
    ];
template(E) -> built_in_rules(fun template/1, E).

to_json(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    Prefix = proplists:get_value(prefix, State),

    PrefixedActionLogFilename = utils:prefixed_object_name(
	Prefix, ?RIAK_ACTION_LOG_FILENAME),
    ExistingObject0 = riak_api:head_object(BucketName,
	PrefixedActionLogFilename),
    case ExistingObject0 of
	not_found ->
	    {["[]"], Req0, State};
	_ ->
	    ExistingObject1 = riak_api:get_object(BucketName, PrefixedActionLogFilename),
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
%%
forbidden(Req0, _State) ->
    Token = case cowboy_req:binding(token, Req0) of
	undefined -> undefined;
	TokenValue -> binary_to_list(TokenValue)
    end,
    case keystone_api:check_token(Token) of
	not_found ->
	    {true, Req0, []};
	Ids ->
	    {false, Req0, Ids}
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    BucketName = binary_to_list(cowboy_req:binding(bucket_name, Req0)),
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),

    case utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName) of
	true ->
	    ParsedQs = cowboy_req:parse_qs(Req0),
	    Prefix0 = case proplists:get_value(<<"prefix">>, ParsedQs) of
		undefined -> undefined;
		    Prefix1 ->
			case utils:is_valid_hex_prefix(Prefix1) of
			    true ->
				binary_to_list(unicode:characters_to_binary(Prefix1));
			    false ->
				undefined
			end
		end,
	    {true, Req0, [{user_name, UserName},
			  {tenant_name, TenantName},
			  {bucket_name, BucketName},
			  {prefix, Prefix0}]};
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
