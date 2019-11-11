%%
%% Stores information on performed actions.
%%
-module(action_log).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2,
         content_types_accepted/2,  forbidden/2, resource_exists/2,
	 previously_existed/2]).
-export([add_record/3, strip_hidden_part/1, fetch_full_object_history/2]).

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
    PrefixedActionLogFilename = utils:prefixed_object_key(
	Prefix, ?RIAK_ACTION_LOG_FILENAME),

    Options = [{acl, public_read}],  % TODO: public_read
    case riak_api:get_object(BucketId, PrefixedActionLogFilename) of
	not_found ->
	    %% Create .riak_action_log.xml
	    RootElement0 = #xmlElement{name=action_log, content=[Record1]},
	    XMLDocument0 = xmerl:export_simple([RootElement0], xmerl_xml),
	    riak_api:put_object(BucketId, Prefix, ?RIAK_ACTION_LOG_FILENAME,
		unicode:characters_to_binary(XMLDocument0), Options);
	ExistingObject ->
	    XMLDocument1 = utils:to_list(proplists:get_value(content, ExistingObject)),
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

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"POST">>], Req, State}.


content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

content_types_accepted(Req, State) ->
    case cowboy_req:method(Req) of
	<<"POST">> ->
	    {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State}
    end.

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

%%
%% Returns pseudo-directory action log
%%
get_action_log(Req0, State, BucketId, Prefix) ->
    PrefixedActionLogFilename = utils:prefixed_object_key(
	Prefix, ?RIAK_ACTION_LOG_FILENAME),
    ExistingObject0 = riak_api:head_object(BucketId,
	PrefixedActionLogFilename),
    case ExistingObject0 of
	not_found -> js_handler:not_found(Req0);
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
%% Retrieves list of objects from Riak CS (all pages).
%%
fetch_full_object_history(BucketId, GUID)
	when erlang:is_list(BucketId), erlang:is_list(GUID) ->
    fetch_full_object_history(BucketId, GUID, [], undefined).

fetch_full_object_history(BucketId, GUID, ObjectList0, Marker0)
	when erlang:is_list(BucketId), erlang:is_list(ObjectList0), erlang:is_list(GUID) ->
    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID++"/"),
    RiakResponse = riak_api:list_objects(BucketId, [{prefix, RealPrefix}, {marker, Marker0}]),
    case RiakResponse of
	not_found -> [];
	_ ->
	    ObjectList1 = [proplists:get_value(key, I) || I <- proplists:get_value(contents, RiakResponse)],
	    Marker1 = proplists:get_value(next_marker, RiakResponse),
	    case Marker1 of
		undefined -> ObjectList0 ++ ObjectList1;
		[] -> ObjectList0 ++ ObjectList1;
		NextMarker ->
		    NextPart = fetch_full_object_history(BucketId, GUID, ObjectList0 ++ ObjectList1, NextMarker),
		    ObjectList0 ++ NextPart
	    end
    end.

%%
%% Returns the list of available versions of object.
%%
get_object_changelog(Req0, State, BucketId, Prefix, ObjectKey) ->
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey),
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	not_found -> js_handler:not_found(Req0);
	RiakResponse0 ->
	    GUID = proplists:get_value("x-amz-meta-guid", RiakResponse0),
	    ObjectList0 = [list_handler:parse_object_record(riak_api:head_object(BucketId, I), [])
			   || I <- fetch_full_object_history(BucketId, GUID)],
	    ObjectList1 = lists:map(
		fun(I) ->
		    AuthorId = utils:to_binary(proplists:get_value(author_id, I)),
		    AuthorName = utils:unhex(utils:to_binary(proplists:get_value(author_name, I))),
		    AuthorTel = utils:to_binary(proplists:get_value(author_tel, I)),
		    [{author_id, AuthorId},
		     {author_name, AuthorName},
		     {author_tel, AuthorTel},
		     {last_modified_utc, proplists:get_value(last_modified_utc, I)}]
		end, ObjectList0),
	    Output = jsx:encode(ObjectList1),
	    {Output, Req0, State}
    end.

to_json(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),

    ParsedQs = cowboy_req:parse_qs(Req0),
    case proplists:get_value(<<"object_key">>, ParsedQs) of
        undefined -> get_action_log(Req0, State, BucketId, Prefix);
        ObjectKey0 ->
	    ObjectKey1 = unicode:characters_to_list(ObjectKey0),
	    get_object_changelog(Req0, State, BucketId, Prefix, ObjectKey1)
    end.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, _State) ->
    case utils:check_token(Req0) of
	not_found -> js_handler:forbidden(Req0, 28);
	expired -> js_handler:forbidden(Req0, 38);
	User -> {false, Req0, [{user,User}]}
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    BucketId = erlang:binary_to_list(cowboy_req:binding(bucket_id, Req0)),
    User = proplists:get_value(user, State),
    UserBelongsToGroup =
	case User of
	    undefined -> false;
	    _ ->
		Groups = User#user.groups,
		TenantId = User#user.tenant_id,
		lists:any(
		    fun(Group) ->
			utils:is_bucket_belongs_to_group(BucketId, TenantId, Group#group.id)
		    end, Groups)
	end,
    case UserBelongsToGroup of
	true ->
	    ParsedQs = cowboy_req:parse_qs(Req0),
	    case list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, ParsedQs)) of
		{error, Number} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req0),
		    {false, Req1, []};
		Prefix ->
		    {true, Req0, [{user, User},
			  {bucket_id, BucketId},
			  {prefix, Prefix}]}
	    end;
	false -> {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.


%%
%% Removes stuff, regular user is not supposed to see.
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
