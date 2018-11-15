%%
%% Allows to delete object or to get its attributes.
%%
-module(object_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2,
    allowed_methods/2, is_authorized/2, forbidden/2,
    resource_exists/2, previously_existed/2,
    delete_resource/2, delete_completed/2]).

-include("riak.hrl").
-include("user.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"DELETE">>], Req, State}.

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

to_json(Req0, State) ->
    ObjectName0 = proplists:get_value(object_name, State),
    Prefix0 = proplists:get_value(prefix, State),
    case proplists:get_value(object_meta, State) of
	undefined -> {<<"[]">>, Req0, []};  %% Pseudo-directory
	ObjectMeta ->
	    UploadTimestamp = proplists:get_value(upload_time, ObjectMeta),
	    ModifiedTime = proplists:get_value(last_modified_utc, ObjectMeta),
	    OrigName = proplists:get_value(orig_name, ObjectMeta),
	    ContentType = erlang:list_to_binary(proplists:get_value(content_type, ObjectMeta)),
	    ContentLength = proplists:get_value(bytes, ObjectMeta),
	    AccessToken =
		case utils:starts_with(ContentType, <<"image/">>) of
		    false -> <<>>;
		    true -> proplists:get_value(access_token, ObjectMeta, <<"">>)
		end,
	    Prefix1 =
		case Prefix0 of
		    undefined -> <<>>;
		    _ -> Prefix0
		end,
	    {jsx:encode([
		{prefix, unicode:characters_to_binary(Prefix1)},
		{key, ObjectName0},
		{orig_name, OrigName},
		{last_modified_utc, ModifiedTime},
		{uploaded, UploadTimestamp},
		{content_length, ContentLength},
		{content_type, ContentType},
		{access_token, AccessToken}
	    ]), Req0, []}
    end.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, _State) ->
    case utils:check_token(Req0) of
	undefined -> {{false, <<"Token">>}, Req0, []};
	not_found -> {{false, <<"Token">>}, Req0, []};
	expired -> {{false, <<"Token">>}, Req0, []};
	User -> {true, Req0, User}
    end.

%%
%% Checks if user has access
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    BucketId = erlang:binary_to_list(cowboy_req:binding(bucket_id, Req0)),
    case utils:is_valid_bucket_id(BucketId, State#user.tenant_id) of
	true ->
	    UserBelongsToGroup = lists:any(fun(Group) ->
		utils:is_bucket_belongs_to_group(BucketId, State#user.tenant_id, Group#group.id) end,
		State#user.groups),
	    case UserBelongsToGroup of
		false -> {true, Req0, []};
		true -> {false, Req0, [{user, State}, {bucket_id, BucketId}]}
	    end;
	false -> {true, Req0, []}
    end.

-spec get_object_meta(string(), string(), binary()) -> proplist()|not_found|pseudo_directory.

get_object_meta(BucketId, Prefix0, ObjectKey0) ->
    List0 = riak_index:get_index(BucketId, Prefix0),
    case riak_index:get_object_record(ObjectKey0, List0) of
	[] ->
	    %% Check if we learned of existence of such pseudo-directory
	    ObjectKey1 =
		case utils:ends_with(ObjectKey0, <<"/">>) of
		    true -> ObjectKey0;
		    false -> << ObjectKey0/binary, <<"/">>/binary >>
		end,
	    ExistingPrefixes = [proplists:get_value(prefix, P) || P <- proplists:get_value(dirs, List0)],
	    case lists:member(ObjectKey1, ExistingPrefixes) of
		false -> not_found;
		true -> pseudo_directory
	    end;
	ObjectRecord -> ObjectRecord
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    case cowboy_req:method(Req0) of
	<<"GET">> ->
	    ParsedQs = cowboy_req:parse_qs(Req0),
	    Prefix0 = proplists:get_value(<<"prefix">>, ParsedQs),
	    case list_handler:validate_prefix(BucketId, Prefix0) of
		{error, Number} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req0),
		    {false, Req1, []};
		Prefix1 ->
		    ObjectKey0 = proplists:get_value(<<"object_name">>, ParsedQs),
		    State1 = [{prefix, Prefix1}, {object_name, ObjectKey0}],
		    case get_object_meta(BucketId, Prefix1, ObjectKey0) of
			not_found -> {false, Req0, []};
			pseudo_directory -> {true, Req0, State0 ++ State1};
			ObjectMeta0 -> {true, Req0, State0 ++ State1 ++ [{object_meta, ObjectMeta0}]}
		    end
	    end;
	<<"DELETE">> ->
	    {ok, Attrs0, Req2} = cowboy_req:read_body(Req0),
	    Attrs1 = jsx:decode(Attrs0),
	    case list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, Attrs1)) of
		{error, Number} ->
		    Req3 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req2),
		    {false, Req3, []};
		Prefix2 ->
		    case proplists:get_value(<<"object_name">>, Attrs1) of
			undefined ->
			    Req4 = cowboy_req:set_resp_body(jsx:encode([{error, 8}]), Req2),
			    {false, Req4, []};
			ObjectKey1 ->
			    State2 = [{prefix, Prefix2}, {object_name, ObjectKey1}],
			    case get_object_meta(BucketId, Prefix2, ObjectKey1) of
				not_found -> {false, Req2, []};
				pseudo_directory -> {true, Req2, State0 ++ State2};
				ObjectMeta1 -> {true, Req2, State0 ++ State2 ++ [{object_meta, ObjectMeta1}]}
			    end
		    end
	    end
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.

delete_resource(Req, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    ObjectName = proplists:get_value(object_name, State),
    Prefix = proplists:get_value(prefix, State),

    %% Check if parent directory exists
    PrefixedIndexFilename = utils:prefixed_object_name(Prefix, ?RIAK_INDEX_FILENAME),
    case riak_api:head_object(BucketId, PrefixedIndexFilename) of
	not_found -> {true, Req, []};
	_ ->
	    delete(State, BucketId, Prefix, ObjectName),
	    {true, Req, []}
    end.

delete_completed(Req0, State) ->
    Req1 = cowboy_req:set_resp_body("{\"status\": \"ok\"}", Req0),
    {true, Req1, State}.

delete(_State, _BucketId, "", "/") -> ok;
delete(State, BucketId, Prefix, ObjectName0) ->
    User = proplists:get_value(user, State),
    ObjectName1 = erlang:binary_to_list(ObjectName0),
    ActionLogRecord0 = #riak_action_log_record{
	action="delete",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()])
    },
    case proplists:get_value(object_meta, State) of
	undefined ->
	    %% Rename directory if ther's no "-deleted-" substring in it.
	    %%     - mark directory as deleted
	    %%     - mark all nested objects as deleted
	    %%     - leave record in action log
	    DstDirectoryName0 = unicode:characters_to_list(utils:unhex(ObjectName0)),
	    Timestamp = utils:timestamp(),
	    case string:str(DstDirectoryName0, "-deleted-") of
		0 ->
		    DstDirectoryName1 = lists:concat([DstDirectoryName0, "-deleted-", Timestamp]),
		    DstDirectoryName2 = utils:hex(unicode:characters_to_binary(DstDirectoryName1)),
		    rename_handler:rename_pseudo_directory(BucketId, Prefix, ObjectName1, unicode:characters_to_binary(DstDirectoryName1)),
		    riak_index:update(BucketId, Prefix, [{to_delete, [{erlang:list_to_binary(DstDirectoryName2++"/"), Timestamp}]}]);
		_ ->
		    riak_index:update(BucketId, Prefix, [{to_delete, [{erlang:list_to_binary(ObjectName0), Timestamp}]}])
	    end,
	    Summary0 = lists:flatten([["Deleted directory \""], DstDirectoryName0 ++ ["/\"."]]),
	    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
	    action_log:add_record(BucketId, Prefix, ActionLogRecord1);
	ObjectMeta ->
	    %% Mark object as deleted
	    riak_index:update(BucketId, Prefix, [{to_delete, [{ObjectName0, utils:timestamp()}]}]),

	    %% Leave record in action log using object's original name
	    case ObjectMeta of
		not_found -> not_found;
		_ ->
		    UnicodeObjectName0 = proplists:get_value(orig_name, ObjectMeta),
		    UnicodeObjectName1 = unicode:characters_to_list(UnicodeObjectName0),
		    Summary0 = lists:flatten([["Delete \""], [UnicodeObjectName1], ["\""]]),
		    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
		    action_log:add_record(BucketId, Prefix, ActionLogRecord1),
		    ok
	    end
    end.
