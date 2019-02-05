%%
%% List handler allows the following requests
%%
%% GET
%%	Returns contetnts of index.etf
%%
%% PATCH
%%	Allows to mark objects as undeleted
%%
%% POST
%%	Create directory
%%
-module(list_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
    to_json/2, allowed_methods/2, forbidden/2, is_authorized/2,
    resource_exists/2, previously_existed/2, patch_resource/2,
    validate_post/3, create_pseudo_directory/2, handle_post/2,
    validate_prefix/2]).

-export([validate_delete/2, get_pseudo_directories/3,
    get_objects/2, format_delete_results/3, delete_resource/2,
    delete_completed/2, delete_pseudo_directory/5, delete_objects/5]).

-include("riak.hrl").
-include("user.hrl").
-include("general.hrl").
-include("action_log.hrl").


init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PATCH">>, <<"POST">>, <<"DELETE">>], Req, State}.


content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

content_types_accepted(Req, State) ->
    case cowboy_req:method(Req) of
	<<"PATCH">> ->
	    {[{{<<"application">>, <<"json">>, '*'}, patch_resource}], Req, State};
	<<"POST">> ->
	    {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State}
    end.

to_json(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    Prefix0 = proplists:get_value(prefix, State),
    %% TODO: replace UTC time with DVV
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    UTCTimestamp = calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}}) - 62167219200,
    case riak_api:head_bucket(BucketId) of
	not_found ->
	    %% Bucket is valid, but it do not exist yet
	    riak_api:create_bucket(BucketId),
	    {jsx:encode([{list, []}, {dirs, []}, {server_utc, UTCTimestamp}]), Req0, []};
	_ ->
	    Prefix1 = utils:to_lower(Prefix0),
	    PrefixedIndexFilename = utils:prefixed_object_key(Prefix1, ?RIAK_INDEX_FILENAME),
	    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
		not_found ->
		    case Prefix1 of
			undefined -> {jsx:encode([{list, []}, {dirs, []}, {server_utc, UTCTimestamp}]), Req0, []};
			_ -> js_handler:not_found(Req0)
		    end;
		RiakResponse ->
		    List0 = erlang:binary_to_term(proplists:get_value(content, RiakResponse)),
			{jsx:encode([{list, proplists:get_value(list, List0)},
			{dirs, proplists:get_value(dirs, List0)},
			{server_utc, UTCTimestamp},
			{uncommitted, proplists:get_value(uncommitted, List0)}
		    ]), Req0, []}
	    end
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
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
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

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix0 = proplists:get_value(<<"prefix">>, ParsedQs),
    case utils:is_valid_hex_prefix(Prefix0) of
	false -> {false, Req0, []};
	true -> {true, Req0, State++[{prefix, Prefix0}]}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.

patch_resource(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    {ok, Attrs0, Req1} = cowboy_req:read_body(Req0),
    Attrs1 = jsx:decode(Attrs0),
    Prefix0 = proplists:get_value(<<"prefix">>, Attrs1),
    case proplists:get_value(<<"undelete">>, Attrs1) of
	undefined -> {true, Req1, []};
	ObjectsList0 ->
	    ObjectsList1 = [utils:to_list(N) || N <- ObjectsList0, byte_size(N) < 255],
	    case indexing:update(BucketId, Prefix0, [{undelete, ObjectsList1}]) of
		lock -> js_handler:too_many(Req0);
		_ -> {true, Req1, []}
	    end
    end.

%%
%% Validates prefix from POST request.
%%
-spec validate_prefix(undefined|list(), undefined|list()) -> list()|{error, integer}.

validate_prefix(undefined, _Prefix) -> undefined;
validate_prefix(_BucketId, undefined) -> undefined;
validate_prefix(BucketId, Prefix0) when erlang:is_list(BucketId),
	erlang:is_binary(Prefix0) orelse Prefix0 =:= undefined ->
    case utils:is_valid_hex_prefix(Prefix0) of
	true ->
	    %% Check if prefix exists
	    Prefix1 = utils:to_lower(Prefix0),
	    case Prefix1 of
		undefined -> undefined;
		_ ->
		    PrefixedIndexFilename = utils:prefixed_object_key(Prefix1, ?RIAK_INDEX_FILENAME),
		    case riak_api:head_object(BucketId, PrefixedIndexFilename) of
			not_found -> {error, 11};
			_ -> Prefix1
		    end
	    end;
	false -> {error, 11}
    end.

validate_directory_name(_BucketId, _Prefix, null) -> {error, 12};
validate_directory_name(_BucketId, _Prefix, undefined) -> {error, 12};
validate_directory_name(BucketId, Prefix0, DirectoryName0)
	when erlang:is_list(BucketId),
	     erlang:is_binary(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_binary(DirectoryName0) ->
    case utils:is_valid_object_key(DirectoryName0) of
	false -> {error, 12};
	true ->
	    case validate_prefix(BucketId, Prefix0) of
		{error, Number} -> {error, Number};
		Prefix1 ->
		    IndexContent = indexing:get_index(BucketId, Prefix1),
		    case indexing:pseudo_directory_exists(IndexContent, DirectoryName0) of
			true -> {error, 10};
			false ->
			    case indexing:object_exists(IndexContent, DirectoryName0) of
				true -> {error, 29};
				false ->
				    HexDirectoryName = utils:hex(DirectoryName0),
				    PrefixedDirectoryName = utils:prefixed_object_key(Prefix1, HexDirectoryName),
				    {PrefixedDirectoryName, Prefix1, DirectoryName0}
			    end
		    end
	    end
    end.

%%
%% Checks if directory name and prefix are correct
%%
validate_post(Req, Body, BucketId) ->
    case jsx:is_json(Body) of
	{error, badarg} -> js_handler:bad_request(Req, 21);
	false -> js_handler:bad_request(Req, 21);
	true ->
	    FieldValues = jsx:decode(Body),
	    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
	    DirectoryName0 = proplists:get_value(<<"directory_name">>, FieldValues),
	    case validate_directory_name(BucketId, Prefix0, DirectoryName0) of
		{PrefixedDirectoryName, Prefix1, DirectoryName1} ->
		    {PrefixedDirectoryName, Prefix1, DirectoryName1};
		{error, Number} -> js_handler:bad_request(Req, Number)
	    end
    end.

%%
%% Slugifies object name and performs indexation
%%
%% Example: "something" is uploaded as 736f6d657468696e67/.rriak_index.etf
%%
-spec create_pseudo_directory(any(), proplist()) -> any().

create_pseudo_directory(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    PrefixedDirectoryName = proplists:get_value(prefixed_directory_name, State),
    DirectoryName = unicode:characters_to_list(proplists:get_value(directory_name, State)),
    Prefix = proplists:get_value(prefix, State),

    case indexing:update(BucketId, PrefixedDirectoryName++"/") of
       lock -> js_handler:too_many(Req0);
       _ ->
           case indexing:update(BucketId, Prefix) of
               lock -> js_handler:too_many(Req0);
               _ ->
                   User = proplists:get_value(user, State),
                   ActionLogRecord0 = #riak_action_log_record{
                       action="mkdir",
                       user_name=User#user.name,
                       tenant_name=User#user.tenant_name,
                       timestamp=io_lib:format("~p", [utils:timestamp()])
                   },
                   Summary0 = lists:flatten([["Created directory \""], DirectoryName ++ ["/\"."]]),
                   ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
                   action_log:add_record(BucketId, Prefix, ActionLogRecord1),
                   {true, Req0, []}
           end
    end.

%%
%% Creates pseudo directory
%%
handle_post(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    case riak_api:head_bucket(BucketId) of
    	not_found -> riak_api:create_bucket(BucketId);
	_ -> ok
    end,
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case validate_post(Req1, Body, BucketId) of
	{true, Req2, []} -> {true, Req2, []};  % error
	{PrefixedDirectoryName, Prefix, DirectoryName} ->
	    create_pseudo_directory(Req1, [
		{prefixed_directory_name, PrefixedDirectoryName},
		{prefix, Prefix},
		{directory_name, DirectoryName},
		{user, proplists:get_value(user, State0)},
		{bucket_id, BucketId}])
    end.

%%
%% Extract pseudo-directories from provided object keys
%%
get_pseudo_directories(Prefix0, List0, ObjectKeys) ->
    lists:filter(
	fun(I) ->
	    case utils:ends_with(I, <<"/">>) of
		true ->
		    PrefixedObjectKey = utils:prefixed_object_key(Prefix0, erlang:binary_to_list(I)),
		    indexing:pseudo_directory_exists(List0, PrefixedObjectKey);
		false -> false
	    end
	end, ObjectKeys).

get_objects(List0, ObjectKeys) ->
    lists:filter(
	fun(I) ->
	    case indexing:get_object_record(List0, I) of
		[] -> false;
		_ -> true
	    end
	end, ObjectKeys).

%%
%% Checks if valid bucket id and JSON request provided.
%%
basic_delete_validations(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    case riak_api:head_bucket(BucketId) of
	not_found -> {error, 7};
	_ ->
	    {ok, Body, Req1} = cowboy_req:read_body(Req0),
	    case jsx:is_json(Body) of
		{error, badarg} -> {error, 21};
		false -> {error, 21};
		true ->
		    FieldValues = jsx:decode(Body),
		    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
		    ObjectKeys = proplists:get_value(<<"object_keys">>, FieldValues),
		    case validate_prefix(BucketId, Prefix0) of
			{error, Number} -> {error, Number};
			Prefix1 -> {Req1, BucketId, Prefix1, ObjectKeys}
		    end
	    end
    end.

%%
%% Checks if list of objects or pseudo-directories provided
%%
validate_delete(Req0, State) ->
    case basic_delete_validations(Req0, State) of
	{error, Number} -> {error, Number};
	{Req1, BucketId, Prefix, ObjectKeys0} ->
	    List0 = indexing:get_index(BucketId, Prefix),
	    PseudoDirectories = get_pseudo_directories(Prefix, List0, ObjectKeys0),
	    ObjectKeys1 = get_objects(List0, ObjectKeys0),
	    case length(PseudoDirectories) =:= 0 andalso length(ObjectKeys1) =:= 0 of
		true -> {error, 34};
		false -> {Req1, BucketId, Prefix, PseudoDirectories, ObjectKeys1}
	    end
    end.

delete_pseudo_directory(_BucketId, "", "/", _ActionLogRecord, _Timestamp) -> {dir_name, "/"};
delete_pseudo_directory(BucketId, Prefix, ObjectKey0, ActionLogRecord0, Timestamp) ->
    ObjectKey1 = erlang:binary_to_list(ObjectKey0),
    %% If ther's no "-deleted-" substring in pseudo-directory.
    %%     - mark directory as deleted
    %%     - mark all nested objects as deleted
    %%     - leave record in action log
    DstDirectoryName0 = unicode:characters_to_list(utils:unhex(ObjectKey0)),
    case string:str(DstDirectoryName0, "-deleted-") of
	0 ->
	    DstDirectoryName1 = lists:concat([DstDirectoryName0, "-deleted-", Timestamp]),
	    %% rename_pseudo_directory() marks pseudo-directory as "uncommited".
	    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey1),
	    rename_handler:rename_pseudo_directory(BucketId, Prefix, PrefixedObjectKey,
		unicode:characters_to_binary(DstDirectoryName1), ActionLogRecord0);
	_ ->
	    %% "-deleted-" substring was found in directory name. No need to add another tag.
	    case indexing:update(BucketId, Prefix, [{to_delete,
				    [{ObjectKey0, Timestamp}]}]) of
		lock -> lock;
		_ ->
		    Summary0 = lists:flatten([["Deleted directory \""], DstDirectoryName0 ++ ["/\"."]]),
		    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
		    action_log:add_record(BucketId, Prefix, ActionLogRecord1),
		    {dir_name, ObjectKey0}
	    end
    end.

delete_objects(BucketId, Prefix, ObjectKeys0, ActionLogRecord0, Timestamp) ->
    %% Mark object as deleted
    ObjectKeys1 = [{K, Timestamp} || K <- ObjectKeys0],
    case indexing:update(BucketId, Prefix, [{to_delete, ObjectKeys1}]) of
	lock -> lock;
	List0 ->
	    %% Leave record in action log
	    OrigNames = lists:map(
		fun(I) ->
		    case indexing:get_object_record([{list, List0}], I) of
			[] -> [];
			ObjectMeta ->
			    UnicodeObjectName0 = proplists:get_value(orig_name, ObjectMeta),
			    [unicode:characters_to_list(UnicodeObjectName0), " "]
		    end
		end, ObjectKeys0),
	    Summary0 = lists:flatten([["Deleted \""], OrigNames, ["\""]]),
	    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
	    action_log:add_record(BucketId, Prefix, ActionLogRecord1),
	    ok
    end.

%%
%% Takes into account result of DELETE operation on list of objects
%% and result of pseudo-directory processing.
%%
format_delete_results(Req0, PseudoDirectoryResults, ObjectsResult) ->
    ErrorResults = lists:filter(
	fun(I) ->
	    case I of
		{error, _} -> true;
		_ -> false
	    end
	end, PseudoDirectoryResults),
    AcceptedResults = lists:filter(
	fun(I) ->
	    case I of
		{accepted, _} -> true;
		_ -> false
	    end
	end, PseudoDirectoryResults),
    case length(ErrorResults) of
	0 ->
	    case length(AcceptedResults) of
		0 ->
		    LockErrors = [I || I <- PseudoDirectoryResults, I =:= lock],
		    case length(LockErrors) =:= 0 andalso ObjectsResult =:= ok of
			true -> {<<"{\"status\": \"ok\"}">>, Req0, []};
			false ->
			    %% If for some reason some of pseudo-directories were not renamed,
			    %% return ``429 Too Many Requests``, so user tries again.
			    js_handler:too_many(Req0)
		    end;
		_ ->
		    %% Return lists of objects that were skipped
		    {DirErrors, ObjectErrors} = lists:unzip([proplists:get_value(accepted, I) || I <- AcceptedResults]),
		    Req1 = cowboy_req:reply(202, #{
			<<"content-type">> => <<"application/json">>
		    }, jsx:encode([{dir_errors, DirErrors}, {object_errors, ObjectErrors}]), Req0),
		    {true, Req1, []}
	    end;
	_ ->
	    %% Return the first error from the list
	    {error, Number} = lists:nth(1, ErrorResults),
	    js_handler:bad_request(Req0, Number)
    end.

delete_resource(Req0, State) ->
    case validate_delete(Req0, State) of
	{error, Number} -> js_handler:bad_request(Req0, Number);
	{Req1, BucketId, Prefix, PseudoDirectories, ObjectKeys1} ->
	    Timestamp = utils:timestamp(),
	    User = proplists:get_value(user, State),
	    ActionLogRecord0 = #riak_action_log_record{
		action="delete",
		user_name=User#user.name,
		tenant_name=User#user.tenant_name,
		timestamp=io_lib:format("~p", [Timestamp])
	    },
	    %% Set "uncommitted" flag only if ther's a lot of delete
	    case length(PseudoDirectories) > 0 of
		true ->
		    case indexing:update(BucketId, Prefix, [{uncommitted, true}]) of
			lock -> js_handler:too_many(Req1);
			_ ->
			    PseudoDirectoryResults = [delete_pseudo_directory(
				BucketId, Prefix, P, ActionLogRecord0, Timestamp) || P <- PseudoDirectories],
			    ObjectsResult = delete_objects(BucketId, Prefix, ObjectKeys1,
							   ActionLogRecord0, Timestamp),
			    format_delete_results(Req1, PseudoDirectoryResults, ObjectsResult)
		    end;
		false ->
		    case delete_objects(BucketId, Prefix, ObjectKeys1,
					ActionLogRecord0, Timestamp) of
			ok -> {<<"{\"status\": \"ok\"}">>, Req1, []};
			lock -> js_handler:too_many(Req1)
		    end
	    end
    end.

delete_completed(Req0, State) ->
    Req1 = cowboy_req:set_resp_body("{\"status\": \"ok\"}", Req0),
    {true, Req1, State}.
