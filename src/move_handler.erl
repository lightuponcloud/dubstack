%%
%% Allows to move objects and pseudo-directories.
%%
-module(move_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, is_authorized/2, forbidden/2,
	 handle_post/2]).

-include("riak.hrl").
-include("entities.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Returns callback 'handle_post()'
%% ( called after 'resource_exists()' )
%%
content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State}.

%%
%% Returns callback 'to_json()'
%% ( called after 'forbidden()' )
%%
content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

%%
%% Validates POST request and sends request to Riak CS to copy objects and delete them
%%
handle_post(Req0, State0) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    case copy_handler:validate_copy_parameters(State0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		State1 -> move(Req0, State1)
	    end;
	_ -> js_handler:bad_request(Req0, 16)
    end.

delete_pseudo_directory(BucketId, Prefix, CopiedObjects, UserId) ->
    HasSkipped = lists:filtermap(
	fun(I) ->
	    case proplists:is_defined(skipped, I) of
		true -> {true, true};
		false ->
		    SrcPrefix = proplists:get_value(src_prefix, I),
		    OldKey = proplists:get_value(old_key, I),
		    PrefixedObjectKey0 = utils:prefixed_object_key(SrcPrefix, OldKey),
		    PrefixedObjectKey1 = erlang:binary_to_list(PrefixedObjectKey0),
		    case proplists:get_value(locked, I) of
			true ->
			    LockUserId = proplists:get_value(lock_user_id, I),
			    case LockUserId =:= UserId of
				false -> ok; %% don't delete source object
				true ->
				    riak_api:delete_object(BucketId, PrefixedObjectKey1),
				    riak_api:delete_object(BucketId, PrefixedObjectKey1 ++ ?RIAK_LOCK_SUFFIX)
			    end;
			_ -> riak_api:delete_object(BucketId, PrefixedObjectKey1)
		    end,
		    false
	    end
	end, CopiedObjects),
    case HasSkipped of
	[] ->
	    PrefixedIndexKey = utils:prefixed_object_key(erlang:binary_to_list(Prefix), ?RIAK_INDEX_FILENAME),
	    riak_api:delete_object(BucketId, PrefixedIndexKey),
	    PrefixedActionLogKey = utils:prefixed_object_key(erlang:binary_to_list(Prefix), ?RIAK_ACTION_LOG_FILENAME),
	    riak_api:delete_object(BucketId, PrefixedActionLogKey);
	_ -> ok %% Do not delete index yet
    end.

move(Req0, State) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State),
    SrcPrefix0 = proplists:get_value(src_prefix, State),
    SrcObjectKeys = proplists:get_value(src_object_keys, State),
    DstBucketId = proplists:get_value(dst_bucket_id, State),
    DstPrefix0 = proplists:get_value(dst_prefix, State),
    User = proplists:get_value(user, State),
    DstIndexContent = indexing:get_index(DstBucketId, DstPrefix0),
    Copied0 = lists:map(
	fun(RequestedKey) ->
	    ObjectKey = element(1, RequestedKey),
	    NewName = element(2, RequestedKey),
	    Copied1 = copy_handler:copy_objects(SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0,
						ObjectKey, NewName, DstIndexContent, User),
	    case utils:ends_with(ObjectKey, <<"/">>) of
		true ->
		    %% Values are the following:
		    %% {previous pseudo-directory name, new name of directory user provided, list}
		    {utils:unhex(ObjectKey), NewName, Copied1};
		false -> {undefined, undefined, lists:nth(1, Copied1)}
	    end
	end, SrcObjectKeys),
    %% Iterate over requested objects and delete those that were copied
    lists:map(
	fun(I) ->
	    case element(1, I) of
		undefined ->
		    %% Object copied, delete only if copy confirmed
		    CopiedOne = element(3, I),
		    case proplists:is_defined(skipped, CopiedOne) of
			true -> ok; %% don't delete
			false ->
			    SrcPrefix1 = proplists:get_value(src_prefix, CopiedOne),
			    OldKey0 = proplists:get_value(old_key, CopiedOne),
			    PrefixedObjectKey0 = utils:prefixed_object_key(SrcPrefix1, OldKey0),
			    PrefixedObjectKey1 = erlang:binary_to_list(PrefixedObjectKey0),
			    %% Delete source object only if not locked by another user
			    case proplists:get_value(src_locked, CopiedOne) of
				false -> riak_api:delete_object(SrcBucketId, PrefixedObjectKey1);
				true ->
				    LockUserId = proplists:get_value(src_lock_user_id, CopiedOne),
				    case LockUserId =:= User#user.id of
					false -> ok; %% don't delete source object
					true ->
					    riak_api:delete_object(SrcBucketId, PrefixedObjectKey1),
					    riak_api:delete_object(SrcBucketId, PrefixedObjectKey1 ++ ?RIAK_LOCK_SUFFIX)
				    end
			    end
		    end;
		_ ->
		    %% Pseudo-directory copied, delete nested objects only if copy confirmed
		    Copied2 = element(3, I),
		    case Copied2 of
			[] ->
			    %% Empty directory was moved
			    IndexPrefix = utils:prefixed_object_key(SrcPrefix0, utils:hex(element(1, I))),
			    delete_pseudo_directory(SrcBucketId, erlang:list_to_binary(IndexPrefix), [], User#user.id);
			_ ->
			    UniqPrefixList = lists:usort([proplists:get_value(src_prefix, J) || J <- Copied2]),
			    %% Find prefixes which have all objects copied, and delete them
			    lists:map(
				fun(CurrentUniqSrcPrefix) ->
				    delete_pseudo_directory(SrcBucketId, CurrentUniqSrcPrefix,
					[K ||K <- Copied2, proplists:get_value(src_prefix, K) =:= CurrentUniqSrcPrefix],
					User#user.id)
				end, UniqPrefixList)
		    end
	    end
	end, Copied0),
    case indexing:update(SrcBucketId, SrcPrefix0) of
	lock -> js_handler:too_many(Req0);
	_ ->
	    %%
	    %% Add action log record
	    %%
	    {CopiedDirectories, CopiedObjects} = copy_handler:prepare_action_log(Copied0),
	    ActionLogRecord0 = #riak_action_log_record{
		action="move",
		user_name=User#user.name,
		tenant_name=User#user.tenant_name,
		timestamp=io_lib:format("~p", [erlang:round(utils:timestamp()/1000)])
	    },
	    SrcPrefix2 =
		case SrcPrefix0 of
		    undefined -> "/";
		    _ -> unicode:characters_to_list(utils:unhex_path(SrcPrefix0)) ++ ["/"]
		end,
	    Summary0 = lists:flatten([["Copied"], CopiedDirectories ++ CopiedObjects,
				      ["\" from \"", SrcPrefix2, "\"."]]),
	    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
	    action_log:add_record(DstBucketId, DstPrefix0, ActionLogRecord1),
	    DstPrefix1 =
		case DstPrefix0 of
		    undefined -> "/";
		    _ -> unicode:characters_to_list(utils:unhex_path(DstPrefix0))++["/"]
		end,
	    %% Update destination pseudo-directory's action log
	    %% only if source and destination paths are different
	    case SrcPrefix2 =:= DstPrefix1 of
		true -> ok;
		false ->
		    Summary1 = lists:flatten([["Copied"], CopiedDirectories ++ CopiedObjects,
					      [" to \""], [DstPrefix1, "\"."]]),
		    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
		    action_log:add_record(SrcBucketId, SrcPrefix0, ActionLogRecord2)
	    end,
	    Result = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], [element(3, I) || I <- Copied0]),
	    Req1 = cowboy_req:reply(200, #{
		<<"content-type">> => <<"application/json">>
	    }, jsx:encode(Result), Req0),
	    {stop, Req1, []}
    end.

%%
%% Serializes response to json
%%
to_json(Req0, State) ->
    {<<"{\"status\": \"ok\"}">>, Req0, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, State) ->
    list_handler:is_authorized(Req0, State).

%%
%% Checks if user has access
%% - To source bucket
%% - To destination bucket
%%
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    copy_handler:copy_forbidden(Req0, State).
