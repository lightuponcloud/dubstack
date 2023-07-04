%%
%% This server performs copy and move operations in background.
%%
-module(copy_server).

-behaviour(gen_server).

%% API
-export([start_link/0, copy/6, move/6]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("log.hrl").
-include("riak.hrl").
-include("entities.hrl").
-include("action_log.hrl").

-record(state, {}).


-spec(copy(SrcBucketId :: string(), DstBucketId :: string(), SrcPrefix :: string(),
	   DstPrefix :: string(), SrcObjectKeys :: list(), User :: user()) -> ok).

copy(SrcBucketId, DstBucketId, SrcPrefix, DstPrefix, SrcObjectKeys, User)
	when erlang:is_list(SrcBucketId) andalso erlang:is_list(DstBucketId) andalso
	    erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined andalso
	    erlang:is_list(DstPrefix) orelse DstPrefix =:= undefined andalso
	    erlang:is_list(SrcObjectKeys) ->
    gen_server:cast(?MODULE, {copy, [SrcBucketId, DstBucketId, SrcPrefix, DstPrefix, SrcObjectKeys, User]}).

-spec(move(SrcBucketId :: string(), DstBucketId :: string(), SrcPrefix :: string(),
	   DstPrefix :: string(), SrcObjectKeys :: list(), User :: user()) -> ok).

move(SrcBucketId, DstBucketId, SrcPrefix, DstPrefix, SrcObjectKeys, User)
	when erlang:is_list(SrcBucketId) andalso erlang:is_list(DstBucketId) andalso
	    erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined andalso
	    erlang:is_list(DstPrefix) orelse DstPrefix =:= undefined andalso
	    erlang:is_list(SrcObjectKeys) ->
    gen_server:cast(?MODULE, {move, [SrcBucketId, DstBucketId, SrcPrefix, DstPrefix, SrcObjectKeys, User]}).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages. This message is received by gen_server:cast() call
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({copy, [SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0, SrcObjectKeys, User]}, State) ->
    DstIndexContent = indexing:get_index(DstBucketId, DstPrefix0),
    Copied0 = lists:map(
	fun(RequestedKey) ->
	    ObjectKey = element(1, RequestedKey),
	    NewName = element(2, RequestedKey),
	    Copied1 = copy_objects(SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0,
				   ObjectKey, NewName, DstIndexContent, User),
	    case utils:ends_with(ObjectKey, <<"/">>) of
		true -> {utils:unhex(ObjectKey), NewName, Copied1};
		false -> {undefined, undefined, lists:nth(1, Copied1)}
	    end
	end, SrcObjectKeys),
    %%
    %% Add action log record
    %%
    {CopiedDirectories, CopiedObjects} = prepare_action_log(Copied0),
    ActionLogRecord0 = #riak_action_log_record{
	action="copy",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [erlang:round(utils:timestamp()/1000)])
    },
    SrcPrefix1 =
	case SrcPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(SrcPrefix0)) ++ ["/"]
	end,
    Summary0 = lists:flatten([["Copied"], CopiedDirectories ++ CopiedObjects,
			     ["\" from \"", SrcPrefix1, "\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(DstBucketId, DstPrefix0, ActionLogRecord1),
    DstPrefix1 =
	case DstPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(DstPrefix0))++["/"]
	end,
    %% Update destination pseudo-directory's action log
    %% only if source and destination paths are different
    case SrcPrefix1 =:= DstPrefix1 of
	true -> ok;
	false ->
	    Summary1 = lists:flatten([["Copied"], CopiedDirectories ++ CopiedObjects,
				      [" to \""], [DstPrefix1, "\"."]]),
	    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
	    action_log:add_record(SrcBucketId, SrcPrefix0, ActionLogRecord2)
    end,
    {noreply, State};

handle_cast({move, [SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0, SrcObjectKeys, User]}, State) ->
    DstIndexContent = indexing:get_index(DstBucketId, DstPrefix0),
    Copied0 = lists:map(
	fun(RequestedKey) ->
	    ObjectKey = element(1, RequestedKey),
	    NewName = element(2, RequestedKey),
	    Copied1 = copy_objects(SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0,
				   ObjectKey, NewName, DstIndexContent, User),
	    case Copied1 of
		{error, _} -> {undefined, undefined, []};
		_ ->
		    case utils:ends_with(ObjectKey, <<"/">>) of
			true ->
			    %% Values are the following:
			    %% {previous pseudo-directory name, new name of directory user provided, list}
			    {utils:unhex(ObjectKey), NewName, Copied1};
			false -> {undefined, undefined, lists:nth(1, Copied1)}
		    end
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
			    SrcPrefix1 =
				case proplists:get_value(src_prefix, CopiedOne) of
				    undefined -> undefined;
				    P -> erlang:binary_to_list(P)
				end,
			    OldKey0 = proplists:get_value(old_key, CopiedOne),
			    PrefixedObjectKey = utils:prefixed_object_key(SrcPrefix1, erlang:binary_to_list(OldKey0)),
			    %% Delete source object only if not locked by another user
			    case proplists:get_value(src_locked, CopiedOne) of
				false ->
				    case riak_api:delete_object(SrcBucketId, PrefixedObjectKey) of
					{error, Reason} ->
					    lager:error("[move_handler] Can't delete ~p/~p: ~p",
							[SrcBucketId, PrefixedObjectKey, Reason]);
					{ok, _} -> sqlite_server:delete_object(SrcBucketId, SrcPrefix1, unicode:characters_to_list(OldKey0))
				    end;
				true ->
				    LockUserId = proplists:get_value(src_lock_user_id, CopiedOne),
				    case LockUserId =:= User#user.id of
					false -> ok;  %% don't delete source object
					true ->
					    case riak_api:delete_object(SrcBucketId, PrefixedObjectKey) of
						{error, Reason} ->
						    lager:error("[move_handler] Can't delete ~p/~p: ~p",
								[SrcBucketId, PrefixedObjectKey, Reason]);
						{ok, _} -> sqlite_server:delete_object(SrcBucketId, SrcPrefix1, unicode:characters_to_list(OldKey0))
					    end,
					    riak_api:delete_object(SrcBucketId, PrefixedObjectKey ++ ?RIAK_LOCK_SUFFIX)
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
			    delete_pseudo_directory(SrcBucketId, IndexPrefix, [], User#user.id);
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
	lock ->
	    lager:warning("[list_handler] Can't update index during moving object, as lock exist: ~p/~p",
		       [SrcBucketId, SrcPrefix0]);
	_ ->
	    %%
	    %% Add action log record
	    %%
	    {CopiedDirectories, CopiedObjects} = prepare_action_log(Copied0),
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
	    Summary0 = lists:flatten([["Moved"], CopiedDirectories ++ CopiedObjects,
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
		    Summary1 = lists:flatten([["Moved"], CopiedDirectories ++ CopiedObjects,
					      [" to \""], [DstPrefix1, "\"."]]),
		    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
		    action_log:add_record(SrcBucketId, SrcPrefix0, ActionLogRecord2)
	    end
    end,
    {noreply, State};

handle_cast(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages. Called by send_after() call.
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) -> ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% Copies object.
%%
%% The following function do not perform copy in the following cases.
%%	- Object is marked as deleted
%%	- Riak CS returned error
%%
-spec do_copy(string(), string(), string(), string(), binary(), list(), any()) -> list().

do_copy(SrcBucketId, DstBucketId, PrefixedObjectKey0, DstPrefix0, NewName0, DstIndexContent, User) ->
    SrcPrefix =
	case utils:dirname(PrefixedObjectKey0) of
	    undefined -> undefined;
	    P -> unicode:characters_to_binary(P)
	end,
    OldKey = filename:basename(PrefixedObjectKey0),
    case riak_api:head_object(SrcBucketId, PrefixedObjectKey0) of
	{error, Reason} ->
	    lager:error("[copy_handler] head_object failed ~p/~p: ~p",
			[SrcBucketId, PrefixedObjectKey0, Reason]),
	    [{src_prefix, SrcPrefix},
	     {dst_prefix, DstPrefix0},
	     {old_key, erlang:list_to_binary(OldKey)},
	     {skipped, non_existing}];
	not_found ->
	    [{src_prefix, SrcPrefix},
	     {dst_prefix, DstPrefix0},
	     {old_key, erlang:list_to_binary(OldKey)},
	     {skipped, non_existing}];
	Metadata0 ->
	    case proplists:get_value("x-amz-meta-is-deleted", Metadata0) of
		"true" ->
		    [{src_prefix, SrcPrefix},
		     {dst_prefix, DstPrefix0},
		     {old_key, erlang:list_to_binary(OldKey)},
		     {skipped, deleted}];
		_ -> do_copy(SrcBucketId, DstBucketId, PrefixedObjectKey0, DstPrefix0,
			     NewName0, DstIndexContent, User, Metadata0)
	    end
    end.

do_copy(SrcBucketId, DstBucketId, PrefixedObjectKey0, DstPrefix0, NewName0, DstIndexContent, User, Metadata0) ->
    OldGUID =
	case proplists:get_value("x-amz-meta-copy-from-guid", Metadata0) of
	    undefined -> proplists:get_value("x-amz-meta-guid", Metadata0);
	    CopyFromGUID -> CopyFromGUID
	end,
    OldBucketId =
	case proplists:get_value("x-amz-meta-copy-from-bucket-id", Metadata0) of
	    undefined -> SrcBucketId;
	    CopyFromBucket -> CopyFromBucket
	end,
    NewGUID = erlang:binary_to_list(riak_crypto:uuid4()),
    ObjectMeta0 = list_handler:parse_object_record(Metadata0, [{guid, NewGUID},
							       {copy_from_guid, OldGUID},
							       {copy_from_bucket_id, OldBucketId},
							       {is_locked, "false"},
							       {lock_user_id, undefined},
							       {lock_user_name, undefined},
							       {lock_user_tel, undefined},
							       {lock_modified_utc, undefined}]),
    UploadId = proplists:get_value("upload-id", ObjectMeta0),
    OrigName0 = utils:unhex(erlang:list_to_binary(proplists:get_value("orig-filename", ObjectMeta0))),
    OrigName1 =
	case NewName0 of
	    undefined -> OrigName0; %% Take destination name from copied object
	    _ -> NewName0 %% provided destination name is a new name in the same prefix
	end,
    %% Determine destination object name, as directory with the same name might exist
    UserName = utils:unhex(erlang:list_to_binary(User#user.name)),
    {ObjectKey0, OrigName2, _IsNewVersion, ExistingObject0, _IsConflict} = riak_api:pick_object_key(
	    DstBucketId, DstPrefix0, OrigName1, undefined, UserName, DstIndexContent),
    %% Do not copy over locked file.
    %% If file is overwritten by copy, its previous version still can be restored.
    {IsLocked, LockUserId} =
	case ExistingObject0 of
	    undefined -> {false, undefined};
	    _ -> {ExistingObject0#object.is_locked,
		  ExistingObject0#object.lock_user_id}
	end,
    ReplaceAllowed =
	case IsLocked of
	    true ->
		case LockUserId =/= undefined andalso User#user.id =/= LockUserId of
		    true -> false;
		    false -> true
		end;
	    false -> true
	end,
    OldKey = filename:basename(PrefixedObjectKey0),
    DstPrefix1 =
	case DstPrefix0 of
	    undefined -> undefined;
	    _ -> unicode:characters_to_binary(DstPrefix0)
	end,
    SrcPrefix =
	case utils:dirname(PrefixedObjectKey0) of
	    undefined -> undefined;
	    P -> unicode:characters_to_binary(P)
	end,
    case ReplaceAllowed of
	false -> [{src_prefix, SrcPrefix},
		  {dst_prefix, DstPrefix1},
		  {old_key, erlang:list_to_binary(OldKey)},
		  {skipped, locked}];
	true ->
	    %% Put pointer to the real object
	    ObjectMeta1 = lists:keyreplace("orig-filename", 1, ObjectMeta0,
					   {"orig-filename", utils:hex(OrigName2)}),
	    %% Copy object metadata to destination object
	    case riak_api:put_object(DstBucketId, DstPrefix0, ObjectKey0, <<>>,
				     [{acl, public_read}, {meta, ObjectMeta1}]) of
		ok ->
		    %% Put "stop" file on copied real object, to prevent its removal, when
		    %% a new version uploaded ( we have just created a link to that object ).
		    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, OldGUID),
		    StopSuffix = ?STOP_OBJECT_SUFFIX,
		    case riak_api:put_object(OldBucketId, RealPrefix, UploadId++StopSuffix, <<>>,
					     [{acl, public_read}]) of
			ok ->
			    IsRenamed = OrigName2 =/= OrigName0,
			    Bytes =
				case proplists:get_value("bytes", ObjectMeta0) of
				    undefined -> undefined;
				    B -> erlang:list_to_integer(B)
				end,
			    SrcIsLocked =
				case utils:to_list(proplists:get_value("x-amz-meta-is-locked", Metadata0)) of
				    "true" -> true;
				    _ -> false
				end,
			    Version = proplists:get_value("version", ObjectMeta0),
			    UploadTime = utils:to_binary(proplists:get_value("upload-time", ObjectMeta0)),
			    SrcLockUserId =
				case proplists:get_value("x-amz-meta-lock-user-id", Metadata0) of
				    undefined -> undefined;
				    U -> erlang:list_to_binary(U)
				end,
			    %% Update SQLite db
			    Obj = #object{
				key = erlang:list_to_binary(ObjectKey0),
				orig_name = unicode:characters_to_list(OrigName2),
				bytes = Bytes,
				guid = erlang:list_to_binary(NewGUID),
				version = erlang:list_to_binary(Version),
				upload_time = UploadTime,
				is_deleted = false,
				author_id = User#user.id,
				author_name = User#user.name,
				author_tel = User#user.tel,
				is_locked = false
			    },
			    sqlite_server:add_object(DstBucketId, DstPrefix0, Obj),

			    [{src_prefix, SrcPrefix},
			     {dst_prefix, DstPrefix1},
			     {old_key, erlang:list_to_binary(OldKey)},
			     {new_key, erlang:list_to_binary(ObjectKey0)},
			     {src_orig_name, OrigName0},
			     {dst_orig_name, OrigName2},
			     {bytes, Bytes},
			     {upload_time, UploadTime},
			     {renamed, IsRenamed},
			     {src_locked, SrcIsLocked},  %% this flag is used in move operation
			     {src_lock_user_id, SrcLockUserId},
			     {guid, erlang:list_to_binary(NewGUID)},
			     {version, erlang:list_to_binary(Version)}];
			_ ->
			    [{src_prefix, SrcPrefix},
			     {dst_prefix, DstPrefix0},
			     {old_key, erlang:list_to_binary(OldKey)},
			     {skipped, server_error}]
		    end;
		{error, Reason} ->
		    lager:error("[copy_handler] Can't put object ~p/~p/~p: ~p",
				[DstBucketId, DstPrefix0, ObjectKey0, Reason]),
		    [{src_prefix, SrcPrefix},
		     {dst_prefix, DstPrefix0},
		     {old_key, erlang:list_to_binary(OldKey)},
		     {skipped, server_error}]
	    end
    end.

%%
%% Removes source prefix to form destination prefix later.
%%
shorten_prefix(SrcPrefix0, CurrentPath) ->
    CurrentSrcPrefix0 = utils:dirname(CurrentPath),
    %% Remove source prefix from path, in order to attach it to the destination path
    case CurrentSrcPrefix0 of
	undefined -> "";
	_ ->
	    case SrcPrefix0 of
		undefined -> CurrentSrcPrefix0;
		_ -> re:replace(CurrentSrcPrefix0, "^"++SrcPrefix0, "", [{return, list}])
	    end
    end.

%%
%% Changes the directory name to NewName, if copied one exists in destination path.
%%
to_dst_prefix(DstBucketId, SrcPrefix, DstPrefix, CurrentPath, NewName, DstIndexContent, UserName) ->
    ShortPrefix0 = shorten_prefix(SrcPrefix, CurrentPath),
    %% Check if current path =:= dir name user requested
    ShortPrefix1 =
	case string:tokens(ShortPrefix0, "/") of
	    [] -> ShortPrefix0;
	    [_FirstToken | RestTokens] ->
		%% Check if new requested name exists.
		%% Mark pseudo-directory as conflicted otherwise.
		case indexing:directory_or_object_exists(DstBucketId, DstPrefix, NewName, DstIndexContent) of
		    false -> ShortPrefix0;
		    {directory, ExistingDirName} ->
			ConflictedName0 = utils:unhex(ExistingDirName),
			ConflictedName1 = riak_api:mark_filename_conflict(ConflictedName0, UserName),
			utils:join_list_with_separator([utils:hex(ConflictedName1)] ++ RestTokens, "/", []);
		    {object, OrigName} ->
			ConflictedName1 = riak_api:mark_filename_conflict(OrigName, UserName),
			utils:join_list_with_separator([utils:hex(ConflictedName1)] ++ RestTokens, "/", [])
		end
	end,
    utils:prefixed_object_key(DstPrefix, ShortPrefix1).

%%
%% Copy objects for provided pseudo-directory or object key.
%%
%% Returns list of tuples:
%% {source prefix/key, destination prefix/key, NewName}
%% where NewName is the name user provided in JSON request.
%%
copy_objects(SrcBucketId, DstBucketId, SrcPrefix, DstPrefix, ObjectKey0, NewName0,
	     DstIndexContent, User) when erlang:is_binary(ObjectKey0) ->
    ObjectKey1 = erlang:binary_to_list(ObjectKey0),
    %% Change destination prefixes. If directory name exists, rename it
    NewPaths =
	case utils:ends_with(ObjectKey0, <<"/">>) of
	    true ->
		%% We expect "/" at the end of prefixes
		%% to save time on processing and make the code less complex
		ON = string:to_lower(ObjectKey1),  %% lowercase hex prefix
		Paths = riak_api:recursively_list_pseudo_dir(SrcBucketId, utils:prefixed_object_key(SrcPrefix, ON)),
		UserName = utils:unhex(erlang:list_to_binary(User#user.name)),
		lists:map(
		    fun(CurrentPath) ->
			CurrentDstPrefix = to_dst_prefix(DstBucketId, SrcPrefix, DstPrefix, CurrentPath,
							 NewName0, DstIndexContent, UserName),
			{CurrentPath, utils:prefixed_object_key(CurrentDstPrefix, filename:basename(CurrentPath)), undefined}
		    end, Paths);
	    false ->
		PrefixedObjectKey = utils:prefixed_object_key(SrcPrefix, ObjectKey1),
		ShortPrefix2 = shorten_prefix(SrcPrefix, PrefixedObjectKey),
		%% No need to check if target object exist, as new target object name will be picked later
		DstNewPrefix = utils:prefixed_object_key(DstPrefix, ShortPrefix2),
		SrcIndexPath = utils:prefixed_object_key(SrcPrefix, ?RIAK_INDEX_FILENAME),
		DstIndexPath = utils:prefixed_object_key(DstPrefix, ?RIAK_INDEX_FILENAME),
		[{PrefixedObjectKey, utils:prefixed_object_key(DstNewPrefix, ObjectKey1), NewName0},
		 {SrcIndexPath, DstIndexPath}]
	end,
    SrcIndexPaths = [I || I <- NewPaths, lists:suffix(?RIAK_INDEX_FILENAME, element(1, I))],
    %% Copy objects directory by directory
    CopiedObjects0 = lists:map(
	fun(SrcDstPath) ->
	    CurrentPrefix = utils:dirname(element(1, SrcDstPath)),
	    NewDst = element(2, SrcDstPath),
	    NewDstPrefix = utils:dirname(NewDst),
	    NewDstIndexContent = indexing:get_index(DstBucketId, NewDstPrefix),
	    %% Copy action log, if it absent in destination directory
	    %% Reason: someone could destroy log of actions by copying/moving directories otherwise
	    SrcActionLog = utils:prefixed_object_key(CurrentPrefix, ?RIAK_ACTION_LOG_FILENAME),
	    DstActionLog = utils:prefixed_object_key(NewDstPrefix, ?RIAK_ACTION_LOG_FILENAME),
	    case riak_api:head_object(DstBucketId, DstActionLog) of
		{error, Reason} ->
		    lager:error("[copy_handler] head_object failed ~p/~p: ~p",
				[DstBucketId, DstActionLog, Reason]),
		    riak_api:copy_object(DstBucketId, DstActionLog, SrcBucketId,
						  SrcActionLog, [{acl, public_read}]);
		not_found -> riak_api:copy_object(DstBucketId, DstActionLog, SrcBucketId,
						  SrcActionLog, [{acl, public_read}]);
		_ -> ok
	    end,
	    Copied0 = [do_copy(SrcBucketId, DstBucketId, element(1, I), NewDstPrefix,
			       element(3, I), NewDstIndexContent, User) || I <- NewPaths,
			utils:is_hidden_object([{key, element(1, I)}]) =/= true andalso
			utils:dirname(element(1, I)) =:= CurrentPrefix],
	    %% Update indices for pseudo-sub-directories, as some objects were renamed.
	    %% Rename map should be updated in index file.
	    indexing:update(DstBucketId, NewDstPrefix, [{copy_from, [
			    {bucket_id, SrcBucketId},
			    {prefix, CurrentPrefix},
			    {copied_names, [I || I <- Copied0, proplists:is_defined(skipped, I) =:= false]}]}]),

	    %% Create pseudo-directory in SQLite
	    case NewDstPrefix of
		undefined -> ok;
		_ ->
		    NewDirectoryName = utils:unhex(erlang:list_to_binary(filename:basename(NewDstPrefix))),
		    sqlite_server:create_pseudo_directory(DstBucketId, utils:dirname(NewDstPrefix),
							  NewDirectoryName, User)
	    end,
	    %% Update parent directory
	    indexing:update(DstBucketId, utils:dirname(NewDstPrefix)),
	    Copied0
	end, SrcIndexPaths),
    %% Make one flat list from all requested objects and pseudo-directories
    lists:foldl(fun(X, Acc) -> X ++ Acc end, [], CopiedObjects0).

%%
%% Prepares action log record.
%%
prepare_action_log(CopiedStuff) ->
    CopiedDirectories = lists:map(
	fun(I) ->
	    case element(1, I) of
		undefined -> []; %% object, not directory
		PrevObjName ->
		    NewObjName = element(2, I),
		    case PrevObjName =:= NewObjName of
			true -> [[" \""], unicode:characters_to_list(PrevObjName), ["/\""]];
			false -> [[" \""], unicode:characters_to_list(PrevObjName),
				  [" as ", "/\"", unicode:characters_to_list(NewObjName), "/\""]]
		    end
	    end
	end, CopiedStuff),
    CopiedObjects = lists:map(
	fun(I) ->
	    case element(1, I) of
		undefined ->
		    CopiedOne = element(3, I),
		    case proplists:is_defined(skipped, CopiedOne) of
			true -> [];
			false ->
			    SrcOrigName = proplists:get_value(src_orig_name, CopiedOne),
			    DstOrigName = proplists:get_value(dst_orig_name, CopiedOne),
			    case SrcOrigName =:= DstOrigName of
				true -> [[" \""], unicode:characters_to_list(DstOrigName), "\""];
				false -> [[" \""], unicode:characters_to_list(SrcOrigName),
					  ["\""], [" as \""], unicode:characters_to_list(DstOrigName), ["\""]]
			    end
		    end;
		_ -> []
	    end
	end, CopiedStuff),
    {CopiedDirectories, CopiedObjects}.


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
			    case LockUserId =/= UserId of
				false -> ok; %% don't delete source object, as someone's editing it
				true ->
				    case riak_api:delete_object(BucketId, PrefixedObjectKey1) of
					{error, Reason} ->
					    lager:error("[move_handler] Can't delete object ~p/~p: ~p",
							[BucketId, PrefixedObjectKey1, Reason]);
					{ok, _} -> sql_server:delete_object(BucketId, SrcPrefix, OldKey)
				    end,
				    riak_api:delete_object(BucketId, PrefixedObjectKey1 ++ ?RIAK_LOCK_SUFFIX)
			    end;
			_ ->
			    case riak_api:delete_object(BucketId, PrefixedObjectKey1) of
				{error, Reason} ->
				    lager:error("[move_handler] Can't delete object ~p/~p: ~p",
						[BucketId, PrefixedObjectKey1, Reason]);
				{ok, _} -> sqlite_server:delete_object(BucketId, SrcPrefix, OldKey)
			    end
		    end,
		    false
	    end
	end, CopiedObjects),
    case HasSkipped of
	[] ->
	    PrefixedIndexKey = utils:prefixed_object_key(Prefix, ?RIAK_INDEX_FILENAME),
	    case riak_api:delete_object(BucketId, PrefixedIndexKey) of
		{error, Reason} ->
		    lager:error("[move_handler] Can't delete object ~p/~p: ~p",
				[BucketId, PrefixedIndexKey, Reason]);
		{ok, _} -> ok
	    end,
	    PrefixedActionLogKey = utils:prefixed_object_key(Prefix, ?RIAK_ACTION_LOG_FILENAME),
	    riak_api:delete_object(BucketId, PrefixedActionLogKey),
	    %% Mark deleted in SQLite db
	    DirName = utils:unhex(erlang:list_to_binary(filename:basename(Prefix))),
	    sqlite_server:delete_pseudo_directory(BucketId, utils:dirname(Prefix),
						  DirName, UserId);
	_ -> ok %% Do not delete index yet
    end.
