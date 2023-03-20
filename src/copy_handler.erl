%%
%% Allows to copy objects and pseudo-directories.
%%
-module(copy_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, is_authorized/2, forbidden/2,
	 handle_post/2, copy_objects/8, copy_forbidden/2,
	 validate_copy_parameters/1, prepare_action_log/1]).

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
%% Checks if source object exists.
%%
-spec validate_src_object_key(list(), list(), list()) -> tuple()|{error, integer()}.

validate_src_object_key(SrcPrefix, DstPrefix, ObjectKey0)
	when erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined,
	     erlang:is_list(DstPrefix) orelse DstPrefix =:= undefined, erlang:is_tuple(ObjectKey0) ->
    case size(ObjectKey0) of
	2 ->
	    ObjectKey1 = element(1, ObjectKey0),
	    DstName = element(2, ObjectKey0),
	    case utils:ends_with(ObjectKey1, <<"/">>) of
		true ->
		    %% Check if valid hex prefix provided
		    try utils:unhex(ObjectKey1) of
			_Value -> validate_dst_name(DstPrefix, ObjectKey1, DstName)
		    catch error:badarg ->
			{error, 14}
		    end;
		false -> validate_dst_name(DstPrefix, ObjectKey1, DstName)
	    end;
	_ -> {error, 15}
    end;
validate_src_object_key(_SrcPrefix, _DstPrefix, _ObjectKey) -> {error, 15}.

validate_dst_name(_DstPrefix, null, _DstName) -> {error, 30};
validate_dst_name(_DstPrefix, <<>>, _DstName) -> {error, 30};
validate_dst_name(_DstPrefix, ObjectKey0, <<>>) -> {ObjectKey0, undefined};
validate_dst_name(_DstPrefix, ObjectKey0, null) -> {ObjectKey0, undefined};
validate_dst_name(DstPrefix, ObjectKey0, DstName0) ->
    case utils:trim_spaces(ObjectKey0) of
	<<>> -> {error, 30};
	_ ->
	    %% In case destination name ends with "/", try to 'unhex'.
	    %% if not hex, remove trailing "/"
	    DstName1 =
		case utils:ends_with(DstName0, <<"/">>) of
		    false -> DstName0;
		    true ->
			try utils:unhex(DstName0) of
			    Value -> Value
			catch error:badarg ->
			    Size0 = byte_size(DstName0)-1,
			    <<DstName2:Size0/binary, _/binary>> = DstName0,
			    DstName2
			end
		end,
	    case utils:is_valid_object_key(DstName1) of
		false -> {error, 12};
		true ->
		    %% Someone could have changed default real object prefix to hex value
		    case utils:starts_with(DstName1, erlang:list_to_binary(?RIAK_REAL_OBJECT_PREFIX))
			    andalso DstPrefix =:= undefined of
			true -> {error, 10};
			false -> {ObjectKey0, DstName1}
		    end
	    end
    end.

%%
%% Checks if every key in src_object_keys exists and ther'a no duplicates
%%
validate_src_object_keys(_SrcPrefix, _DstPrefix, null) -> {error, 15};
validate_src_object_keys(_SrcPrefix, _DstPrefix, undefined) -> {error, 15};
validate_src_object_keys(_SrcPrefix, _DstPrefix, <<>>) -> {error, 15};
validate_src_object_keys(_SrcPrefix, _DstPrefix, []) -> {error, 15};
validate_src_object_keys(SrcPrefix, DstPrefix, SrcObjectMap0)
	when erlang:is_list(SrcObjectMap0), erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined,
	     erlang:is_list(DstPrefix) orelse DstPrefix =:= undefined ->
    SrcObjectMap1 = [validate_src_object_key(SrcPrefix, DstPrefix, I) || I <- SrcObjectMap0],
    Error = lists:keyfind(error, 1, SrcObjectMap1),
    case Error of
	{error, Number} -> {error, Number};
	_ ->
	    case utils:has_duplicates(SrcObjectMap1) of
		true -> {error, 31};
		false ->
		    %% Check if destination names list do not have duplicates,
		    %% as that would copy objects over each other
		    LowecaseList = [ux_string:to_lower(unicode:characters_to_list(element(2, I)))
				    || I <- SrcObjectMap1],
		    UniqSet = sets:to_list(sets:from_list(LowecaseList)),
		    case length(UniqSet) =:= length(SrcObjectMap1) of
			true -> SrcObjectMap1;  %% No duplicates
			false -> {error, 31}
		    end
	    end
    end.

-spec validate_copy_parameters(proplist()) -> any().

validate_copy_parameters(State0) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State0),
    DstBucketId = proplists:get_value(dst_bucket_id, State0),
    SrcPrefix0 = proplists:get_value(src_prefix, State0),
    DstPrefix0 = proplists:get_value(dst_prefix, State0),
    SrcObjectKeys0 = proplists:get_value(src_object_keys, State0),

    SrcPrefix1 = list_handler:validate_prefix(SrcBucketId, SrcPrefix0),
    DstPrefix1 = list_handler:validate_prefix(DstBucketId, DstPrefix0),

    SrcObjectKeys1 = validate_src_object_keys(SrcPrefix1, DstPrefix1, SrcObjectKeys0),
    Error = lists:keyfind(error, 1, [SrcPrefix1, DstPrefix1, SrcObjectKeys1]),
    case Error of
	{error, Number} -> {error, Number};
	_ ->
	    %% The destination directory might be subdirectory of the source directory.
	    SrcObjectKeys2 = lists:filter(
		fun(N) ->
		    SrcPrefix2 =
			case SrcPrefix1 of
			    undefined -> undefined;
			    _ -> erlang:list_to_binary(SrcPrefix1)
			end,
		    PN0 = utils:prefixed_object_key(SrcPrefix2, element(1, N)),
		    case utils:starts_with(DstPrefix1, PN0) of
			false -> true;
			true -> false
		    end
		end, SrcObjectKeys1),
	    case length(SrcObjectKeys2) =:= 0 orelse (DstPrefix1 =:= SrcPrefix1 andalso length(SrcObjectKeys0) > 1) of
		true -> {error, 13};
		false ->
		    User = proplists:get_value(user, State0),
		    [{src_bucket_id, SrcBucketId},
		     {dst_bucket_id, DstBucketId},
		     {src_prefix, SrcPrefix1},
		     {dst_prefix, DstPrefix1},
		     {src_object_keys, SrcObjectKeys2},
		     {user, User}]
	    end
    end.

%%
%% Validates POST request and sends request to Riak CS to copy object or pseudo-directory
%%
handle_post(Req0, State0) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    case validate_copy_parameters(State0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		State1 -> copy(Req0, State1)
	    end;
	_ -> js_handler:bad_request(Req0, 16)
    end.

validate_dst_prefix(undefined) -> null;
validate_dst_prefix(null) -> null;
validate_dst_prefix(<<>>) -> null;
validate_dst_prefix(Prefix) when erlang:is_list(Prefix) ->
    unicode:characters_to_binary(Prefix).

%%
%% Copies object.
%%
%% The following function do not perform copy in the following cases.
%%	- Object is marked as deleted
%%	- Riak CS returned error
%%
-spec do_copy(string(), string(), string(), string(), binary(), list(), any()) -> list().

do_copy(SrcBucketId, DstBucketId, PrefixedObjectKey0, DstPrefix0, NewName0, DstIndexContent, User) ->
    DstPrefix1 = validate_dst_prefix(DstPrefix0),
    SrcPrefix =
	case utils:dirname(PrefixedObjectKey0) of
	    undefined -> null;
	    P -> unicode:characters_to_binary(P)
	end,
    OldKey = filename:basename(PrefixedObjectKey0),
    case riak_api:head_object(SrcBucketId, PrefixedObjectKey0) of
	{error, Reason} ->
	    lager:error("[copy_handler] head_object failed ~p/~p: ~p",
			[SrcBucketId, PrefixedObjectKey0, Reason]),
	    [{src_prefix, SrcPrefix},
	     {dst_prefix, DstPrefix1},
	     {old_key, erlang:list_to_binary(OldKey)},
	     {skipped, non_existing}];
	not_found ->
	    [{src_prefix, SrcPrefix},
	     {dst_prefix, DstPrefix1},
	     {old_key, erlang:list_to_binary(OldKey)},
	     {skipped, non_existing}];
	Metadata0 ->
	    case proplists:get_value("x-amz-meta-is-deleted", Metadata0) of
		"true" ->
		    [{src_prefix, SrcPrefix},
		     {dst_prefix, DstPrefix1},
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
	    undefined -> null;
	    _ -> unicode:characters_to_binary(DstPrefix0)
	end,
    SrcPrefix =
	case utils:dirname(PrefixedObjectKey0) of
	    undefined -> null;
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
				    undefined -> null;
				    B -> erlang:list_to_integer(B)
				end,
			    SrcIsLocked =
				case utils:to_list(proplists:get_value("x-amz-meta-is-locked", Metadata0)) of
				    "true" -> true;
				    _ -> false
				end,
			    Version = proplists:get_value("version", ObjectMeta0),
			    UploadTime = proplists:get_value("upload-time", ObjectMeta0),
			    SrcLockUserId = proplists:get_value("x-amz-meta-lock-user-id", Metadata0),
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
			     {version, Version}];
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
	    %% Update indices for pseudo-sub-directories, as some of objects were renamed.
	    %% Rename map should be updated in index file.
	    indexing:update(DstBucketId, NewDstPrefix, [{copy_from, [
			    {bucket_id, SrcBucketId},
			    {prefix, CurrentPrefix},
			    {copied_names, [I || I <- Copied0, proplists:is_defined(skipped, I) =:= false]}]}]),
	    %% Update parent directory
	    indexing:update(DstBucketId, utils:dirname(NewDstPrefix)),

	    lists:map(
		fun(I) ->
		    case proplists:get_value(skipped, I) of
			locked -> ok;
			server_error -> ok;
			undefined ->
			    ObjectKey = proplists:get_value(new_key, I),
			    OrigName = proplists:get_value(dst_orig_name, I),
			    Bytes = proplists:get_value(bytes, I),
			    GUID = proplists:get_value(guid, I),
			    EncodedVersion = proplists:get_value(version, I),
			    UploadTime = proplists:get_value(upload_time, I),
			    %% Update SQLite db
			    Obj = #object{
				key = ObjectKey,
				orig_name = unicode:characters_to_list(OrigName),
				bytes = Bytes,
				guid = GUID,
				version = EncodedVersion,
				upload_time = UploadTime,
				is_deleted = false,
				author_id = User#user.id,
				author_name = User#user.name,
				author_tel = User#user.tel,
				is_locked = false
			    },
			    sqlite_server:add_object(DstBucketId, DstPrefix, Obj)
		    end
		end, Copied0),
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

copy(Req0, State) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State),
    SrcPrefix0 = proplists:get_value(src_prefix, State),
    SrcObjectKeys = proplists:get_value(src_object_keys, State),
    DstBucketId = proplists:get_value(dst_bucket_id, State),
    DstPrefix0 = proplists:get_value(dst_prefix, State),
    User = proplists:get_value(user, State),
    T0 = utils:timestamp(),

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
    Result = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], [element(3, I) || I <- Copied0]),
    T1 = utils:timestamp(),
    Req1 =
	case length(Result) of
	    0 -> 
		cowboy_req:reply(304, #{
		    <<"content-type">> => <<"application/json">>,
		    <<"elapsed-time">> => io_lib:format("~.2f", [utils:to_float(T1-T0)/1000])
		}, <<"[]">>, Req0);
	    _ ->
		cowboy_req:reply(200, #{
		    <<"content-type">> => <<"application/json">>,
		    <<"elapsed-time">> => io_lib:format("~.2f", [utils:to_float(T1-T0)/1000])
		}, jsx:encode(Result), Req0)
    end,
    {stop, Req1, []}.

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
is_authorized(Req0, _State) ->
    case utils:get_token(Req0) of
	undefined -> js_handler:unauthorized(Req0, 28);
	Token -> login_handler:get_user_or_error(Req0, Token)
    end.

copy_forbidden(Req0, User0) ->
    SrcBucketId =
	case cowboy_req:binding(src_bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    TenantId = User0#user.tenant_id,
    IsCopyAllowed =
	case utils:is_valid_bucket_id(SrcBucketId, TenantId) of
	    false -> false;
	    true ->
		UserBelongsToSrcGroup = lists:any(fun(Group) ->
			utils:is_bucket_belongs_to_group(SrcBucketId, TenantId, Group#group.id) end,
			User0#user.groups),
		case UserBelongsToSrcGroup of
		    true -> true;
		    false ->
			case utils:is_public_bucket_id(SrcBucketId) of
			    true -> User0#user.staff;
			    false -> false
			end
		end
	end,
    case IsCopyAllowed of
	true ->
	    {ok, Body, Req1} = cowboy_req:read_body(Req0),
	    case jsx:is_json(Body) of
		{error, badarg} -> js_handler:bad_request(Req1, 21);
		false -> js_handler:bad_request(Req1, 21);
		true ->
		    FieldValues = jsx:decode(Body),
		    DstBucketId0 =
			case proplists:get_value(<<"dst_bucket_id">>, FieldValues) of
			    undefined -> undefined;
			    DstBucketId1 -> unicode:characters_to_list(DstBucketId1)
			end,
		    TenantId = User0#user.tenant_id,
		    DstBucketCanBeModified =
			case utils:is_valid_bucket_id(DstBucketId0, TenantId) of
			    false -> false;
			    true ->
				UserBelongsToDstGroup = lists:any(fun(Group) ->
				    utils:is_bucket_belongs_to_group(DstBucketId0, TenantId, Group#group.id) end,
				    User0#user.groups),
				case UserBelongsToDstGroup of
				    true -> true;
				    false ->
					case utils:is_public_bucket_id(DstBucketId0) of
					    true -> User0#user.staff;  %% Only staff user can copy to public bucket
					    false -> false
					end
				end
			end,
		    case DstBucketCanBeModified of
			false -> js_handler:forbidden(Req1, 26, stop);
			true -> {false, Req1, [
				    {user, User0},
				    {src_bucket_id, SrcBucketId},
				    {dst_bucket_id, DstBucketId0},
				    {src_prefix, proplists:get_value(<<"src_prefix">>, FieldValues)},
				    {dst_prefix, proplists:get_value(<<"dst_prefix">>, FieldValues)},
				    {src_object_keys, proplists:get_value(<<"src_object_keys">>, FieldValues)}
				]}
		    end
	    end;
	false ->
	    User1 = admin_users_handler:user_to_proplist(User0),
	    js_handler:forbidden(Req0, 27, proplists:get_value(groups, User1), stop)
    end.

%%
%% Checks if user has access
%% - To source bucket
%% - To destination bucket
%%
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    copy_forbidden(Req0, State).
