%%%
%%% Renames object
%%%
%%% Check priv/en.json for error codes.
%%%
-module(rename_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, forbidden/2, is_authorized/2,
	 handle_post/2, rename_pseudo_directory/6, rename_pseudo_directory/5]).

-include("riak.hrl").
-include("entities.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Returns callback 'to_json()'
%% ( called after 'forbidden()' )
%%
content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

%%
%% Returns callback 'handle_post()'
%% ( called after 'resource_exists()' )
%%
content_types_accepted(Req, State) ->
    case cowboy_req:method(Req) of
	<<"POST">> -> {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State};
	_ -> {[], Req, State}
    end.

%%
%% Checks if provided source prefix is valid.
%% If object key ends with "/", it checks this prefix as well.
%%
validate_src_object_key(BucketId, Prefix0, SrcObjectKey0) ->
    case list_handler:validate_prefix(BucketId, Prefix0) of
	{error, Number0} -> {error, Number0};
	Prefix1 ->
	    case utils:ends_with(SrcObjectKey0, <<"/">>) of
		true ->
		    case list_handler:validate_prefix(BucketId,
			    utils:prefixed_object_key(Prefix0, SrcObjectKey0)) of
			{error, Number1} -> {error, Number1};
			SrcObjectKey1 -> {Prefix1, SrcObjectKey1}
		    end;
		false -> {Prefix1, string:to_lower(erlang:binary_to_list(SrcObjectKey0))}
	    end
    end.

%%
%% Check if correct source directory provided
%%
check_src_dir(BucketId, Prefix) ->
    PrefixedIndexFilename = utils:prefixed_object_key(Prefix, ?RIAK_INDEX_FILENAME),
    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
	not_found -> {error, 9};
	IndexContent0 -> erlang:binary_to_term(proplists:get_value(content, IndexContent0))
    end.

%%
%% Checks if pseudo-directory exists, by requesting its index object and
%% then checking if requested name is present.
%%
%% Returns the following
%%
%% - Prefixed destination object name
%% - {error, code}
%%
validate_dst_name(BucketId, Prefix, DstName0, IndexContent)
	when erlang:is_binary(DstName0) ->
    case IndexContent of
	{error, Number} -> {error, Number};
	_ ->
	    case indexing:directory_or_object_exists(BucketId, Prefix, DstName0, IndexContent) of
		{directory, _DirName} -> {error, 10};
		{object, _OrigName} -> {error, 29};
		false -> utils:prefixed_object_key(Prefix, utils:hex(DstName0))
	    end
    end.

%%
%% Checks if object exists and if not marked as deleted.
%%
validate_src_dst_name(BucketId, Prefix, SrcObjectKey, DstName0, IndexContent)
	when erlang:is_binary(SrcObjectKey) andalso erlang:is_binary(DstName0) ->
    case validate_dst_name(BucketId, Prefix, DstName0, IndexContent) of
	{error, Reason} -> {error, Reason};
	PrefixedDstName ->
           case utils:ends_with(SrcObjectKey, <<"/">>) of
               true ->
		    case list_handler:validate_prefix(BucketId, SrcObjectKey) of
			{error, Number} -> {error, Number};
			SrcPrefix -> {SrcPrefix, PrefixedDstName}
		    end;
               false ->
		    %% Check source object key or prefix
		    case indexing:get_object_record(IndexContent, SrcObjectKey) of
			[] -> {error, 9};
			_ -> {SrcObjectKey, PrefixedDstName}
		    end
	    end
    end.

validate_post(BucketId, FieldValues) ->
    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
    SrcObjectKey0 = proplists:get_value(<<"src_object_key">>, FieldValues),
    DstObjectName0 = proplists:get_value(<<"dst_object_name">>, FieldValues),
    case (SrcObjectKey0 =:= undefined orelse DstObjectName0 =:= undefined) of
	true -> {error, 9};
	false ->
	    case validate_src_object_key(BucketId, Prefix0, SrcObjectKey0) of
		{error, Number} -> {error, Number};
		{Prefix1, SrcObjectKey1} ->
		    case utils:is_valid_object_key(DstObjectName0) of
			false -> {error, 9};
			true ->
			    [{prefix, Prefix1},
			     {src_object_key, SrcObjectKey1},
			     {dst_object_name, DstObjectName0}]
		    end
	    end
    end.

%%
%% Validates provided fields and updates object index, in case of object rename
%% or moves objects to the new prefix, in case prefix rename requested.
%%
handle_post(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case jsx:is_json(Body) of
	{error, badarg} -> js_handler:bad_request(Req1, 21);
	false -> js_handler:bad_request(Req1, 21);
	true ->
	    FieldValues = jsx:decode(Body),
	    case validate_post(BucketId, FieldValues) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		State1 ->
		    Prefix0 = proplists:get_value(prefix, State1),
		    SrcObjectKey0 = erlang:list_to_binary(proplists:get_value(src_object_key, State1)),
		    DstObjectName0 = proplists:get_value(dst_object_name, State1),
		    IndexContent = check_src_dir(BucketId, Prefix0),
		    case validate_src_dst_name(BucketId, Prefix0, SrcObjectKey0, DstObjectName0, IndexContent) of
			lock -> js_handler:too_many(Req0);
			{error, Number} -> js_handler:bad_request(Req0, Number);
			{SrcObjectKey1, PrefixedDstDirectoryName1} ->
			    State2 = lists:keyreplace(src_object_key, 1, State1, {src_object_key, SrcObjectKey1}) ++
				[{prefixed_dst_directory_name, PrefixedDstDirectoryName1++"/"}],
			    rename(Req0, BucketId, State0 ++ State2, IndexContent)
		    end
	    end
    end.

%%
%% Copies file and checks the status of copy.
%% Returns ok in case of success. Otherwise returns filename.
%%
copy_delete(BucketId, PrefixedSrcDirectoryName, PrefixedDstDirectoryName, PrefixedObjectKey0) ->
    PrefixedObjectKey1 = re:replace(PrefixedObjectKey0, "^" ++ PrefixedSrcDirectoryName, "", [{return, list}]),
    %% We have cheked previously that destination directory do not exist
    PrefixedObjectKey2 = utils:prefixed_object_key(PrefixedDstDirectoryName, PrefixedObjectKey1),

    CopyResult = riak_api:copy_object(BucketId, PrefixedObjectKey2, BucketId, PrefixedObjectKey0, [{acl, public_read}]),
    case proplists:get_value(content_length, CopyResult, 0) == 0 of
	true -> PrefixedObjectKey2;
	false ->
	    %% Delete regular object
	    riak_api:delete_object(BucketId, PrefixedObjectKey0),
	    PrefixedObjectKey2,
	    ok
    end.

%%
%% Rename can be performed within one bucket only for now.
%%
%% Prefix0 -- current pseudo-directory
%%
%% SrcDirectoryName0 -- hex encoded pseudo-directory, that should be renamed
%%
rename_pseudo_directory(BucketId, Prefix0, PrefixedSrcDirectoryName, DstDirectoryName0,
			ActionLogRecord0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_list(PrefixedSrcDirectoryName), erlang:is_binary(DstDirectoryName0) ->
    PrefixedDstDirectoryName0 =
	case Prefix0 of
	    undefined -> utils:hex(DstDirectoryName0);
	    _ -> utils:prefixed_object_key(Prefix0, utils:hex(DstDirectoryName0))
	end,
    rename_pseudo_directory(BucketId, Prefix0, PrefixedSrcDirectoryName, DstDirectoryName0,
			    PrefixedDstDirectoryName0, ActionLogRecord0).

-spec rename_pseudo_directory(BucketId, Prefix, PrefixedSrcDirectoryName, DstDirectoryName,
			      PrefixedDstDirectoryName0, ActionLogRecord) ->
    not_found|exists|true when
    BucketId :: string(),
    Prefix :: string()|undefined,
    PrefixedSrcDirectoryName :: string(),
    PrefixedDstDirectoryName0 :: string(),  %% prefixed hex-encoded directory name
    DstDirectoryName :: binary(),           %% original directory name
    ActionLogRecord :: riak_action_log_record().

rename_pseudo_directory(BucketId, Prefix0, PrefixedSrcDirectoryName, DstDirectoryName0,
			PrefixedDstDirectoryName0, ActionLogRecord0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_list(PrefixedSrcDirectoryName), erlang:is_binary(DstDirectoryName0) ->
    List0 = riak_api:recursively_list_pseudo_dir(BucketId, PrefixedSrcDirectoryName),
    %% Move only not locked object keys.
    %% Leave locked ones where they are.
    RenameResult0 = [copy_delete(BucketId, PrefixedSrcDirectoryName,
				 PrefixedDstDirectoryName0, PrefixedObjectKey)
		     || PrefixedObjectKey <- List0,
		     utils:is_hidden_prefix(PrefixedObjectKey) =:= false andalso
		     lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectKey) =/= true andalso
		     lists:member(PrefixedObjectKey ++ ?RIAK_LOCK_SUFFIX, List0) =:= false],
    LockedPaths0 = [filename:dirname(PrefixedObjectKey) || PrefixedObjectKey <- List0,
        utils:is_hidden_prefix(PrefixedObjectKey) =:= false andalso
	lists:member(PrefixedObjectKey ++ ?RIAK_LOCK_SUFFIX, List0) =:= true],
    %% generate list of paths to keep
    LockedPaths1 = lists:map(
        fun(P) ->
            Bits = string:tokens(P, "/"),
            lists:flatten([lists:foldr(
                fun (A, B) ->
                    lists:join("/", [A, B])
                end, [], lists:sublist(Bits, I)) || I <- lists:seq(1, length(Bits))])
        end, LockedPaths0),
    %% Update indices for nested pseudo-directories
    PseudoDirectoryMoveResult = lists:map(
	fun(PrefixedObjectKey) ->
	    case lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectKey) of
		false -> ok;
		true ->
		    SrcPrefix =
			case filename:dirname(PrefixedObjectKey) of
			    "." -> undefined;
			    P0 -> P0++"/"
			end,
		    DstKey0 = re:replace(PrefixedObjectKey, "^"++PrefixedSrcDirectoryName,
					 "", [{return, list}]),
		    DstPrefix =
			case utils:prefixed_object_key(PrefixedDstDirectoryName0, DstKey0) of
			    "." -> undefined;
			    P1 ->
				case filename:dirname(P1) of
				    "." -> undefined;
				    P2 -> P2++"/"
				end
			end,
		    case indexing:update(BucketId, DstPrefix, [{copy_from, [
					 {bucket_id, BucketId}, {prefix, SrcPrefix}]}]) of
			lock -> filename:dirname(PrefixedObjectKey);
			_ ->
			    case lists:member(filename:dirname(PrefixedObjectKey)++"/", LockedPaths1) of
				true -> 
				    case indexing:update(BucketId, filename:dirname(PrefixedObjectKey)++"/") of
					lock -> filename:dirname(PrefixedObjectKey);
					_ -> ok
				    end;
				false ->
				    riak_api:delete_object(BucketId, PrefixedObjectKey),
				    ok
			    end
		    end
	    end
	end, List0),
    RenameErrors1 = [I || I <- PseudoDirectoryMoveResult, I =/= ok],
    RenameErrors2 = [I || I <- RenameResult0, I =/= ok],
    case length(RenameErrors1) =:= 0 andalso length(RenameErrors2) =:= 0 of
	false -> {accepted, {RenameErrors1, RenameErrors2}};
	true ->
	    DstDirectoryName1 = utils:hex(DstDirectoryName0),
	    case ActionLogRecord0#riak_action_log_record.action of
		"delete" ->
		    %%
		    %% This function can be called when user deletes directory
		    %% In this case we might need to rename directory, in order
		    %% to allow "undelete" operation later.
		    %%
		    Timestamp = ActionLogRecord0#riak_action_log_record.timestamp,
		    case indexing:update(BucketId, Prefix0, [{to_delete,
					 [{erlang:list_to_binary(DstDirectoryName1++"/"), Timestamp}]}]) of
			lock -> lock;
			_ ->
			    DstDirectoryName2 = unicode:characters_to_list(DstDirectoryName0),
			    Summary0 = lists:flatten([["Deleted directory \""], DstDirectoryName2, ["/\"."]]),
			    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
			    action_log:add_record(BucketId, Prefix0, ActionLogRecord1),
			    {dir_name, DstDirectoryName0}
		    end;
		_ ->
		    %% Update pseudo-directory index
		    case indexing:update(BucketId, Prefix0) of
			lock -> lock;
			_ ->
			    SrcObjectKey0 = filename:basename(PrefixedSrcDirectoryName),
			    SrcObjectKey1 = unicode:characters_to_list(utils:unhex(erlang:list_to_binary(SrcObjectKey0))),
			    DstDirectoryName2 = unicode:characters_to_list(DstDirectoryName0),
			    Summary0 = lists:flatten([["Renamed \""], [SrcObjectKey1, "\" to \"", DstDirectoryName2, "\""]]),
			    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
			    action_log:add_record(BucketId, Prefix0, ActionLogRecord1),
			    {dir_name, DstDirectoryName0}
		    end
	    end
    end.

%%
%% Returns error, if object is locked.
%% Otherwise returns metadata.
%%
get_object_meta(IsLocked, LockUserId, User, BucketId, Prefix0, SrcObjectKey0) ->
    RenameAllowed =
	case IsLocked of
	    true ->
		case LockUserId =/= undefined andalso User#user.id =/= LockUserId of
		    true -> false;
		    false -> true
		end;
	    false -> true
	end,
    case RenameAllowed of
	false -> {error, 43};
	true ->
	    PrefixedSrcObjectKey = utils:prefixed_object_key(Prefix0, SrcObjectKey0),
	    case riak_api:head_object(BucketId, PrefixedSrcObjectKey) of
		not_found -> not_found;
		Metadata0 ->
		    case proplists:get_value("x-amz-meta-is-deleted", Metadata0) of
			"true" -> not_found;
			_ -> Metadata0
		    end
	    end
    end.

%%
%% Copies object using new name, deletes old object.
%%
rename_object(BucketId, Prefix0, SrcObjectKey0, DstObjectName0, User, IndexContent, ActionLogRecord0) ->
    PrefixedSrcObjectKey = utils:prefixed_object_key(Prefix0, SrcObjectKey0),

    %% We can't just update index with new name, as further file upload or rename operations
    %% would complain "object exist". Provided they have the same object name.
    UserName = utils:unhex(erlang:list_to_binary(User#user.name)),

    {ObjectKey0, OrigName0, _IsNewVersion, ExistingObject0, _IsConflict} = riak_api:pick_object_key(
	    BucketId, Prefix0, DstObjectName0, 0, undefined, UserName, IndexContent),
    %% Check if target object exists and if it locked.
    %% If not, check if source object is locked.
    {IsLocked, LockUserId} =
	case ExistingObject0 of
	    undefined ->
		%% Check if source object is locked
		PrefixedSrcLockKey0 = utils:prefixed_object_key(Prefix0, SrcObjectKey0 ++ ?RIAK_LOCK_SUFFIX),
		case riak_api:head_object(BucketId, PrefixedSrcLockKey0) of
		    not_found -> {false, undefined};
		    LockMeta -> {true, proplists:get_value("x-amz-meta-lock-user-id", LockMeta)}
		end;
	    _ -> {ExistingObject0#object.is_locked, ExistingObject0#object.lock_user_id}
	end,
    case get_object_meta(IsLocked, LockUserId, User, BucketId, Prefix0, SrcObjectKey0) of
	{error, Number} -> {error, Number};
	not_found -> not_found;
	Metadata0 ->
	    Meta = list_handler:parse_object_record(Metadata0, [{orig_name, utils:hex(OrigName0)}]),
	    case riak_api:put_object(BucketId, Prefix0, ObjectKey0, <<>>,
				     [{acl, public_read}, {meta, Meta}]) of
		ok ->
		    case ObjectKey0 =:= SrcObjectKey0 of
			true -> ok;  %% Don't delete object, as it has replaced another one
			false -> riak_api:delete_object(BucketId, PrefixedSrcObjectKey) %% Delete a regular object
		    end,
		    %% Rename lock file name
		    case IsLocked of
			true ->
			    PrefixedSrcLockKey1 = PrefixedSrcObjectKey ++ ?RIAK_LOCK_SUFFIX,
			    PrefixedDstLockKey = utils:prefixed_object_key(Prefix0, ObjectKey0 ++ ?RIAK_LOCK_SUFFIX),
			    riak_api:copy_object(BucketId, PrefixedSrcLockKey1, BucketId, PrefixedDstLockKey),
			    riak_api:delete_object(BucketId, PrefixedDstLockKey);
			false -> ok
		    end,
		    %% Find original object name for action log record
		    ObjectRecord = lists:nth(1,
			[I || I <- proplists:get_value(list, IndexContent),
			 proplists:get_value(object_key, I) =:= erlang:list_to_binary(SrcObjectKey0)]),
		    PreviousOrigName = unicode:characters_to_list(proplists:get_value(orig_name, ObjectRecord)),

		    %% Update objects index
		    case indexing:update(BucketId, Prefix0, [{modified_keys, [ObjectKey0]}]) of
			lock -> lock;
			_ ->
			    %% Create action log record
			    OrigName2 = unicode:characters_to_list(OrigName0),
			    Summary1 = lists:flatten([["Renamed \""], [PreviousOrigName], ["\" to \""],
						      [OrigName2], ["\""]]),
			    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
			    action_log:add_record(BucketId, Prefix0, ActionLogRecord2),
			    [{orig_name, unicode:characters_to_binary(OrigName2)}]
		    end;
		_ -> {error, 5}
	    end
    end.

rename(Req0, BucketId, State, IndexContent) ->
    Prefix0 = proplists:get_value(prefix, State),
    SrcObjectKey0 = proplists:get_value(src_object_key, State),
    DstObjectName0 = proplists:get_value(dst_object_name, State),
    PrefixedDstDirectoryName = proplists:get_value(prefixed_dst_directory_name, State),
    User = proplists:get_value(user, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="rename",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [erlang:round(utils:timestamp()/1000)])
    },
    SrcObjectKey1 = utils:to_list(SrcObjectKey0),
    case utils:ends_with(SrcObjectKey1, <<"/">>) of
	true ->
	    case rename_pseudo_directory(BucketId, Prefix0, SrcObjectKey1, DstObjectName0,
					 PrefixedDstDirectoryName, ActionLogRecord0) of
		lock -> js_handler:too_many(Req0);
		{accepted, {RenameErrors1, RenameErrors2}} ->
		    %% Rename is not complete, as Riak CS was busy.
		    %% Return names of objects that were not copied.
		    Req1 = cowboy_req:reply(202, #{
			<<"content-type">> => <<"application/json">>
		    }, jsx:encode([{dir_errors, RenameErrors1}, {object_errors, RenameErrors2}]), Req0),
		    {true, Req1, []};
		{error, Number} -> js_handler:bad_request(Req0, Number);
		{dir_name, DstDirectoryName0} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{dir_name, DstDirectoryName0}]), Req0),
		    {true, Req1, []}
	    end;
	false ->
	    case rename_object(BucketId, Prefix0, SrcObjectKey1, DstObjectName0, User, IndexContent, ActionLogRecord0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		lock -> js_handler:too_many(Req0);
		not_found -> js_handler:not_found(Req0);
		Body ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode(Body), Req0),
		    {true, Req1, []}
	    end
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
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, User) ->
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
	true ->
	    UserBelongsToGroup = lists:any(fun(Group) ->
		utils:is_bucket_belongs_to_group(BucketId, User#user.tenant_id, Group#group.id) end,
		User#user.groups),
	    case UserBelongsToGroup of
		false ->
		    PUser = admin_users_handler:user_to_proplist(User),
		    js_handler:forbidden(Req0, 37, proplists:get_value(groups, PUser));
		true -> {false, Req0, [{user, User}, {bucket_id, BucketId}]}
	    end;
	false -> js_handler:forbidden(Req0, 7)
    end.
