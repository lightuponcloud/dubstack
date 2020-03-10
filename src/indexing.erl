%%
%% Stores per-directory index in Riak CS.
%% Index contains information on renamed and deleted objects.
%%
-module(indexing).
-export([update/2, update/3, get_index/2, get_object_record/2,
	 directory_or_object_exists/4, pseudo_directory_exists/2, to_object/1,
	 get_object_record_by_orig_name/2]).

-include_lib("xmerl/include/xmerl.hrl").

-import(xmerl_xs,
    [ xslapply/2, value_of/1, select/2, built_in_rules/2 ]).

-include("riak.hrl").
-include("entities.hrl").

%%
%% Returns information from metadata and headers.
%% It is stored in index in a form, convenient for JSON serialization.
%%
prepare_object_record(Record0, DeletedObjects) ->
    Metadata = proplists:get_value(metadata, Record0),
    ObjectKey0 = proplists:get_value(key, Record0),
    ObjectKey1 = filename:basename(ObjectKey0),
    OrigName0 = proplists:get_value("x-amz-meta-orig-filename", Metadata, ObjectKey1),
    GUID = proplists:get_value("x-amz-meta-guid", Metadata),
    ModifiedTime =
	case proplists:get_value("x-amz-meta-modified-utc", Metadata) of
	    undefined -> null;
	    T -> utils:to_integer(T)
	end,
    UploadTimestamp0 = proplists:get_value(upload_timestamp, Record0, ""),
    UploadTimestamp1 = calendar:datetime_to_gregorian_seconds(UploadTimestamp0) - 62167219200,
    Bytes =
	case proplists:get_value("x-amz-meta-bytes", Metadata) of
	    undefined -> 0;  %% It might be undefined in case of corrupted metadata
	    B -> utils:to_integer(B)
	end,
    ContentType = proplists:get_value(content_type, Metadata, 0),
    Etag = proplists:get_value(etag, Metadata, ""),
    Md5 = string:strip(Etag, both, $"),

    %% Object meta tag orig-filename can't be renamed,
    %% But I can store a new name in index object
    OrigName1 = unicode:characters_to_list(utils:unhex(erlang:list_to_binary(OrigName0))),
    IsDeleted =
	case proplists:get_value(erlang:list_to_binary(ObjectKey1), DeletedObjects) of
	    undefined ->
		DeletedFlag = proplists:get_value("x-amz-meta-is-deleted", Metadata, "false"),
		erlang:list_to_atom(DeletedFlag);
	    true -> true;
	    _ -> true
	end,
    LockedFlag = proplists:get_value("x-amz-meta-is-locked", Metadata, "false"),
    IsLocked = erlang:list_to_atom(LockedFlag),
    LockUserId =
	case proplists:get_value("x-amz-meta-lock-user-id", Metadata) of
	    undefined -> null;
	    UID -> erlang:list_to_binary(UID)
	end,
    LockUserName =
	case proplists:get_value("x-amz-meta-lock-user-name", Metadata) of
	    undefined -> null;
	    UN -> utils:unhex(erlang:list_to_binary(UN))
	end,
    LockUserTel =
	case proplists:get_value("x-amz-meta-lock-user-tel", Metadata) of
	    undefined -> null;
	    Tel -> utils:unhex(erlang:list_to_binary(Tel))
	end,
    LockModifiedTime =
	case proplists:get_value("x-amz-meta-lock-modified-utc", Metadata) of
	    undefined -> null;
	    V -> utils:to_integer(V)
	end,
    AuthorTel =
	case proplists:get_value("x-amz-meta-author-tel", Metadata) of
	    undefined -> null;
	    C -> utils:unhex(erlang:list_to_binary(C))
	end,
    Result = [{orig_name, unicode:characters_to_binary(OrigName1)},
     {last_modified_utc, ModifiedTime},
     {upload_time, UploadTimestamp1},
     {guid, erlang:list_to_binary(GUID)},
     {author_id, erlang:list_to_binary(proplists:get_value("x-amz-meta-author-id", Metadata))},
     {author_name, utils:unhex(erlang:list_to_binary(proplists:get_value("x-amz-meta-author-name", Metadata)))},
     {author_tel, AuthorTel},
     {is_locked, IsLocked},
     {lock_user_id, LockUserId},
     {lock_user_name, LockUserName},
     {lock_user_tel, LockUserTel},
     {lock_modified_utc, LockModifiedTime},
     {is_deleted, IsDeleted},
     {object_key, erlang:list_to_binary(ObjectKey1)},
     {bytes, Bytes},
     {content_type, unicode:characters_to_binary(ContentType)},
     {md5, unicode:characters_to_binary(Md5)}
    ],
    Width = proplists:get_value("x-amz-meta-width", Metadata),
    Height = proplists:get_value("x-amz-meta-height", Metadata),
    case Width =/= undefined of
	true ->
	    Result ++ [{width, erlang:list_to_integer(Width)},
		       {height, erlang:list_to_integer(Height)}];
	false -> Result
    end.

%%
%% Since index contains JSON values, such as binaries and null's,
%% sometimes it has to be transformed back to usual object record.
%%
-spec to_object(proplist()) -> object().

to_object(IndexMeta) ->
    ModifiedTime =
	case proplists:get_value(last_modified_utc, IndexMeta) of
	    null -> undefined;
	    T -> T
	end,
    LockUserId =
	case proplists:get_value(lock_user_id, IndexMeta) of
	    null -> undefined;
	    UID -> erlang:binary_to_list(UID)
	end,
    LockUserName =
	case proplists:get_value(lock_user_name, IndexMeta) of
	    null -> undefined;
	    UN -> UN
	end,
    LockUserTel =
	case proplists:get_value(lock_user_tel, IndexMeta) of
	    null -> undefined;
	    Tel -> Tel
	end,
    LockModifiedTime =
	case proplists:get_value(lock_modified_utc, IndexMeta) of
	    null -> undefined;
	    V -> V
	end,
    AuthorTel =
	case proplists:get_value(author_tel, IndexMeta) of
	    null -> undefined;
	    C -> C
	end,
    #object{
	key = erlang:binary_to_list(proplists:get_value(object_key, IndexMeta)),
	orig_name = proplists:get_value(orig_name, IndexMeta),
	last_modified_utc = ModifiedTime,
	upload_time = proplists:get_value(upload_time, IndexMeta),
        bytes = proplists:get_value(bytes, IndexMeta),
        guid = erlang:binary_to_list(proplists:get_value(guid, IndexMeta)),
        is_deleted = erlang:atom_to_list(proplists:get_value(is_deleted, IndexMeta)),
        author_id = erlang:binary_to_list(proplists:get_value(author_id, IndexMeta)),
        author_name = proplists:get_value(author_name, IndexMeta),
        author_tel = AuthorTel,
        is_locked = proplists:get_value(is_locked, IndexMeta),
        lock_user_id = LockUserId,
        lock_user_name = LockUserName,
        lock_user_tel = LockUserTel,
	lock_modified_utc = LockModifiedTime,
        md5 = erlang:binary_to_list(proplists:get_value(md5, IndexMeta)),
	content_type = proplists:get_value(content_type, IndexMeta)
    }.

%%
%% Retrieves objects list from Riak CS (all pages),
%% queries detailed attributes of existing objects,
%% removes non-existent objects from index.
%%
%% Returns aggregated list of objects with metadata,
%% as well as list of deleted and renamed objects.
%%
-spec get_diff_list(BucketId, Prefix0, List0, DeletedObjects0, ModifiedKeys, IsUncommitted) -> proplist() when
	BucketId :: string(),
	Prefix0 :: string(),
	List0 :: list(),
	DeletedObjects0 :: list(),
	ModifiedKeys :: proplist(),  %% list of object keys that were modified
	IsUncommitted :: boolean().

get_diff_list(BucketId, Prefix0, List0, DeletedObjects0, ModifiedKeys, IsUncommitted)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_list(List0), erlang:is_list(DeletedObjects0),
	     erlang:is_list(ModifiedKeys), erlang:is_boolean(IsUncommitted) ->
    Prefix1 =
	case Prefix0 of
	    undefined -> undefined;
	    _ -> erlang:list_to_binary(Prefix0)
	end,
    IndexListContents = proplists:get_value(list, List0),
    List1 = fetch_full_list(BucketId, Prefix0),
    ActualListContents = proplists:get_value(list, List1),
    PrefixList0 = proplists:get_value(dirs, List1),

    %% Exclude non-existent objects from deleted list
    DeletedObjects1 =
	case length(DeletedObjects0) of
	    0 -> [];
	    _ -> [{K, proplists:get_value(K, DeletedObjects0)}
		  || K <- proplists:get_keys(DeletedObjects0),
		    proplists:is_defined(
			utils:prefixed_object_key(Prefix0, erlang:binary_to_list(K)),
			ActualListContents) andalso
		    (utils:ends_with(K, <<"/">>) =:= false andalso
			utils:ends_with(K, erlang:list_to_binary(?RIAK_ACTION_LOG_FILENAME)) =:= false andalso
			utils:ends_with(K, erlang:list_to_binary(?RIAK_LOCK_INDEX_FILENAME)) =:= false)
		 ] ++ [{K, proplists:get_value(K, DeletedObjects0)}
		    || K <- proplists:get_keys(DeletedObjects0),
		    lists:member(utils:prefixed_object_key(Prefix0, erlang:binary_to_list(K)), PrefixList0)]
	end,
    %% Filter out non-existent records and modified ones
    List2 = lists:filter(
	fun(R) ->
	    ObjectKey0 = proplists:get_value(object_key, R),
	    IsExist = proplists:is_defined(
		erlang:binary_to_list(utils:prefixed_object_key(Prefix1, ObjectKey0)),
		ActualListContents),
	    IsDeleted = proplists:is_defined(ObjectKey0, DeletedObjects1),
	    IsModified = lists:member(erlang:binary_to_list(ObjectKey0), ModifiedKeys),
	    IsExist andalso IsModified =:= false andalso IsDeleted =:= false
	end, IndexListContents),
    IndexKeys = lists:map(
	fun(R0) ->
	    R1 = utils:prefixed_object_key(Prefix1, proplists:get_value(object_key, R0)),
	    erlang:binary_to_list(R1)
	end, List2),

    %% List3 is the list of objects that should be fetched from Riak CS
    List3 = lists:filtermap(
	fun(R) ->
	    case lists:member(element(1, R), IndexKeys) of
		true -> false;
		false ->
		    Metadata = [
			{metadata, riak_api:get_object_metadata(BucketId, element(1, R))},
			{upload_timestamp, element(2, R)},
			{key, element(1, R)}],
		    {true, prepare_object_record(Metadata, DeletedObjects1)}
	    end
	end, ActualListContents),
    PrefixList1 = [[
		    {prefix, unicode:characters_to_binary(I)},
		    {bytes, 0},
		    {is_deleted, proplists:is_defined(erlang:list_to_binary(filename:basename(I)++"/"),
						      DeletedObjects1)}
		  ] ||  I <- PrefixList0,
		    utils:starts_with(I, erlang:list_to_binary(?RIAK_REAL_OBJECT_PREFIX)) =/= true],
    [{dirs, PrefixList1},
     {list, List2 ++ List3},
     {to_delete, DeletedObjects1},
     {uncommitted, IsUncommitted}].

%%
%% Retrieves objects list from Riak CS with upload timestamp (all pages)
%%
fetch_full_list(BucketId, Prefix)
	when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    fetch_full_list(BucketId, Prefix, [], undefined).

fetch_full_list(BucketId, Prefix, ObjectList0, Marker0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined,
	     erlang:is_list(ObjectList0) ->
    RiakResponse =
	case Prefix of
	    undefined -> riak_api:list_objects(BucketId, [{marker, Marker0}]);
	    _ -> riak_api:list_objects(BucketId, [{prefix, Prefix}, {marker, Marker0}])
	end,
    case RiakResponse of
	not_found -> [{dirs, []}, {list, []}, {renamed, []}, {to_delete, []}];
	_ ->
	    Contents = proplists:get_value(contents, RiakResponse),
	    Marker1 = proplists:get_value(next_marker, RiakResponse),
	    PrefixList0 = proplists:get_value(common_prefixes, RiakResponse),
	    PrefixList1 = [proplists:get_value(prefix, I) || I <- PrefixList0,
		proplists:get_value(prefix, I) =/= "./"],
	    %% Collect all the information about objects
	    ObjectList1 = [{proplists:get_value(key, R), proplists:get_value(last_modified, R)} || R <- Contents,
		utils:is_hidden_object(R) =/= true
	    ],
	    case Marker1 of
		undefined -> [{dirs, PrefixList1}, {list, ObjectList0 ++ ObjectList1}];
		[] -> [{dirs, PrefixList1}, {list, ObjectList0 ++ ObjectList1}];
		NextMarker ->
		    NextPart = fetch_full_list(BucketId, Prefix, ObjectList0 ++ ObjectList1, NextMarker),
		    [{dirs, PrefixList1 ++ proplists:get_value(dirs, NextPart, [])},
		     {list, ObjectList0 ++ proplists:get_value(list, NextPart, [])}]
	    end
    end.

%%
%% Returns contents of existing index.
%%
-spec get_index(list(), list()) -> list().

get_index(BucketId, Prefix0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined ->
    PrefixedIndexFilename = utils:prefixed_object_key(Prefix0, ?RIAK_INDEX_FILENAME),
    %% Get index object in destination directory
    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
	not_found -> [{dirs, []}, {list, []}, {renamed, []}, {to_delete, []}];
	C -> erlang:binary_to_term(proplists:get_value(content, C))
    end.

%%
%% Returns object details, extracted from index.
%%
-spec get_object_record(proplist(), string()|binary()) -> proplist().

get_object_record(IndexContent, ObjectKey) when erlang:is_list(ObjectKey) ->
    get_object_record(IndexContent, erlang:list_to_binary(ObjectKey));
get_object_record(IndexContent, ObjectKey) when erlang:is_binary(ObjectKey) ->
    Record = lists:filter(
	fun(R) ->
	    proplists:get_value(object_key, R) =:= ObjectKey
	end, proplists:get_value(list, IndexContent)),
    case length(Record) of
	0 -> [];
	_ -> lists:nth(1, Record)
    end.

%%
%% Checks if object with specified original name exists
%%
-spec get_object_record_by_orig_name(proplist(), binary()) -> true|false.

get_object_record_by_orig_name(IndexContent, OrigName0)
	when erlang:is_list(IndexContent), erlang:is_binary(OrigName0) ->
    %% Get a lowercase value
    OrigName1 = ux_string:to_lower(unicode:characters_to_list(OrigName0)),
    %% Find first object with matching lowercase original name
    ExistingObjectRecord = utils:firstmatch(
	proplists:get_value(list, IndexContent, []),
	fun(ObjectRecord) ->
	    OrigName2 = proplists:get_value(orig_name, ObjectRecord),
	    OrigName3 = unicode:characters_to_list(OrigName2),
	    ux_string:to_lower(OrigName3) =:= OrigName1
	end),
    case length(ExistingObjectRecord) of
	0 -> not_found;
	_ -> ExistingObjectRecord
    end.

%%
%% Checks if pseudo-directory exists.
%%
%% Returns tuple: {original directory name in lowercase, original hex representation}
%%
-spec pseudo_directory_exists(proplist(), binary()) -> true|false.

pseudo_directory_exists(IndexContent, DirectoryName0)
	when erlang:is_list(IndexContent), erlang:is_binary(DirectoryName0) ->
    ExistingNames =
	lists:map(
	    fun(P0) ->
		P1 = proplists:get_value(prefix, P0),
		P2 = lists:last([T || T <- binary:split(P1, <<"/">>, [global]), erlang:byte_size(T) > 0]),
		P3 = unicode:characters_to_list(utils:unhex(P2)),
		{ux_string:to_lower(P3), P2}
	    end, proplists:get_value(dirs, IndexContent, [])),
    DirectoryName1 = ux_string:to_lower(unicode:characters_to_list(DirectoryName0)),
    lists:keyfind(DirectoryName1, 1, ExistingNames).

%%
%% Checks if object or pseudo-directory is present in index.
%%
%% Returns
%%
%% - {directory, /hex/prefix/}
%% - {object, /hex-prefix/object_key}
%% - {false, /hex-prefixed-hex-name}
%%
-spec directory_or_object_exists(string(), string(), binary(), proplist()) -> any().

directory_or_object_exists(BucketId, Prefix, Name0, IndexContent)
	when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined, erlang:is_binary(Name0) ->
    %% Remove "/" at the end, if present
    Name1 =
	case utils:ends_with(Name0, <<"/">>) of
	    true ->
		Size0 = byte_size(Name0)-1,
		<<Name2:Size0/binary, _/binary>> = Name0,
		Name2;
	    false -> Name0
	end,
    case pseudo_directory_exists(IndexContent, Name1) of
	{_, DirName} -> {directory, DirName};
	false ->
	    case get_object_record_by_orig_name(IndexContent, Name1) of
		not_found -> false;
		ExistingObjectRecord ->
		    case proplists:get_value(is_deleted, ExistingObjectRecord) of
			true -> false;
			false -> {object, proplists:get_value(orig_name, ExistingObjectRecord)}
		    end
	    end
    end.

%%
%% Creates list of objects
%% Stores list in ETF format in bucket/prefix.
%%
%% RiakOptions can include ACL for index object, as well as other Riak CS options
%%
%% Options can include the following:
%% - {modified_keys, ["key1", "key2", .. "keyN"]}
%%
%% - {copy_from, [{bucket_id, name}, {prefix, prefix-to-.riak_index.etf/}]
%%   instructs "update" function to copy deleted objects map from source index
%%
%%   For example:
%%
%%   {copy_from, [
%%     {bucket_id, "the-example-team-public"},
%%     {prefix, "d08732/"},
%%     {copied_names, [
%%             [{src_prefix, "d08732/"},
%%              {dst_prefix, "d08732/d08732/"},
%%              {old_key, "something.random"},
%%              {new_key, "something-1.random"},
%%              {src_orig_name, "Something.Random"},
%%              {dst_orig_name, "Something-1.Random"},
%%              {bytes, 1532357691},
%%              {renamed, true},
%%              {md5, "321e2c18374c5f9920439c314a0448e8"}]
%%     ]}
%%   ]}
%%   ``copied_names`` is optional.
%%
%% - {to_delete, [{"blah.png", 1532357691}]}
%%
%% - {uncommitted, true}
%%   Marks prefix as such that have uncommitted changes.
%%   This flag is set by long-running operations, such as copy/move/rename
%%   to let client know that new changes will appear soon.
%%
-spec update(string(), list()) -> proplist().

update(BucketId, Prefix) when erlang:is_list(BucketId),
	erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    RiakOptions = [{acl, public_read}],  % TODO: public_read
    update(BucketId, Prefix, [], RiakOptions).

-spec update(string(), list(), proplist()) -> proplist().

update(BucketId, Prefix, Options) when erlang:is_list(BucketId),
	erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    RiakOptions = [{acl, public_read}],  % TODO: public_read
    update(BucketId, Prefix, Options, RiakOptions).

-spec update(string(), list(), proplist(), proplist()) -> proplist().

update(BucketId, Prefix0, Options, RiakOptions)
	when erlang:is_list(BucketId),
	     erlang:is_list(Prefix0) orelse Prefix0 =:= undefined ->
    PrefixedLockFilename = utils:prefixed_object_key(Prefix0, ?RIAK_LOCK_INDEX_FILENAME),
    case riak_api:head_object(BucketId, PrefixedLockFilename) of
	not_found ->
	    %% Create lock file instantly
	    LockMeta = [{"modified-utc", erlang:round(utils:timestamp()/1000)}],
	    LockOptions = [{acl, public_read}, {meta, LockMeta}],

	    riak_api:put_object(BucketId, Prefix0, ?RIAK_LOCK_INDEX_FILENAME, <<>>, LockOptions),

	    %% Retrieve existing index object first
	    List0 = get_index(BucketId, Prefix0),

	    ModifiedKeys0 = proplists:get_value(modified_keys, Options, []),

	    DeletedObjects0 = proplists:get_value(to_delete, Options, []),
	    DeletedObjects1 = proplists:get_value(to_delete, List0, []),

	    %% Add deleted object map from source index ( Used in COPY and MOVE ).
	    {ModifiedKeys1, DeletedObjects3} =
		case proplists:get_value(copy_from, Options) of
		    undefined -> {[], []};
		    CopyFrom ->
			SrcBucketId = proplists:get_value(bucket_id, CopyFrom),
			SrcPrefix = proplists:get_value(prefix, CopyFrom),
			PrefixedSrcIndexFilename = utils:prefixed_object_key(SrcPrefix, ?RIAK_INDEX_FILENAME),
			case riak_api:get_object(SrcBucketId, PrefixedSrcIndexFilename) of
			    not_found -> {[], []};
			    Content ->
				CopiedNames = proplists:get_value(copied_names, CopyFrom, []),
				CopiedKeys = [erlang:binary_to_list(proplists:get_value(new_key, I))
					      || I <- CopiedNames],
				SrcList = erlang:binary_to_term(proplists:get_value(content, Content)),
				{CopiedKeys, proplists:get_value(to_delete, SrcList, [])}
			end
		end,
	    ModifiedKeys2 = ModifiedKeys0 ++ ModifiedKeys1,
	    DeletedObjects2 = DeletedObjects0 ++ [
		{K, proplists:get_value(K, DeletedObjects1)} || K <- proplists:get_keys(DeletedObjects1),
		    (proplists:is_defined(K, DeletedObjects0) =/= true andalso
		     lists:member(erlang:binary_to_list(K), ModifiedKeys2) =:= false)
	    ],

	    %% Retrieve existing index object first
	    List0 = get_index(BucketId, Prefix0),

	    IsUncommitted = proplists:get_value(uncommitted, Options, false),
	    List1 = get_diff_list(BucketId, Prefix0, List0,
				  DeletedObjects2 ++ DeletedObjects3,
				  ModifiedKeys2, IsUncommitted),
	    riak_api:put_object(BucketId, Prefix0, ?RIAK_INDEX_FILENAME, term_to_binary(List1), RiakOptions),

	    %% Remove lock
	    riak_api:delete_object(BucketId, PrefixedLockFilename),
	    List1; %% necessary for pseudo_directory_exists()
	IndexLockMeta ->
	    %% Check for stale index
	    DeltaSeconds =
		case proplists:get_value("x-amz-meta-modified-utc", IndexLockMeta) of
		    undefined -> 0;
		    T -> erlang:round(utils:timestamp()/1000) - utils:to_integer(T)
		end,
	    case DeltaSeconds > ?RIAK_LOCK_INDEX_COOLOFF_TIME of
		true ->
		    riak_api:delete_object(BucketId, PrefixedLockFilename),
		    update(BucketId, Prefix0, Options, RiakOptions);
		false -> lock
	    end
    end.
