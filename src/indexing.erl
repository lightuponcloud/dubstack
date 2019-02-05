%%
%% Stores per-directory index in Riak CS.
%% Index contains information on renamed and deleted objects.
%%
-module(indexing).
-export([update/2, update/3, get_index/2, get_object_record/2,
	 object_exists/2, pseudo_directory_exists/2]).

-include_lib("xmerl/include/xmerl.hrl").

-import(xmerl_xs,
    [ xslapply/2, value_of/1, select/2, built_in_rules/2 ]).

-include("riak.hrl").

%%
%% Returns information from metadata and headers
%%
%% ObjectRenameMap -- [{some_object_key: new  name}]
%%
prepare_object_record(Record0, LastModifiedUTCMap, ObjectRenameMap, DeletedObjects, AccessTokens, MD5map) ->
    Metadata = proplists:get_value(metadata, Record0),
    ObjectKey0 = proplists:get_value(key, Record0),
    ObjectKey1 = filename:basename(ObjectKey0),
    OrigName0 =
	case proplists:get_value("x-amz-meta-orig-filename", Metadata, ObjectKey1) of
	    undefined -> ObjectKey1;
	    N -> N
	end,
    ModifiedTime =
	case proplists:get_value(ObjectKey1, LastModifiedUTCMap) of
	    undefined ->
		case proplists:get_value("x-amz-meta-modified-utc", Metadata) of
		    undefined -> "";
		    T0 -> utils:to_integer(T0)
		end;
	    T1 -> T1
	end,
    UploadTimestamp0 = proplists:get_value(upload_timestamp, Record0, ""),
    UploadTimestamp1 = calendar:datetime_to_gregorian_seconds(UploadTimestamp0) - 62167219200,
    Bytes =
	case proplists:get_value(content_length, Metadata, undefined) of
	    undefined -> 0;
	    Value -> utils:to_integer(Value)
	end,
    ContentType = proplists:get_value(content_type, Metadata, 0),
    Md5 =
	case proplists:get_value(ObjectKey0, MD5map) of
	    undefined ->
		Etag = proplists:get_value(etag, Metadata, ""),
		string:strip(Etag, both, $");
	    V -> V
	end,
    %% Object meta tag orig-filename can't be renamed,
    %% But I can store a new name in index object.
    OrigName1 =
	case proplists:get_value(ObjectKey1, ObjectRenameMap) of
	    undefined ->
		%% If not renamed, use original name by default
		unicode:characters_to_list(erlang:list_to_binary(OrigName0));
	    Attrs ->
		%% Check if key in rename map belongs to that object
		case proplists:get_value(bytes, Attrs) =:= Bytes of
		    true -> proplists:get_value(name, Attrs);
		    false -> unicode:characters_to_list(erlang:list_to_binary(OrigName0))
		end
	end,
    IsDeleted =
	case proplists:get_value(erlang:list_to_binary(ObjectKey1), DeletedObjects) of
	    undefined -> false;
	    _ -> true
	end,
    Token =
	case proplists:get_value(ObjectKey0, AccessTokens) of
	    undefined -> undefined;
	    T2 -> unicode:characters_to_binary(T2)
	end,
    [{object_key, erlang:list_to_binary(ObjectKey1)},
     {orig_name, unicode:characters_to_binary(OrigName1)},
     {bytes, Bytes},
     {content_type, unicode:characters_to_binary(ContentType)},
     {upload_time, UploadTimestamp1},
     {last_modified_utc, ModifiedTime},
     {is_deleted, IsDeleted},
     {md5, unicode:characters_to_binary(Md5)},
     {access_token, Token}].

%%
%% Retrieves objects list from Riak CS (all pages),
%% queries detailed attributes of existing objects,
%% removes non-existent objects from index.
%%
%% Returns aggregated list of objects with metadata,
%% as well as list of deleted and renamed objects.
%%
-spec get_diff_list(BucketId, Prefix0, List0, LastModifiedUTCMap, ObjectRenameMap0, DeletedObjects0, AccessTokens0, MD5map0,
	    IsUncommitted) -> proplist() when
	BucketId :: string(),
	Prefix0 :: string(),
	List0 :: list(),
	LastModifiedUTCMap :: proplist(),  %% Map of object keys and their last_modified_utc values
	ObjectRenameMap0 :: proplist(),    %% Stored map of keys and new names
	DeletedObjects0 :: proplist(),     %% list of objects, marked as deleted
	AccessTokens0 :: proplist(),       %% list of secret tokens, used to check if user can read object by URL
	MD5map0 :: proplist(),             %% since COPY operation destroys MD5, it has to be saved
	IsUncommitted :: boolean().

get_diff_list(BucketId, Prefix0, List0, LastModifiedUTCMap0, ObjectRenameMap0, DeletedObjects0, AccessTokens0, MD5map0, IsUncommitted)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_list(List0), erlang:is_list(ObjectRenameMap0), erlang:is_list(LastModifiedUTCMap0),
	     erlang:is_list(DeletedObjects0), erlang:is_list(AccessTokens0), erlang:is_boolean(IsUncommitted) ->
    Prefix1 =
	case Prefix0 of
	    undefined -> undefined;
	    _ -> erlang:list_to_binary(Prefix0)
	end,
    IndexListContents = proplists:get_value(list, List0),
    List1 = fetch_full_list(BucketId, Prefix0),
    ActualListContents = proplists:get_value(list, List1),
    PrefixList0 = proplists:get_value(dirs, List1),

    %% Exclude non-existent objects from lastModified map
    LastModifiedUTCMap1 =
	case length(LastModifiedUTCMap0) of
	    0 -> [];
	    _ -> [{K, proplists:get_value(K, LastModifiedUTCMap0)}
		  || K <- proplists:get_keys(LastModifiedUTCMap0),
		  proplists:is_defined(utils:prefixed_object_key(Prefix0, K), ActualListContents)]
	end,
    %% Exclude non-existent objects from rename map
    ObjectRenameMap1 =
	case length(ObjectRenameMap0) of
	    0 -> [];
	    _ -> [{K, proplists:get_value(K, ObjectRenameMap0)}
		  || K <- proplists:get_keys(ObjectRenameMap0),
		  proplists:is_defined(utils:prefixed_object_key(Prefix0, K), ActualListContents)]
	end,
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
    %% Filter out non-existent records, those that were renamed and marked as deleted
    List2 = lists:filter(
	fun(R) ->
	    ObjectKey0 = proplists:get_value(object_key, R),
	    ObjectKey1 = erlang:binary_to_list(ObjectKey0),
	    IsExist = proplists:is_defined(
		erlang:binary_to_list(utils:prefixed_object_key(Prefix1, ObjectKey0)),
		ActualListContents),
	    IsRenamed = proplists:is_defined(ObjectKey1, ObjectRenameMap1),
	    IsDeleted = proplists:is_defined(ObjectKey1, DeletedObjects1),
	    IsModified = proplists:is_defined(ObjectKey1, LastModifiedUTCMap1),
	    IsExist andalso IsRenamed =:= false andalso IsDeleted =:= false andalso IsModified =:=  false
	end, IndexListContents),

    %% Add access tokens to those objects that do not have them assigned yet.
    %% Those tokens are stored in index file, as they can be changed by user.
    %% Exclude tokens for non-existent objects.
    AccessTokens1 = [
	{K, riak_crypto:random_string()} || K <- proplists:get_keys(ActualListContents),
	proplists:is_defined(K, AccessTokens0) =:= false andalso
	utils:is_hidden_object([{key, K}]) =:= false
	] ++ [I || I <- AccessTokens0, proplists:is_defined(element(1, I), ActualListContents)],

    %% Fetch the missing and modified records ( renamed or updated ones )
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
		    {true, prepare_object_record(Metadata, LastModifiedUTCMap1, ObjectRenameMap1,
						 DeletedObjects1, AccessTokens1, MD5map0)}
	    end
	end, ActualListContents),
    [{dirs, [[{prefix, unicode:characters_to_binary(I)},
	      {bytes, 0},
	      {is_deleted, proplists:is_defined(erlang:list_to_binary(filename:basename(I)++"/"), DeletedObjects1)}]
	     || I <- PrefixList0]},
     {list, List2 ++ List3},
     {renamed, ObjectRenameMap1},
     {to_delete, DeletedObjects1},
     {access_tokens, AccessTokens1},
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
	not_found -> [{dirs, []}, {list, []}, {renamed, []}, {to_delete, []}, {access_tokens, []}];
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
-spec object_exists(proplist(), binary()) -> true|false.

object_exists(IndexContent, OrigName0)
	when erlang:is_list(IndexContent), erlang:is_binary(OrigName0) ->
    ExistingNames = lists:map(
	fun(I) ->
	    OrigName1 = proplists:get_value(orig_name, I),
	    OrigName2 = unicode:characters_to_list(OrigName1),
	    ux_string:to_lower(OrigName2)
	end, proplists:get_value(list, IndexContent, [])),
    OrigName3 = ux_string:to_lower(unicode:characters_to_list(OrigName0)),
    lists:member(OrigName3, ExistingNames).
    
%%
%% Checks if pseudo-directory exists.
%%
-spec pseudo_directory_exists(proplist(), list()|binary()) -> true|false.

pseudo_directory_exists(IndexContent, DirectoryName0)
	when erlang:is_list(DirectoryName0), erlang:is_list(IndexContent) ->
    DirectoryName1 = unicode:characters_to_binary(utils:unhex_path(DirectoryName0)),
    pseudo_directory_exists(IndexContent, DirectoryName1);

pseudo_directory_exists(IndexContent, DirectoryName0)
	when erlang:is_binary(DirectoryName0), erlang:is_list(IndexContent) ->
    ExistingPrefixes = lists:map(
	fun(P0) ->
	    P1 = proplists:get_value(prefix, P0),
	    P2 = unicode:characters_to_list(utils:unhex_path(erlang:binary_to_list(P1))),
	    ux_string:to_lower(P2)
	end, proplists:get_value(dirs, IndexContent, [])),
    DirectoryName1 = ux_string:to_lower(unicode:characters_to_list(DirectoryName0)),
    lists:member(DirectoryName1, ExistingPrefixes).

%%
%% Creates list of objects
%% Stores list in ETF format in bucket/prefix.
%%
%% RiakOptions can include ACL for index object, as well as other Riak CS options
%%
%% Options can include the following:
%% - {renamed, {<some object name>, [{name, <new name>}, {bytes, <size in bytes>}]}}
%%   orig_name of Riak CS object will be changed in index to "new one"
%%
%%   For example:
%%    {renamed, [{"blah.png", [{name,[1111,1111,46,112,110,103]}, {bytes,43523}]}]}
%%
%% - {copy_from, [{bucket_id, name}, {prefix, prefix-to-.riak_index.etf/}]
%%   instructs "update" function to copy renamed objects map from source index
%%
%%   For example:
%%
%%   {copy_from, [
%%	{bucket_id, "the-example-team-public"},
%%	{prefix, "d08732/"},
%%	{copied_names, [
%%		[{src_prefix, "d08732/"},
%%		 {dst_prefix, "d08732/d08732/"},
%%		 {old_key, "something.random"},
%%		 {new_key, "something-1.random"},
%%		 {src_orig_name, "Something.Random"},
%%		 {dst_orig_name, "Something-1.Random"},
%%		 {bytes, 1532357691},
%%		 {renamed, true},
%%		 {md5, "321e2c18374c5f9920439c314a0448e8"}]
%%	]}
%%   ]}
%%   ``copied_names`` is optional.
%%
%% - {last_modified_map, [{"blah.png", 1547059658}]}
%%
%% - {to_delete, [{"blah.png", 1532357691}]}
%%
%% - {undelete, ["blah.png"]}
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
	    riak_api:put_object(BucketId, Prefix0, ?RIAK_LOCK_INDEX_FILENAME, <<>>, RiakOptions),

	    %% Retrieve existing index object first
	    List0 = get_index(BucketId, Prefix0),

	    %% Existing object could have been modified. If time is different, it should be updated.
	    LastModifiedUTCMap0 = proplists:get_value(last_modified_map, Options, []),
	    LastModifiedUTCMap1 = proplists:get_value(last_modified_map, List0, []),
	    LastModifiedUTCMap2 = LastModifiedUTCMap0 ++ [
		{K, proplists:get_value(K, LastModifiedUTCMap1)}
		|| K <- proplists:get_keys(LastModifiedUTCMap1),
		(proplists:is_defined(K, LastModifiedUTCMap0) =/= true)
	    ],

	    %% Take renamed object list from options and the retrieved one
	    ObjectRenameMap0 = proplists:get_value(renamed, Options, []),
	    ObjectRenameMap1 = proplists:get_value(renamed, List0, []),

	    %% Take deleted object list from options and the retrieved one
	    DeletedObjects0 = proplists:get_value(to_delete, Options, []),
	    DeletedObjects1 = proplists:get_value(to_delete, List0, []),
	    UndeleteList = proplists:get_value(undelete, Options, []),

	    %% Merge map from Options with the map from Riak CS index
	    ObjectRenameMap2 = ObjectRenameMap0 ++ [
		{K, proplists:get_value(K, ObjectRenameMap1)}
		|| K <- proplists:get_keys(ObjectRenameMap1),
		(proplists:is_defined(K, ObjectRenameMap0) =/= true)
	    ],
	    DeletedObjects2 = DeletedObjects0 ++ [
		{K, proplists:get_value(K, DeletedObjects1)} || K <- proplists:get_keys(DeletedObjects1),
		(proplists:is_defined(K, DeletedObjects0) =/= true andalso
		 lists:member(K, UndeleteList) =:= false)
	    ],
	    %% Add renamed/deleted object maps from source index ( Used in COPY and MOVE ).
	    [LastModifiedUTCMap3, ObjectRenameMap3, DeletedObjects3, MD5map0] =
		case proplists:get_value(copy_from, Options) of
		    undefined -> [[], [], [], []];
		    CopyFrom ->
			SrcBucketId = proplists:get_value(bucket_id, CopyFrom),
			SrcPrefix = proplists:get_value(prefix, CopyFrom),
			CopiedNames1 = proplists:get_value(copied_names, CopyFrom, []),
			DstKeys = [proplists:get_value(new_key, I) || I <- CopiedNames1],
			PrefixedSrcIndexFilename = utils:prefixed_object_key(SrcPrefix, ?RIAK_INDEX_FILENAME),
			case riak_api:get_object(SrcBucketId, PrefixedSrcIndexFilename) of
			    not_found -> [[], [], [], []];
			    Content ->
				SrcList = erlang:binary_to_term(proplists:get_value(content, Content)),
				%% Add new renamed AND copied objects to rename map.
				%% We avoid overwriting objects on desktop client this way
				ObjectRenameMap4 = [I || I <- proplists:get_value(renamed, SrcList, []),
						    lists:member(element(1, I), DstKeys)],
				%% Go through copied object _keys_ and find renamed ones
				%% Add copied-renamed objects to renamed map
				ObjectRenameMap5 = [{proplists:get_value(new_key, P), [
				    {name, proplists:get_value(dst_orig_name, P)},
				    {bytes, proplists:get_value(bytes, P)}]}
				 || P <- CopiedNames1, proplists:get_value(renamed, P) =:= true],
				ObjectRenameMap6 = lists:filter(
				    fun({K, _}) -> proplists:is_defined(K, ObjectRenameMap5) =:= false end,
				    ObjectRenameMap4),
				UTCMap = lists:map(
				    fun(I) ->
					NewKey = proplists:get_value(new_key, I),
					DstPrefix = proplists:get_value(dst_prefix, I),
					PrefixedNewKey = utils:prefixed_object_key(DstPrefix, NewKey),
					{PrefixedNewKey, proplists:get_value(last_modified_utc, I)}
				    end, CopiedNames1),
				MD5map1 = lists:map(
				    fun(I) ->
					NewKey = proplists:get_value(new_key, I),
					DstPrefix = proplists:get_value(dst_prefix, I),
					PrefixedNewKey = utils:prefixed_object_key(DstPrefix, NewKey),
					{PrefixedNewKey, proplists:get_value(md5, I)}
				    end, CopiedNames1),
				[UTCMap, ObjectRenameMap5 ++ ObjectRenameMap6,
				 proplists:get_value(to_delete, SrcList, []),
				 MD5map1]
			end
		end,
	    IsUncommitted = proplists:get_value(uncommitted, Options, false),
	    List1 = get_diff_list(BucketId, Prefix0, List0, LastModifiedUTCMap2 ++ LastModifiedUTCMap3,
		ObjectRenameMap2 ++ ObjectRenameMap3, DeletedObjects2 ++ DeletedObjects3,
		proplists:get_value(access_tokens, List0, []), MD5map0, IsUncommitted),
	    riak_api:put_object(BucketId, Prefix0, ?RIAK_INDEX_FILENAME, term_to_binary(List1), RiakOptions),
	    %% Remove lock
	    riak_api:delete_object(BucketId, PrefixedLockFilename),
	    proplists:get_value(list, List1);
	_ -> lock
    end.
