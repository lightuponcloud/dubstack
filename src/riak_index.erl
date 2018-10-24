%%
%% Stores per-directory index in Riak CS.
%% Index contains information on renamed and deleted objects.
%%
-module(riak_index).
-export([update/2, update/3, get_detailed_list/5, get_full_list/2,
	 get_index/2, get_object_record/2]).

-include_lib("xmerl/include/xmerl.hrl").

-import(xmerl_xs,
    [ xslapply/2, value_of/1, select/2, built_in_rules/2 ]).

-include("riak.hrl").

%%
%% Returns information from metadata and headers
%%
%% ObjectRenameMap -- [{some_object_name: new  name}]
%%
prepare_object_record(Record0, ObjectRenameMap, DeletedObjects, AccessTokens) ->
    Metadata = proplists:get_value(metadata, Record0),
    ObjectKey = filename:basename(proplists:get_value(key, Record0)),
    OrigName0 =
	case proplists:get_value("x-amz-meta-orig-filename", Metadata, ObjectKey) of
	    undefined -> ObjectKey;
	    N -> N
	end,
    ModifiedTime =
	case proplists:get_value("x-amz-meta-modified-utc", Metadata) of
	    undefined -> "";
	    T0 -> utils:to_integer(T0)
	end,
    UploadTimestamp0 = proplists:get_value(upload_timestamp, Record0, ""),
    UploadTimestamp1 = calendar:datetime_to_gregorian_seconds(UploadTimestamp0) - 62167219200,
    Bytes =
	case proplists:get_value(content_length, Metadata, undefined) of
	    undefined -> 0;
	    Value -> utils:to_integer(Value)
	end,
    ContentType = proplists:get_value(content_type, Metadata, 0),
    Etag = proplists:get_value(etag, Metadata, ""),
    Md5 = string:strip(Etag, both, $"),

    %% Object meta tag orig-filename can't be renamed,
    %% But I can store a new name in index object.
    OrigName1 =
	case proplists:get_value(ObjectKey, ObjectRenameMap) of
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
	case proplists:get_value(erlang:list_to_binary(ObjectKey), DeletedObjects) of
	    undefined -> false;
	    _ -> true
	end,
    Token =
	case proplists:get_value(ObjectKey, AccessTokens) of
	    undefined -> undefined;
	    T1 -> unicode:characters_to_binary(T1)
	end,
    [{object_name, erlang:list_to_binary(ObjectKey)},
     {orig_name, unicode:characters_to_binary(OrigName1)},
     {bytes, Bytes},
     {content_type, unicode:characters_to_binary(ContentType)},
     {upload_time, UploadTimestamp1},
     {last_modified_utc, ModifiedTime},
     {is_deleted, IsDeleted},
     {md5, unicode:characters_to_binary(Md5)},
     {access_token, Token}].

%%
%% Queries all pages of objects from Riak CS
%%
-spec get_full_list(string(), string()|undefined) -> proplist().

get_full_list(BucketId, Prefix)
	when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    get_full_list(BucketId, Prefix, [], undefined).

get_full_list(BucketId, Prefix, ObjectList0, Marker0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    RiakResponse =
	case Prefix of
	    undefined ->
		riak_api:list_objects(BucketId, [{marker, Marker0}]);
	    _ ->
		riak_api:list_objects(BucketId, [{prefix, Prefix}, {marker, Marker0}])
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
	    ObjectList1 = [
		[{
		    metadata, riak_api:get_object_metadata(BucketId, proplists:get_value(key, R))
		},
		{
		    upload_timestamp, proplists:get_value(last_modified, R)
		},
		{
		    key, proplists:get_value(key, R)
		}] || R <- Contents,
		utils:is_hidden_object(R) =/= true
	    ],
	    case Marker1 of
		undefined -> [{dirs, PrefixList1}, {list, ObjectList0 ++ ObjectList1}];
		[] -> [{dirs, PrefixList1}, {list, ObjectList0 ++ ObjectList1}];
		NextMarker ->
		    NextPart = get_full_list(BucketId, Prefix, ObjectList0 ++ ObjectList1, NextMarker),
		    [{dirs, PrefixList1 ++ proplists:get_value(dirs, NextPart, [])},
		     {list, ObjectList0 ++ proplists:get_value(list, NextPart, [])}]
	    end
    end.

%%
%% Returns aggregated list of objects with metadata,
%% as well as list of deleted and renamed objects.
%%
-spec get_detailed_list(string()|undefined, proplist(), proplist(), proplist(), proplist()) -> proplist().

get_detailed_list(Prefix, List0, ObjectRenameMap0, DeletedObjects0, AccessTokens0)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined,
	     erlang:is_list(List0), erlang:is_list(ObjectRenameMap0),
	     erlang:is_list(DeletedObjects0), erlang:is_list(AccessTokens0) ->
    AllKeys = [filename:basename(proplists:get_value(key, R)) || R <- proplists:get_value(list, List0)],
    PrefixList0 = proplists:get_value(dirs, List0),

    % Exclude non-existent objects from rename map
    ObjectRenameMap1 =
	case length(ObjectRenameMap0) of
	    0 -> [];
	    _ -> [{K, proplists:get_value(K, ObjectRenameMap0)}
		  || K <- proplists:get_keys(ObjectRenameMap0),
		  lists:member(K, AllKeys)]
	end,
    % Exclude non-existent objects from deleted list
    DeletedObjects1 =
	case length(DeletedObjects0) of
	    0 -> [];
	    _ -> [{K, proplists:get_value(K, DeletedObjects0)}
		  || K <- proplists:get_keys(DeletedObjects0),
			lists:member(erlang:binary_to_list(K), AllKeys) andalso
			(utils:ends_with(K, <<"/">>) =:= false andalso
			utils:ends_with(K, erlang:list_to_binary(?RIAK_ACTION_LOG_FILENAME)) =:= false andalso
			utils:ends_with(K, erlang:list_to_binary(?RIAK_LOCK_INDEX_FILENAME)) =:= false)
		 ] ++ [{K, proplists:get_value(K, DeletedObjects0)}
			|| K <- proplists:get_keys(DeletedObjects0),
			lists:member(utils:prefixed_object_name(Prefix, erlang:binary_to_list(K)), PrefixList0)]
	end,
    %% Add access tokens to those objects that do not have them assigned yet.
    %% Those tokens are stored in index file, as they can be changed by user.
    %% Also exclude non-existent objects.
    AccessTokens1 = AccessTokens0 ++ [
	{K, riak_crypto:random_string()} || K <- AllKeys,
	proplists:is_defined(K, AccessTokens0) =:= false andalso
	utils:is_hidden_object([{key, K}]) =:= false],
    [{dirs, [[
	    {prefix, unicode:characters_to_binary(I)},
	    {bytes, 0},
	    {is_deleted, proplists:is_defined(erlang:list_to_binary(filename:basename(I)++"/"), DeletedObjects1)}
	] || I <- proplists:get_value(dirs, List0)]},
     {list, [prepare_object_record(I, ObjectRenameMap1, DeletedObjects1, AccessTokens1)
		|| I <- proplists:get_value(list, List0)]},
     {renamed, ObjectRenameMap1},
     {to_delete, DeletedObjects1},
     {access_tokens, AccessTokens1}].

%%
%% Returns contents of existing index.
%%
get_index(BucketId, Prefix0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined ->
    PrefixedIndexFilename = utils:prefixed_object_name(Prefix0, ?RIAK_INDEX_FILENAME),
    %% Get index object in destination directory
    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
	not_found -> [{dirs, []}, {list, []}, {renamed, []}, {to_delete, []}, {access_tokens, []}];
	C -> erlang:binary_to_term(proplists:get_value(content, C))
    end.

%%
%% Returns record of object from index.
%%
get_object_record(ObjectKey, IndexContent) when erlang:is_list(ObjectKey) ->
    get_object_record(erlang:list_to_binary(ObjectKey), IndexContent);
get_object_record(ObjectKey, IndexContent) when erlang:is_binary(ObjectKey) ->
    Record = lists:filter(
	fun(R) -> proplists:get_value(object_name, R) =:= ObjectKey end,
	proplists:get_value(list, IndexContent)),
    case length(Record) of
	0 -> [];
	_ -> lists:nth(1, Record)
    end.

%%
%% Creates list of objects for bucket and prefix,
%% checks for renamed and marked as "deleted" ones.
%% Then stores list in ETF format.
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
%%		[{dst_prefix, "d08732/"},
%%		{old_key, "somethind.random"},
%%		{new_key, "something-1.random"},
%%		{orig_name, "Something-1.Random"}]  %% orig_name is optional
%%	]}
%%   ]}
%%
%% - {to_delete, {"blah.png", 1532357691}}
%%
%% - {undelete, ["blah.png"]}
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

    PrefixedLockFilename = utils:prefixed_object_name(Prefix0, ?RIAK_LOCK_INDEX_FILENAME),
    case riak_api:head_object(BucketId, PrefixedLockFilename) of
	not_found ->
	    %% Create lock file instantly
	    riak_api:put_object(BucketId, Prefix0, ?RIAK_LOCK_INDEX_FILENAME, <<>>, RiakOptions),

	    %% Retrieve existing index object first
	    List0 = get_index(BucketId, Prefix0),
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
	    %% Add renamed/deleted object maps from source index.
	    %% Used in COPY and MOVE.
	    [ObjectRenameMap3, DeletedObjects3] =
		case proplists:get_value(copy_from, Options) of
		    undefined -> [[],[]];
		    CopyFrom ->
			SrcBucketId = proplists:get_value(bucket_id, CopyFrom),
			SrcPrefix = proplists:get_value(prefix, CopyFrom),
			CopiedNames1 = proplists:get_value(copied_names, CopyFrom),
			PrefixedSrcIndexFilename = utils:prefixed_object_name(SrcPrefix, ?RIAK_INDEX_FILENAME),
			case riak_api:get_object(SrcBucketId, PrefixedSrcIndexFilename) of
			    not_found -> [[],[]];
			    Content ->
				SrcList = erlang:binary_to_term(proplists:get_value(content, Content)),
				%% Add new renamed AND copied objects to rename map.
				%% We avoid overwriting objects on desktop client this way
				ObjectRenameMap4 = proplists:get_value(renamed, SrcList, []),
				%% Go through copied object _keys_ and find renamed ones
				%% Add copied-renamed objects to renamed map
				ObjectRenameMap5 = [{proplists:get_value(new_key, P), [
				    {name, proplists:get_value(dst_orig_name, P)},
				    {bytes, proplists:get_value(bytes, P)}]}
				 || P <- CopiedNames1, proplists:get_value(renamed, P) =:= true],
				ObjectRenameMap6 = lists:filter(
				    fun({K, _}) -> proplists:is_defined(K, ObjectRenameMap5) =:= false end,
				    ObjectRenameMap4),
				[ObjectRenameMap5 ++ ObjectRenameMap6,  proplists:get_value(to_delete, SrcList, [])]
			end
		end,
	    List1 = get_full_list(BucketId, Prefix0),
	    List2 = get_detailed_list(Prefix0, List1,
		ObjectRenameMap2 ++ ObjectRenameMap3,
		DeletedObjects2 ++ DeletedObjects3,
		proplists:get_value(access_tokens, List0, [])),
	    riak_api:put_object(BucketId, Prefix0, ?RIAK_INDEX_FILENAME, term_to_binary(List2), RiakOptions),
	    %% Remove lock
	    riak_api:delete_object(BucketId, PrefixedLockFilename),
	    proplists:get_value(list, List2);
	_ -> []
    end.
