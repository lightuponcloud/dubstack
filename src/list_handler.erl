%%
%% List handler allows the following requests
%%
%% GET
%%	Returns contetnts of index.etf
%%
%% PATCH
%%	Allows to change state of object:
%%		-- undelete
%%		-- lock / unlock
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
    validate_prefix/2, parse_object_record/2]).

-export([validate_delete/2, get_pseudo_directories/3,
    get_objects/2, format_delete_results/3, delete_resource/2,
    delete_completed/2, delete_pseudo_directory/5, delete_objects/5]).

-include("riak.hrl").
-include("user.hrl").
-include("general.hrl").
-include("action_log.hrl").
-include("log.hrl").

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
	    Prefix1 = prefix_lowercase(Prefix0),
	    PrefixedIndexFilename = utils:prefixed_object_key(Prefix1, ?RIAK_INDEX_FILENAME),
	    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
		not_found -> {jsx:encode([{list, []}, {dirs, []}, {server_utc, UTCTimestamp}]), Req0, []};
		RiakResponse ->
		    List0 = erlang:binary_to_term(proplists:get_value(content, RiakResponse)),
		    {jsx:encode([
			{list, proplists:get_value(list, List0)},
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
    utils:is_authorized(Req0).

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
		false -> js_handler:forbidden(Req0, 37);
		true -> {false, Req0, [{user, User}, {bucket_id, BucketId}]}
	    end;
	false -> js_handler:forbidden(Req0, 7)
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

%%
%% Parse object record from Metadata, replacing values from provided Options
%%
parse_object_record(Metadata, Options) ->
    OrigName =
	case proplists:get_value(orig_name, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-orig-filename", Metadata);
	    V0 -> V0
	end,
    ModifiedTime =
	case proplists:get_value(last_modified_utc, Options, undefined) of
	    undefined -> utils:to_integer(proplists:get_value("x-amz-meta-modified-utc", Metadata));
	    V1 -> V1
	end,
    UploadTime =
	case proplists:get_value(upload_time, Metadata, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-upload-time", Metadata);
	    V2 -> V2
	end,
    TotalBytes =
	case proplists:get_value(bytes, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-bytes", Metadata);
	    V3 -> V3
	end,
    GUID =
	case proplists:get_value(guid, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-guid", Metadata);
	    V4 -> V4
	end,
    IsDeleted =
	case proplists:get_value(is_deleted, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-is-deleted", Metadata);
	    V5 -> V5
	end,
    AuthorId =
	case proplists:get_value(author_id, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-author-id", Metadata);
	    V6 -> V6
	end,
    AuthorName =
	case proplists:get_value(author_name, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-author-name", Metadata);
	    V7 -> V7
	end,
    AuthorTel =
	case proplists:get_value(author_tel, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-author-tel", Metadata);
	    V8 -> V8
	end,
    IsLocked =
	case proplists:get_value(is_locked, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-is-locked", Metadata);
	    V9 -> V9
	end,
    LockUserId =
	case proplists:get_value(user_id, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-lock-user-id", Metadata);
	    Va -> Va
	end,
    LockModifiedTime =
	case proplists:get_value(lock_modified_utc, Options, undefined) of
	    undefined -> proplists:get_value("x-amz-meta-lock-modified-utc", Metadata);
	    Vb -> Vb
	end,
    ContentType =
	case proplists:get_value(content_type, Options, undefined) of
	    undefined -> proplists:get_value(content_type, Metadata);
	    Vc -> Vc
	end,
    Md5 =
	case proplists:get_value(md5, Options, undefined) of
	    undefined ->
		Etag = proplists:get_value(etag, Metadata, ""),
		string:strip(Etag, both, $");
	    Vd -> Vd
	end,
    [
	{orig_name, OrigName},
	{last_modified_utc, ModifiedTime},
	{upload_time, UploadTime},
	{bytes, TotalBytes},
	{guid, GUID},
	{author_id, AuthorId},
	{author_name, AuthorName},
	{author_tel, AuthorTel},
	{is_deleted, IsDeleted},
	{lock_user_id, LockUserId},
	{is_locked, IsLocked},
	{lock_modified_utc, LockModifiedTime},
	{content_type, ContentType},
	{md5, Md5}
    ].

validate_patch(Body, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    case jsx:is_json(Body) of
	{error, badarg} -> {error, 21};
	false -> {error, 21};
	true ->
	    FieldValues = jsx:decode(Body),
	    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
	    case validate_prefix(BucketId, Prefix0) of
		{error, Number} -> {error, Number};
		Prefix1 ->
		    ObjectsList0 = proplists:get_value(<<"objects">>, FieldValues),
		    ObjectsList1 =
			case ObjectsList0 of
			    undefined -> [];
			    _ -> [utils:to_list(N) || N <- ObjectsList0, byte_size(N) < 255]
			end,
		    case proplists:get_value(<<"op">>, FieldValues) of
			undefined -> {error, 41};
			<<"lock">> -> {Prefix1, ObjectsList1, lock};
			<<"unlock">> -> {Prefix1, ObjectsList1, unlock};
			<<"undelete">> -> {Prefix1, ObjectsList1, undelete};
			_ -> {error, 41}
		    end
	    end
    end.

%%
%% PATCH request is used to mark objects as locked/unlocked/not deleted.
%%
patch_resource(Req0, State) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case validate_patch(Body, State) of
	{error, Number} -> js_handler:bad_request(Req1, Number);
	{Prefix, ObjectsList, Operation} ->
	    User = proplists:get_value(user, State),
	    UserId = User#user.id,
	    BucketId = proplists:get_value(bucket_id, State),
	    case Operation of
		lock ->
		    UpdatedOnes0 = [update_lock(UserId, BucketId, Prefix, K, true) || K <- ObjectsList],
		    UpdatedOnes1 = [I || I <- UpdatedOnes0, I =/= not_found andalso I =/= error],
		    Req2 = cowboy_req:set_resp_body(jsx:encode(UpdatedOnes1), Req1),
		    {true, Req2, []};
		unlock ->
		    UpdatedOnes0 = [update_lock(UserId, BucketId, Prefix, K, false) || K <- ObjectsList],
		    UpdatedOnes1 = [I || I <- UpdatedOnes0, I =/= not_found andalso I =/= error],
		    Req2 = cowboy_req:set_resp_body(jsx:encode(UpdatedOnes1), Req1),
		    {true, Req2, []};
		undelete ->
		    ModifiedKeys0 = [undelete(BucketId, Prefix, K) || K <- ObjectsList],
		    ModifiedKeys1 = [proplists:get_value(object_key, I) || I <- ModifiedKeys0, I =/= not_found],
		    case indexing:update(BucketId, Prefix, [{undelete, ModifiedKeys1}]) of
			lock -> js_handler:too_many(Req0);
			_ -> {true, Req1, []}
		    end
	    end
    end.

undelete(BucketId, Prefix, ObjectKey) ->
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey),
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	not_found -> not_found;
	Metadata0 ->
	    LockModifiedTime =  io_lib:format("~p", [utils:timestamp()/1000]),
	    Metadata1 = parse_object_record(Metadata0, [{lock_modified_utc, LockModifiedTime}]),

	    ModifiedTime = utils:to_list(proplists:get_value(last_modified_utc, Metadata1)),
	    LockModifiedTime = proplists:get_value(lock_modified_utc, Metadata1),
	    IsLocked = proplists:get_value(is_locked, Metadata1),
	    Options = [{acl, public_read}, {meta, [{"orig-filename", proplists:get_value(orig_name, Metadata1)},
						   {"modified-utc", ModifiedTime},
						   {"upload-time", proplists:get_value(upload_time, Metadata1)},
						   {"bytes", proplists:get_value(bytes, Metadata1)},
						   {"guid", proplists:get_value(guid, Metadata1)},
						   {"author-id", proplists:get_value(author_id, Metadata1)},
						   {"author-name", proplists:get_value(author_name, Metadata1)},
						   {"author-tel", proplists:get_value(author_tel, Metadata1)},
						   {"lock-user-id", proplists:get_value(lock_user_id, Metadata1)},
						   {"is-locked", IsLocked},
						   {"lock-modified-utc", LockModifiedTime},
						   {"is-deleted", proplists:get_value(is_deleted, Metadata1)}]}],
	    Response = riak_api:put_object(BucketId, Prefix, ObjectKey, <<>>, Options),
	    case Response of
		ok ->
		    [{object_key, ObjectKey},
		     {is_locked, IsLocked},
		     {lock_modified_utc, LockModifiedTime}];
		_ -> error
	    end
    end.


update_lock(UserId0, BucketId, Prefix, ObjectKey, IsLocked0) when erlang:is_boolean(IsLocked0) ->
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey),
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	not_found -> not_found;
	Metadata0 ->
	    LockModifiedTime =  io_lib:format("~p", [utils:timestamp()/1000]),
	    Metadata1 = parse_object_record(Metadata0, [{lock_modified_utc, LockModifiedTime}]),

	    UserId1 = proplists:get_value(lock_user_id, Metadata1),
	    ModifiedTime = proplists:get_value(last_modified_utc, Metadata1),
	    LockModifiedTime = proplists:get_value(lock_modified_utc, Metadata1),
	    IsLocked1 = proplists:get_value(is_locked, Metadata1),
	    IsDeleted = proplists:get_value(is_deleted, Metadata1),
	    Options = [{acl, public_read}, {meta, [{"orig-filename", proplists:get_value(orig_name, Metadata1)},
						   {"modified-utc", utils:to_list(ModifiedTime)},
						   {"upload-time", proplists:get_value(upload_time, Metadata1)},
						   {"bytes", proplists:get_value(bytes, Metadata1)},
						   {"guid", proplists:get_value(guid, Metadata1)},
						   {"author-id", proplists:get_value(author_id, Metadata1)},
						   {"author-name", proplists:get_value(author_name, Metadata1)},
						   {"author-tel", proplists:get_value(author_tel, Metadata1)},
						   {"lock-user-id", UserId0},
						   {"is-locked", erlang:atom_to_list(IsLocked0)},
						   {"lock-modified-utc", LockModifiedTime},
						   {"is-deleted", IsDeleted}]}],
	    case UserId1 =/= undefined andalso UserId0 =/= UserId1 andalso IsDeleted =:= false of
		true ->
		    %% Locked by someone else
		    [{object_key, erlang:list_to_binary(ObjectKey)},
		     {is_locked, IsLocked1},
		     {lock_user_id, erlang:list_to_binary(UserId1)},
		     {lock_modified_utc, erlang:list_to_binary(LockModifiedTime)}];
		false ->
		    Response = riak_api:put_object(BucketId, Prefix, ObjectKey, <<>>, Options),
		    case Response of
			ok ->
			    WasLocked = proplists:get_value("x-amz-meta-is-locked", Metadata0),
			    case WasLocked =/= IsLocked0 of
				true -> ?INFO("Lock state changes from ~p to ~p: ~p/~p",
					      [WasLocked, IsLocked0, BucketId, PrefixedObjectKey]);
				false -> ok
			    end,
			    [{object_key, erlang:list_to_binary(ObjectKey)},
			     {is_locked, IsLocked0},
			     {lock_user_id, erlang:list_to_binary(UserId1)},
			     {lock_modified_utc, erlang:list_to_binary(LockModifiedTime)}];
			_ -> error
		    end
	    end
    end.

%%
%% Receives binary hex prefix value, returns lowercase hex string.
%% Appends "/" at the end if absent.
%%
-spec prefix_lowercase(binary()) -> list()|undefined.

prefix_lowercase(Prefix0) when erlang:is_binary(Prefix0) ->
    case Prefix0 of
       <<>> -> undefined;
       Prefix1 ->
           Prefix2 = string:to_lower(lists:flatten(erlang:binary_to_list(Prefix1))),
           case utils:ends_with(Prefix1, <<"/">>) of
               true -> Prefix2;
               false -> Prefix2++"/"
           end
    end;
prefix_lowercase(undefined) -> undefined.

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
	    Prefix1 = prefix_lowercase(Prefix0),
	    case Prefix1 of
		undefined -> undefined;
		_ ->
		    PrefixedIndexFilename = utils:prefixed_object_key(Prefix1, ?RIAK_INDEX_FILENAME),
		    case riak_api:head_object(BucketId, PrefixedIndexFilename) of
			not_found -> {error, 11};
			_ -> Prefix1
		    end
	    end;
	false -> {error, 36}
    end.

%%
%% Checks if object or pseudo-directory is present in index.
%%
directory_or_object_exists(BucketId, Prefix, DirectoryName) ->
    IndexContent = indexing:get_index(BucketId, Prefix),
    case indexing:pseudo_directory_exists(IndexContent, DirectoryName) of
	true -> {error, 10};
	false ->
	    case indexing:object_exists(IndexContent, DirectoryName) of
		false ->
		    HexDirectoryName1 = utils:hex(DirectoryName),
		    PrefixedDirectoryName1 = utils:prefixed_object_key(Prefix, HexDirectoryName1),
		    {PrefixedDirectoryName1, Prefix, DirectoryName};
		ExistingObjectRecord ->
		    case proplists:get_value(is_deleted, ExistingObjectRecord) of
			true ->
			    HexDirectoryName0 = utils:hex(DirectoryName),
			    PrefixedDirectoryName0 = utils:prefixed_object_key(Prefix, HexDirectoryName0),
			    StaleObjectKey = proplists:get_value(object_key, ExistingObjectRecord),
			    PrefixedStaleObjectKey = utils:prefixed_object_key(
				Prefix, erlang:binary_to_list(StaleObjectKey)),
			    {stale_object, PrefixedStaleObjectKey, PrefixedDirectoryName0, Prefix, DirectoryName};
			false -> {error, 29}
		    end
	    end
    end.

validate_directory_name(_BucketId, _Prefix, null) -> {error, 12};
validate_directory_name(_BucketId, null, _DirectoryName0) -> {error, 36};
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
		    case utils:starts_with(DirectoryName0, erlang:list_to_binary(?RIAK_REAL_OBJECT_PREFIX))
			    andalso Prefix0 =:= undefined of
			true -> {error, 10};
			false -> directory_or_object_exists(BucketId, Prefix1, DirectoryName0)
		    end
	    end
    end.

%%
%% Checks if directory name and prefix are correct
%%
validate_post(Req, Body, BucketId) ->
    case jsx:is_json(Body) of
	{error, badarg} -> {error, 21};
	false -> {error, 21};
	true ->
	    FieldValues = jsx:decode(Body),
	    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
	    DirectoryName0 = proplists:get_value(<<"directory_name">>, FieldValues),
	    case validate_directory_name(BucketId, Prefix0, DirectoryName0) of
		{error, Number} -> js_handler:bad_request(Req, Number);
		Attrs -> Attrs
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
                       timestamp=io_lib:format("~p", [utils:timestamp()/1000])
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
	{error, Number} -> js_handler:bad_request(Req1, Number);
	{PrefixedDirectoryName, Prefix, DirectoryName} ->
	    create_pseudo_directory(Req1, [
		{prefixed_directory_name, PrefixedDirectoryName},
		{prefix, Prefix},
		{directory_name, DirectoryName},
		{user, proplists:get_value(user, State0)},
		{bucket_id, BucketId}]);
	{stale_object, PrefixedStaleObjectKey, PrefixedDirectoryName, Prefix1, DirectoryName1} ->
	    %% Remove stale object and then create directory
	    riak_api:delete_object(BucketId, PrefixedStaleObjectKey),
	    create_pseudo_directory(Req1, [
		{prefixed_directory_name, PrefixedDirectoryName},
		{prefix, Prefix1},
		{directory_name, DirectoryName1},
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
		    ObjectKeys = [I || I <- proplists:get_value(<<"object_keys">>, FieldValues, []), I =/= null],
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
delete_pseudo_directory(BucketId, Prefix, ObjectKey0, ActionLogRecord0, _Timestamp) ->
    ObjectKey1 = erlang:binary_to_list(ObjectKey0),
    DstDirectoryName0 = unicode:characters_to_list(utils:unhex(ObjectKey0)),
    %%
    %% The following operation would allow to undelete files in future.
    %% But is is too expensive.
    %%
    %% case string:str(DstDirectoryName0, "-deleted-") of
    %% 	0 ->
    %% 	    %%     - mark directory as deleted
    %% 	    %%     - mark all nested objects as deleted
    %% 	    %%     - leave record in action log
    %% 	    %% "-deleted-" substring was found in directory name. No need to add another tag.
    %% 	    %% rename_pseudo_directory() marks pseudo-directory as "uncommited".
    %% 	    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey1),
    %% 	    %% DstDirectoryName1 = lists:concat([DstDirectoryName0, "-deleted-", Timestamp]),
    %% 	    %%rename_handler:rename_pseudo_directory(BucketId, Prefix, PrefixedObjectKey,
    %% 	    %%	unicode:characters_to_binary(DstDirectoryName1), ActionLogRecord0);
    %% 	_ ->
    %% 	    case indexing:update(BucketId, Prefix, [{to_delete,
    %% 				    [{ObjectKey0, Timestamp}]}]) of
    %% 		lock -> lock;
    %% 		_ ->
    %% 		    Summary0 = lists:flatten([["Deleted directory \""], DstDirectoryName0 ++ ["/\"."]]),
    %% 		    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    %% 		    action_log:add_record(BucketId, Prefix, ActionLogRecord1),
    %% 		    {dir_name, ObjectKey0}
    %% 	    end
    %% end.

    %% Just delete files
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey1),
    List0 = riak_api:recursively_list_pseudo_dir(BucketId, PrefixedObjectKey),
    [riak_api:delete_object(BucketId, Key) || Key <- List0],
    Summary0 = lists:flatten([["Deleted directory \""], DstDirectoryName0 ++ ["/\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(BucketId, Prefix, ActionLogRecord1),
    {dir_name, ObjectKey0}.


delete_objects(BucketId, Prefix, ObjectKeys0, ActionLogRecord0, Timestamp) ->
    %% Mark object as deleted
    ObjectKeys1 = [{K, Timestamp} || K <- ObjectKeys0],
    case indexing:update(BucketId, Prefix, [{to_delete, ObjectKeys1}]) of
	lock -> lock;
	List0 ->
	    case length(ObjectKeys0) of
		0 -> ok;
		_ ->
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
	    end
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
		timestamp=io_lib:format("~p", [Timestamp/1000])
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
