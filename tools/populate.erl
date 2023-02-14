%%
%% This script is used for re-indexing contents of Riak CS,
%% in case it is corrupted for some reason.
%%
%% Reads index files ( ".riak_index.etf" ), stored in Riak CS and
%% populates SQLite DB which desktop clients use for synchronisation.
%%
-module(populate).

-export([main/0]).

-include("../include/entities.hrl").
-include("../include/riak.hrl").


%%
%% Find parent directory id for adding new dir
%%
get_pseudo_directory(_DbName, _Prefix, undefined) -> undefined;
get_pseudo_directory(DbName, Prefix, Name) ->
    SQL = sql_lib:get_pseudo_directory(Prefix, Name),
    case sqlite3:sql_exec(DbName, SQL) of
	[{columns, _}, {rows,[OrigName]}] -> OrigName;
	[{columns, _}, {rows,[]}] -> undefined;
	Error ->
	    io:fwrite("Error querying parent directory id ~p: ~p~n", [Name, Error]),
	    throw("Error querying directory id")
    end.

%%
%% Add pseudo-directories first
%%
create_pseudo_directories(_DbName, undefined) -> ok;  %% root dir
create_pseudo_directories(DbName, Prefix) ->
    DirName = utils:unhex(erlang:list_to_binary(filename:basename(Prefix))),
    ParentPrefix = utils:dirname(Prefix),
    io:fwrite("Checking if dir exists: ~p / ~p~n", [ParentPrefix, DirName]),
    case get_pseudo_directory(DbName, ParentPrefix, DirName) of
	undefined ->
	    %% Directory do not exist, create one
	    case sql_lib:create_pseudo_directory(ParentPrefix, DirName) of
		{error, Reason0} ->
		    io:fwrite("Error composing SQL for creating directory ~p: ~p~n", [Prefix, Reason0]),
		    throw("Error composing SQL");
		SQL0 ->
		    case sqlite3:sql_exec(DbName, SQL0) of
			{rowid, _} ->
			    io:fwrite("Created directory ~p~n", [Prefix]),
			    create_pseudo_directories(DbName, ParentPrefix);
			Error1 ->
			    io:fwrite("Error creating directory ~p: ~p~n", [Prefix, Error1]),
			    throw("Error executing SQL")
		    end
	    end;
	_ -> ok
    end.


sql_exec(DbName, Prefix0, Obj0) ->
    Key = proplists:get_value(object_key, Obj0),
io:fwrite("Key: ~p~n", [Key]),
    OrigName = proplists:get_value(orig_name, Obj0),
    UserId = proplists:get_value(author_id, Obj0),
    UserName = proplists:get_value(author_name, Obj0),
    UserTel = proplists:get_value(author_tel, Obj0),
    Timestamp =
	case proplists:get_value(lock_modified_utc, Obj0) of
	    null -> erlang:round(utils:timestamp()/1000);
	    T -> T
	end,
    Obj1 = #object{
	key = Key,
	orig_name = OrigName,
	bytes = proplists:get_value(bytes, Obj0),
	guid = proplists:get_value(guid, Obj0),
	version = proplists:get_value(version, Obj0),
	upload_time = proplists:get_value(upload_time, Obj0),
	is_deleted = proplists:get_value(is_deleted, Obj0),
	author_id = UserId,
	author_name = UserName,
	author_tel = UserTel,
	is_locked = proplists:get_value(is_locked, Obj0),
	lock_user_id = proplists:get_value(lock_user_id, Obj0),
	lock_user_name = proplists:get_value(lock_user_name, Obj0),
	lock_user_tel = proplists:get_value(lock_user_tel, Obj0),
	lock_modified_utc = Timestamp
    },
    SQL0 = sql_lib:get_object(Prefix0, OrigName),
    io:fwrite("Checking if object exists ~p / ~p", [Prefix0, Key]),
    case sqlite3:sql_exec(DbName, SQL0) of
	[{columns, _}, {rows,[_|_]}] -> ok;
	[{columns, _}, {rows,[]}] ->
	    io:fwrite("Adding object ~p / ~p", [Prefix0, Key]),
	    case sql_lib:add_object(Prefix0, Obj1) of
		{error, Error3} ->
		    io:fwrite("Error composing SQL for add object operation ~p/~p: ~p",
			[Prefix0, Obj1#object.key, Error3]);
		SQL1 ->
		    case sqlite3:sql_exec(DbName, SQL1) of
			{rowid, _} ->
			    io:fwrite("Added: ~p/~p~n", [Prefix0, Obj1#object.key]);
			Error4 ->
			    io:fwrite("Error adding object ~p/~p: ~p~n", [Prefix0, Obj1#object.key, Error4]),
			    throw("Error adding object")
		    end
	    end;
	Error ->
	    io:fwrite("Error getting object: ~p/~p: ~p~n", [Prefix0, Obj1#object.key, Error]),
	    throw("Error getting object")
    end.

%%
%% Updates SQLite db file in Riak CS. Makes sure it is not locked first.
%%
%% ``BucketId`` -- bucket in Riak CS to update SQLite db in
%% ``DbFn`` -- filename on local filesystem to read from
%%
update_db(BucketId, UserId, DbFn) ->
    %% Check lock file first
    Timestamp = erlang:round(utils:timestamp()/1000),
    case riak_api:head_object(BucketId, ?DB_VERSION_LOCK_FILENAME) of
	not_found ->
	    %% Create lock file instantly
	    LockMeta = [{"modified-utc", Timestamp}],
	    LockOptions = [{acl, public_read}, {meta, LockMeta}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_LOCK_FILENAME, <<>>, LockOptions),

	    upload_db(BucketId, UserId, DbFn, Timestamp),

	    riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME),  %% Remove lock object
	    %file:delete(DbFn),
	    ok;
	IndexLockMeta ->
	    %% Check for stale index
	    DeltaSeconds =
		case proplists:get_value("x-amz-meta-modified-utc", IndexLockMeta) of
		    undefined -> 0;
		    T -> Timestamp - utils:to_integer(T)
		end,
	    case DeltaSeconds > ?DB_VERSION_LOCK_COOLOFF_TIME of
		true ->
		    riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME),  %% Remove lock object
		    upload_db(BucketId, UserId, DbFn, Timestamp),
		    ok;
		false ->
		    io:fwrite("DB object is locked and lock has not expired yet"),
		    throw("DB object locked")
	    end
    end.

upload_db(BucketId, UserId, DbFn, Timestamp) ->
    Version0 =
	case riak_api:head_object(BucketId, ?DB_VERSION_KEY) of
	    not_found ->
		base64:encode(jsx:encode(indexing:increment_version(undefined, Timestamp, UserId)));
	    Metadata ->
		case proplists:get_value("x-amz-meta-version", Metadata) of
		    undefined ->
			base64:encode(jsx:encode(indexing:increment_version(undefined, Timestamp, UserId)));
		    V -> V
		end
	end,
    {ok, Blob} = file:read_file(DbFn),
    RiakOptions = [{acl, public_read}, {meta, [
	{"version", Version0},
	{"user_id", UserId},
	{"bytes", byte_size(Blob)}]}],
    riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, RiakOptions).


main() ->
    inets:start(),
    {ok, BucketArg} = init:get_argument(bucket),
    BucketId = lists:flatten(BucketArg),
    case BucketId of
	undefined -> throw("BucketId is expected");
	[] -> throw("BucketId is expected");
	_ -> ok
    end,

    {ok, UserIdArg} = init:get_argument(userid),
    UserId = lists:flatten(UserIdArg),
    case UserId of
	undefined -> throw("BucketId is expected");
	[] -> throw("BucketId is expected");
	_ -> ok
    end,

    io:fwrite("Indexing bucket ~p", [BucketId]),

    %BucketId = "the-integrationtests-integration1-res",
    List0 = riak_api:recursively_list_pseudo_dir(BucketId, undefined),
    IndexName1 = erlang:list_to_binary(?RIAK_INDEX_FILENAME),

    Cmd = lists:flatten(io_lib:format("/bin/mktemp --suffix=~s.db", [BucketId])),
    TempFn0 = os:cmd(Cmd),
    TempFn1 = re:replace(TempFn0, "[\r\n]", "", [global, {return, list}]),

    DbName = db_files,
    {ok, Pid} = sqlite3:open(DbName, [{file, TempFn1}]),
    sql_lib:create_table_if_not_exist(DbName),

    lists:foreach(
	fun(I) ->
	    case utils:ends_with(erlang:list_to_binary(I), IndexName1) of
		true ->
		    %% Add pseudo-directories first
		    create_pseudo_directories(DbName, utils:dirname(I)),
		    case riak_api:get_object(BucketId, I) of
			{error, _Reason} -> ok;
			not_found -> ok;
			IndexObject ->
			    Index = erlang:binary_to_term(proplists:get_value(content, IndexObject)),
			    List1 = proplists:get_value(list, Index),
			    lists:foreach(
				fun(J) ->
				    sql_exec(DbName, utils:dirname(I), J)
				end, List1)
		    end;
		false -> ok
	    end
	end, List0),
    sqlite3:close(Pid),

    update_db(BucketId, UserId, TempFn1).
