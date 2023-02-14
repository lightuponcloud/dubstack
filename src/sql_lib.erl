%%
%% Functions for generating SQL statements for SQLite3
%%
-module(sql_lib).

-export([create_table_if_not_exist/1, create_pseudo_directory/2, add_object/2,
	 delete_pseudo_directory/3, lock_object/3, delete_object/3, get_object/2,
	 get_pseudo_directory/2]).

-include("entities.hrl").


%%
%% Creates table if not exist.
%%
create_table_if_not_exist(DbName) ->
    %% Check if table exists
    Result = sqlite3:read(DbName, sqlite_master, {name, "items"}),
    case proplists:get_value(rows, Result) of
	[] ->
	    TableInfo = [{id, integer, [{primary_key, [asc, autoincrement]}]},
		 {prefix, text},
		 {key, text, [not_null]},  %% object key in URL
		 {orig_name, text, [not_null]},  %% UTF-8 filename
		 {is_dir, boolean, not_null},  %% flag indicating whether record is directory
		 {is_deleted, boolean, not_null},
		 {is_locked, boolean, not_null},
		 {bytes, integer},
		 {guid, text},  %% unique identifier on filesystem ( dirs do not have GUID )
		 {version, text},  %% DVV
		 {last_modified_utc, integer},  %% timestamp
		 {author_id, text},
		 {author_name, text},
		 {author_tel, text},
		 {lock_user_id, text},
		 {lock_user_name, text},
		 {lock_user_tel, text},
		 {lock_modified_utc, integer},
		 {md5, text}],
	    case sqlite3:create_table(DbName, items, TableInfo) of
		ok -> ok;
		{error, _, Reason} ->
		    lager:error("[sql_lib] error creating table: ~p", [Reason])
	    end;
	_ -> ok  %% table exists
    end.


-spec(create_pseudo_directory(Prefix :: string() | undefined, Name :: binary()) -> ok | {error, any()}).
create_pseudo_directory(Prefix, Name)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined andalso erlang:is_binary(Name) ->
    Key = utils:hex(Name),
    Timestamp = erlang:round(utils:timestamp()/1000),
    Data = [{prefix, Prefix},
	    {key, Key},
	    {orig_name, unicode:characters_to_list(Name)},
	    {is_dir, true},
	    {is_deleted, false},
	    {is_locked, false},
	    {bytes, 0},
	    {guid, null},
	    {version, null},
	    {last_modified_utc, Timestamp},
	    {author_id, null},
	    {author_name, null},
	    {author_tel, null},
	    {md5, null}],
    try sqlite3_lib:write_sql(items, Data) of
        SQL -> SQL
    catch
        _:Exception -> {error, Exception}
    end.

%%
%% Returns SQL for querying pseudo-directory by its prefix and name.
%%
%% Prefix0 -- prefix of directory name we are looking for
%% Name -- the name of directory, hex-encoded
%%
-spec(get_pseudo_directory(Prefix :: string() | undefined, OrigName :: binary()) -> ok | {error, any()}).
get_pseudo_directory(Prefix, OrigName)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_binary(OrigName) ->
    SQL0 = ["SELECT key FROM items WHERE is_dir = ", sqlite3_lib:value_to_sql(true),
	    " AND orig_name = ", sqlite3_lib:value_to_sql(OrigName)],
    case Prefix of
	undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	_ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
    end.


-spec(add_object(Prefix :: string() | undefined,
		 Obj :: #object{}) -> ok | {error, any()}).
add_object(Prefix, Obj)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    Data = [{prefix, Prefix},
	    {key, Obj#object.key},
	    {orig_name, Obj#object.orig_name},
	    {is_dir, false},
	    {is_deleted, Obj#object.is_deleted},
	    {is_locked, Obj#object.is_locked},
	    {bytes, Obj#object.bytes},
	    {guid, Obj#object.guid},
	    {version, Obj#object.version},
	    {last_modified_utc, Obj#object.upload_time},
	    {author_id, Obj#object.author_id},
	    {author_name, Obj#object.author_name},
	    {author_tel, Obj#object.author_tel},
	    {lock_user_id, Obj#object.lock_user_id},
	    {lock_user_name, Obj#object.lock_user_name},
	    {lock_user_tel, Obj#object.lock_user_tel},
	    {lock_modified_utc, Obj#object.lock_modified_utc},
	    {md5, Obj#object.md5}],
    try sqlite3_lib:write_sql(items, Data) of
        SQL -> SQL
    catch
        _:Exception -> {error, Exception}
    end.

%%
%% Get object key if exists
%%
-spec(get_object(Prefix :: string() | undefined, OrigName :: binary()) -> ok | {error, any()}).
get_object(Prefix, OrigName)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_binary(OrigName) ->
    SQL0 = ["SELECT key FROM items WHERE is_dir = ", sqlite3_lib:value_to_sql(false),
	    " AND orig_name = ", sqlite3_lib:value_to_sql(OrigName)],
    case Prefix of
	undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	_ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
    end.


-spec(delete_pseudo_directory(Prefix :: string() | undefined,
	Key :: string(), UserId :: string()) -> ok | {error, any()}).
delete_pseudo_directory(Prefix, Key, UserId)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Key) andalso erlang:is_list(UserId) ->
    SQL0 = ["UPDATE items SET is_deleted = ", sqlite3_lib:value_to_sql(true),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(true)],
    case Prefix of
	undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	_ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
    end.


-spec(lock_object(Prefix :: string() | undefined,
	Key :: string(), Value :: boolean()) -> ok | {error, any()}).
lock_object(Prefix, Key, Value) ->
    SQL0 = ["UPDATE items SET is_locked = ", sqlite3_lib:value_to_sql(Value),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(false)],
    case Prefix of
	undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	_ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
    end.


-spec(delete_object(Prefix :: string() | undefined,
	Key :: string(), UserId :: string() ) -> ok | {error, any()}).
delete_object(Prefix, Key, UserId)
	when erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Key) andalso erlang:is_list(UserId) ->
    SQL0 = ["UPDATE items SET is_deleted = ", sqlite3_lib:value_to_sql(true),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(false)],
    case Prefix of
	undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	_ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
    end.
