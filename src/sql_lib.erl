%%
%% Functions for generating SQL statements for SQLite3
%%
-module(sql_lib).

-export([create_table_if_not_exist/1, create_pseudo_directory/2, add_object/2,
	 lock_object/3, delete_object/2, get_object/2, 
	 get_pseudo_directory/2, delete_pseudo_directory/2,
	 rename_object/5, rename_pseudo_directory/4]).

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
		 {prefix, text, [{default, ""}]},
		 {key, text, [not_null]},  %% object key in URL
		 {orig_name, text, [not_null]},  %% UTF-8 filename
		 {is_dir, boolean, not_null},  %% flag indicating whether record is directory
		 {is_locked, boolean, not_null},
		 {bytes, integer},
		 {guid, text, [{default, ""}]},  %% unique identifier on filesystem ( dirs do not have GUID )
		 {version, text, [{default, ""}]},  %% DVV
		 {last_modified_utc, integer},  %% timestamp
		 {author_id, text, [{default, ""}]},
		 {author_name, text, [{default, ""}]},
		 {author_tel, text, [{default, ""}]},
		 {lock_user_id, text, [{default, ""}]},
		 {lock_user_name, text, [{default, ""}]},
		 {lock_user_tel, text, [{default, ""}]},
		 {lock_modified_utc, integer},
		 {md5, text, [{default, ""}]}],
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
	    {is_locked, false},
	    {bytes, 0},
	    {guid, ""},
	    {version, ""},
	    {last_modified_utc, Timestamp},
	    {author_id, ""},
	    {author_name, ""},
	    {author_tel, ""},
	    {md5, ""}],
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
-spec(get_pseudo_directory(Prefix0 :: string() | undefined, OrigName :: binary()) -> ok | {error, any()}).
get_pseudo_directory(Prefix0, OrigName)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
	    andalso erlang:is_binary(OrigName) ->
    SQL = ["SELECT key FROM items WHERE is_dir = ", sqlite3_lib:value_to_sql(true),
	    " AND orig_name = ", sqlite3_lib:value_to_sql(OrigName)],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].


-spec(delete_pseudo_directory(Prefix0 :: string(), Name :: binary()) -> ok | {error, any()}).
delete_pseudo_directory(Prefix0, Name)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
	    andalso erlang:is_binary(Name) ->
    SQL = ["DELETE FROM items WHERE (orig_name = ", sqlite3_lib:value_to_sql(Name),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(true), ")"
	    " OR prefix LIKE \"", erlang:list_to_binary(lists:flatten([utils:hex(Name), "%"])), "\""],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].


-spec(rename_pseudo_directory(BucketId :: string(), Prefix0 :: string(), SrcKey :: string(),
		    DstName :: binary()) -> ok | {error, any()}).
rename_pseudo_directory(BucketId, Prefix0, SrcKey, DstName)
    when erlang:is_list(BucketId) andalso erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
	andalso erlang:is_list(SrcKey) andalso erlang:is_binary(DstName) ->
    SQL = ["UPDATE items SET orig_name = ", sqlite3_lib:value_to_sql(DstName),
	   " WHERE key = ", sqlite3_lib:value_to_sql(SrcKey)],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].


-spec(add_object(Prefix0 :: string() | undefined,
		 Obj :: #object{}) -> ok | {error, any()}).
add_object(Prefix0, Obj)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined ->
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    Data = [{prefix, Prefix1},
	    {key, Obj#object.key},
	    {orig_name, Obj#object.orig_name},
	    {is_dir, false},
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
-spec(get_object(Prefix0 :: string() | undefined, OrigName :: binary()) -> ok | {error, any()}).
get_object(Prefix0, OrigName)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
	    andalso erlang:is_binary(OrigName) ->
    SQL = ["SELECT key FROM items WHERE is_dir = ", sqlite3_lib:value_to_sql(false),
	    " AND orig_name = ", sqlite3_lib:value_to_sql(OrigName)],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].


-spec(rename_object(BucketId :: string(), Prefix0 :: string(), SrcKey :: string(),
		    DstKey :: string(), DstName :: binary()) -> ok | {error, any()}).
rename_object(BucketId, Prefix0, SrcKey, DstKey, DstName)
    when erlang:is_list(BucketId) andalso erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
	andalso erlang:is_list(SrcKey) andalso erlang:is_list(DstKey) andalso erlang:is_binary(DstName) ->
    SQL = ["UPDATE items SET key = ", sqlite3_lib:value_to_sql(DstKey),
	   ", orig_name = ", sqlite3_lib:value_to_sql(DstName),
	   " WHERE key = ", sqlite3_lib:value_to_sql(SrcKey)],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].


-spec(lock_object(Prefix0 :: string() | undefined,
	Key :: string(), Value :: boolean()) -> ok | {error, any()}).
lock_object(Prefix0, Key, Value) ->
    SQL = ["UPDATE items SET is_locked = ", sqlite3_lib:value_to_sql(Value),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(false)],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].


-spec(delete_object(Prefix0 :: string() | undefined, Key :: string()) -> ok | {error, any()}).
delete_object(Prefix0, Key)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined andalso erlang:is_list(Key) ->
    SQL = ["DELETE FROM items WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(false)],
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    SQL ++ [" AND prefix = ",  sqlite3_lib:value_to_sql(Prefix1), ";"].
