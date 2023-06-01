%%
%% Functions for generating SQL statements for SQLite3
%%
-module(sql_lib).

-export([create_table_if_not_exist/1, create_pseudo_directory/3, add_object/2,
	 lock_object/3, delete_object/2, get_object/2, 
	 get_pseudo_directory/2, delete_pseudo_directory/2,
	 rename_object/4, rename_pseudo_directory/3]).

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


-spec(create_pseudo_directory(Prefix0 :: string() | undefined, Name :: binary(),
			      User :: #user{}) -> ok | list()).
create_pseudo_directory(Prefix0, Name, User)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined andalso erlang:is_binary(Name) ->
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    Key = utils:hex(Name),
    Timestamp = erlang:round(utils:timestamp()/1000),
    ["INSERT OR REPLACE INTO items (id, prefix, key, orig_name, is_dir, is_locked, ",
     "bytes, last_modified_utc, author_id, author_name, author_tel) ",
     "VALUES ((SELECT id FROM items WHERE prefix = ", sqlite3_lib:value_to_sql(Prefix1),
     " AND key = ", sqlite3_lib:value_to_sql(Key), "), ",
     sqlite3_lib:value_to_sql(Prefix1), ", ", sqlite3_lib:value_to_sql(Key), ", ", 
     sqlite3_lib:value_to_sql(Name), ", ", sqlite3_lib:value_to_sql(true), ", ",
     sqlite3_lib:value_to_sql(false), ", ", sqlite3_lib:value_to_sql(0), ", ",
     sqlite3_lib:value_to_sql(Timestamp), ", ", sqlite3_lib:value_to_sql(User#user.id),
     ", ", sqlite3_lib:value_to_sql(User#user.name), ", ",
     sqlite3_lib:value_to_sql(User#user.tel), ");"].

%%
%% Returns SQL for querying pseudo-directory by its prefix and name.
%%
%% Prefix0 -- prefix of directory name we are looking for
%% Name -- the name of directory, hex-encoded
%%
-spec(get_pseudo_directory(Prefix0 :: string() | undefined, OrigName :: binary()) -> ok | list()).
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


-spec(delete_pseudo_directory(Prefix0 :: string(), Name :: binary()) -> ok | list()).
delete_pseudo_directory(Prefix0, Name)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
	    andalso erlang:is_binary(Name) ->
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    ["DELETE FROM items WHERE (orig_name = \"", Name, "\" AND is_dir = ",
     sqlite3_lib:value_to_sql(true), 
     " AND prefix = ", sqlite3_lib:value_to_sql(Prefix1), ") OR prefix LIKE \"",
     erlang:list_to_binary(lists:flatten([utils:hex(Name), "%"])), "\"", ";"].


-spec(rename_pseudo_directory(Prefix0 :: string(), SrcKey :: string(), DstName :: binary()) -> ok | list()).
rename_pseudo_directory(Prefix0, SrcKey, DstName)
    when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
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
		 Obj :: #object{}) -> ok | list()).
add_object(Prefix0, Obj)
	when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined ->
    Prefix1 =
	case Prefix0 of
	    undefined -> "";
	    _ -> Prefix0
	end,
    ["INSERT OR REPLACE INTO items (id, prefix, key, orig_name, is_dir, ",
     "is_locked, bytes, guid, version, last_modified_utc, author_id, "
     "author_name, author_tel, lock_user_id, lock_user_name, lock_user_tel, "
     "lock_modified_utc, md5) VALUES ((SELECT id FROM items WHERE prefix = ",
     sqlite3_lib:value_to_sql(Prefix1), " AND key = ", sqlite3_lib:value_to_sql(Obj#object.key), "), ",
     sqlite3_lib:value_to_sql(Prefix1), ", ", sqlite3_lib:value_to_sql(Obj#object.key), ", ",
     sqlite3_lib:value_to_sql(Obj#object.orig_name), ", ", sqlite3_lib:value_to_sql(false), ", ",
     sqlite3_lib:value_to_sql(Obj#object.is_locked), ", ", sqlite3_lib:value_to_sql(Obj#object.bytes), ", ",
     sqlite3_lib:value_to_sql(Obj#object.guid), ", ", sqlite3_lib:value_to_sql(Obj#object.version), ", ",
     sqlite3_lib:value_to_sql(Obj#object.upload_time), ", ", sqlite3_lib:value_to_sql(Obj#object.author_id), ", ",
     sqlite3_lib:value_to_sql(Obj#object.author_name), ", ", sqlite3_lib:value_to_sql(Obj#object.author_tel), ", ",
     sqlite3_lib:value_to_sql(Obj#object.lock_user_id), ", ", sqlite3_lib:value_to_sql(Obj#object.lock_user_name), ", ",
     sqlite3_lib:value_to_sql(Obj#object.lock_user_tel), ", ",
     sqlite3_lib:value_to_sql(Obj#object.lock_modified_utc), ", ",
     sqlite3_lib:value_to_sql(Obj#object.md5), ");"].

%%
%% Get object key if exists
%%
-spec(get_object(Prefix0 :: string() | undefined, OrigName :: binary()) -> ok | list()).
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


-spec(rename_object(Prefix0 :: string(), SrcKey :: string(),
		    DstKey :: string(), DstName :: binary()) -> ok | list()).
rename_object(Prefix0, SrcKey, DstKey, DstName)
    when erlang:is_list(Prefix0) orelse Prefix0 =:= undefined
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
	Key :: string(), Value :: boolean()) -> ok | list()).
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


-spec(delete_object(Prefix0 :: string() | undefined, Key :: string()) -> ok | list()).
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
