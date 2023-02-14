%%
%% This server updates contents of sqlite databases stored in every bucket.
%%
%% The DB contains tree of filesystem on client side.
%% Server and client databases can be compared by client app.
%%
%%
-module(sqlite_server).

-behaviour(gen_server).

%% API
-export([start_link/0, create_pseudo_directory/4, delete_pseudo_directory/4,
         lock_object/5, add_object/9, delete_object/4]).

-export([update_db/4, integrity_check/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("log.hrl").
-include("riak.hrl").

-define(SERVER, ?MODULE).
%% sql_queue -- List of queued SQL statements (those that could not have been executed because of lock )
%% timers -- list of timers, stored in case they need to be cancelled
-record(state, {sql_queue = [], timers = []}).

-spec(create_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      Name :: string(), UserId :: string() ) -> ok | {error, any()}).
create_pseudo_directory(BucketId, Prefix, Name, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Name) andalso erlang:is_list(UserId) ->
    Timestamp = erlang:round(utils:timestamp()/1000),
    case sql_lib:create_pseudo_directory(Prefix, Name, UserId, Timestamp) of
	{error, Reason} ->
	    lager:error("[sqlite_server] Failed composing SQL for ~p/~p ~p ~p ~p: ~p",
			[BucketId, Prefix, Name, UserId, Timestamp, Reason]);
	SQL -> gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL, Timestamp})
    end.


-spec(add_object(BucketId :: string(), Prefix :: string(), Key :: string(), Name :: string(),
		 ByteCnt :: integer(), GUID :: string(), Version :: string(), Timestamp :: integer(),
		 UserId :: string()) ->
    ok | {error, any()}).
add_object(BucketId, Prefix, Key, Name, ByteCnt, GUID, Version, Timestamp, UserId) ->
    Data = [{prefix, Prefix},
	    {key, Key},
	    {name, Name},
	    {is_dir, false},
	    {is_deleted, false},
	    {is_locked, false},
	    {byte_count, ByteCnt},
	    {guid, GUID},
	    {version, Version},
	    {last_modified_utc, Timestamp}],
    try sqlite3_lib:write_sql(items, Data) of
        SQL -> gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL, Timestamp})
    catch
        _:Exception -> {error, Exception}
    end.


-spec(delete_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      Key :: string(), UserId :: string() ) -> ok | {error, any()}).
delete_pseudo_directory(BucketId, Prefix, Key, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Key) andalso erlang:is_list(UserId) ->
    Timestamp = erlang:round(utils:timestamp()/1000),
    SQL0 = ["UPDATE items SET is_deleted = ", sqlite3_lib:value_to_sql(true),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(true)],
    SQL1 =
	case Prefix of
	    undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	    _ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
	end,
    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL1, Timestamp}).


-spec(lock_object(BucketId :: string(), Prefix :: string(), Key :: string(),
		  Value :: boolean(), UserId :: string()) -> ok | {error, any()}).
lock_object(BucketId, Prefix, Key, Value, UserId) ->
    SQL0 = ["UPDATE items SET is_locked = ", sqlite3_lib:value_to_sql(Value),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(false)],
    SQL1 =
	case Prefix of
	    undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	    _ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
	end,
    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL1, undefined}).

-spec(delete_object(BucketId :: string(), Prefix :: string(),
		    Key :: string(), UserId :: string() ) -> ok | {error, any()}).
delete_object(BucketId, Prefix, Key, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Key) andalso erlang:is_list(UserId) ->
    SQL0 = ["UPDATE items SET is_deleted = ", sqlite3_lib:value_to_sql(true),
	    " WHERE key = ", sqlite3_lib:value_to_sql(Key),
	    " AND is_dir = ", sqlite3_lib:value_to_sql(false)],
    SQL1 =
	case Prefix of
	    undefined -> SQL0 ++ [" AND prefix is NULL", ";"];
	    _ -> SQL0 ++ [" AND prefix = ", sqlite3_lib:value_to_sql(Prefix), ";"]
	end,
    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL1, undefined}).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) -> {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages. This message is received by gen_server:cast() call
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({exec_sql, BucketId, UserId, SQL, Timestamp}, State0) ->
    case update_db(BucketId, UserId, SQL, Timestamp) of
	ok -> {noreply, State0};
	{timer, Timer} ->
	    Timers = State0#state.timers,
	    {noreply, State0#state{timers = Timers ++ [[{bucket_id, BucketId}, {timer, Timer}]]}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages. Called by send_after() call.
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({update_db, Details}, State) ->
    BucketId = proplists:get_value(bucket_id, Details),
    UserId = proplists:get_value(user_id, Details),
    SQL = proplists:get_value(sql, Details),
    Timestamp = proplists:get_value(timestamp, Details),
    case update_db(BucketId, UserId, SQL, Timestamp) of
	ok -> {noreply, State};
	{timer, Timer} ->
	    Timers = State#state.timers,
	    {noreply, State#state{timers = Timers ++ [[{bucket_id, BucketId}, {timer, Timer}]] }}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{timers = Timers} = _State) ->
    %% Cancel timers
    lists:all(
	fun(I) ->
	    Timer = proplists:get_value(timer, I),
	    erlang:cancel_timer(Timer)
	end, Timers),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% Checks integrity of SQLite db.
%%
%% Table or index entries that are out of sequence
%% Misformatted records
%% Missing pages
%% Missing or surplus index entries
%% UNIQUE, CHECK, and NOT NULL constraint errors
%% Integrity of the freelist
%% Sections of the database that are used more than once, or not at all
%%
integrity_check(TempFn) ->
    {ok, Pid} = sqlite3:open(integritycheck, [{file, TempFn}]),
    Result = sqlite3:sql_exec(integritycheck, "pragma integrity_check;"),
    IsConsistent =
	case proplists:get_value(rows, Result) of
	    [{<<"ok">>}] -> true;
	    _ -> false
	end,
    sqlite3:close(Pid),
    IsConsistent.

%%
%% Write DB to Riak CS.
%%
exec_sql(BucketId, UserId, TempFn, SQL, Timestamp) ->
    DbName = db_fies,
    case riak_api:head_object(BucketId, ?DB_VERSION_KEY) of
	not_found ->
	    %% No SQLite db found, create a new one
	    {ok, Pid0} = sqlite3:open(DbName, [{file, TempFn}]),
	    case sql_lib:create_table_if_not_exist(DbName) of
		ok ->
		    Version = indexing:increment_version(undefined, Timestamp, UserId),
		    exec_sql(Pid0, DbName, TempFn, BucketId, UserId, Version, SQL, Timestamp);
		{error, Reason} ->
		    ?ERROR("[sqlite_server] error: ~p~n", [Reason]),
		    % lager:error("[sqlite_server] error: ~p~n", [Reason]),
		    ok
	    end;
	Metadata ->
	    %% Read db, then call exec_sql
	    Response = riak_api:get_object(BucketId, ?DB_VERSION_KEY),
	    Content = proplists:get_value(content, Response),
	    file:write_file(TempFn, Content),
	    {ok, Pid1} = sqlite3:open(DbName, [{file, TempFn}]),

	    Version0 = proplists:get_value("x-amz-meta-version", Metadata),
	    Version1 = jsx:decode(base64:decode(Version0)),
	    exec_sql(Pid1, DbName, TempFn, BucketId, UserId, Version1, SQL, Timestamp)
    end.

exec_sql(DbPid, DbName, TempFn, BucketId, UserId, Version0, SQL, Timestamp0) ->
    Version1 =
	case Timestamp0 of
	    undefined ->
		%% no need to change version if timestamp of the change is not specified
		Version0;
	    _ -> indexing:increment_version(Version0, Timestamp0, UserId)
	end,
    case sqlite3:sql_exec(DbName, SQL) of
	{rowid, _} ->
	    sqlite3:close(DbPid),
	    {ok, Blob} = file:read_file(TempFn),
	    RiakOptions = [{acl, public_read}, {meta, [
		{"version", base64:encode(jsx:encode(Version1))},
		{"user_id", UserId},
		{"bytes", byte_size(Blob)}]}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, RiakOptions);
	ok ->
	    sqlite3:close(DbPid),
	    {ok, Blob} = file:read_file(TempFn),
	    RiakOptions = [{acl, public_read}, {meta, [
		{"version", base64:encode(jsx:encode(Version1))},
		{"user_id", UserId},
		{"bytes", byte_size(Blob)}]}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, RiakOptions);
	Error -> ?ERROR("[sqlite_server] SQL error: ~p", [Error])
    end.


%%
%% Updates SQLite db file in Riak CS. Makes sure it is not locked first.
%%
%% BucketId -- bucket where to write db file to
%% UserId -- last change owner id
%% TempFn -- SQLite db file
%%
update_db(BucketId, UserId, SQL, Timestamp0) ->
    %% Check lock file first
    case riak_api:head_object(BucketId, ?DB_VERSION_LOCK_FILENAME) of
	not_found ->
	    %% Create lock file instantly
	    Timestamp1 =
		case Timestamp0 of
		    undefined -> erlang:round(utils:timestamp()/1000);
		    _ -> Timestamp0
		end,
	    LockMeta = [{"modified-utc", Timestamp1}],
	    LockOptions = [{acl, public_read}, {meta, LockMeta}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_LOCK_FILENAME, <<>>, LockOptions),
	    TempFn0 = os:cmd("/bin/mktemp --suffix=.db"),
	    TempFn1 = re:replace(TempFn0, "[\r\n]", "", [global, {return, list}]),
	    exec_sql(BucketId, UserId, TempFn1, SQL, Timestamp0),
	    riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME),  %% Remove lock object
	    file:delete(TempFn1),
	    ok;
	IndexLockMeta ->
	    Timestamp2 =
		case Timestamp0 of
		    undefined -> erlang:round(utils:timestamp()/1000);
		    _ -> Timestamp0
		end,
	    %% Check for stale index
	    DeltaSeconds =
		case proplists:get_value("x-amz-meta-modified-utc", IndexLockMeta) of
		    undefined -> 0;
		    T -> Timestamp2 - utils:to_integer(T)
		end,
	    case DeltaSeconds > ?DB_VERSION_LOCK_COOLOFF_TIME of
		true ->
		    riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME),  %% Remove lock object
		    TempFn0 = os:cmd("/bin/mktemp --suffix=.db"),
		    TempFn1 = re:replace(TempFn0, "[\r\n]", "", [global, {return, list}]),
		    exec_sql(BucketId, UserId, TempFn1, SQL, Timestamp0),
		    file:delete(TempFn1),
		    ok;
		false ->
		    %% Queue request
		    Timer = erlang:send_after(1000, self(), {update_db, [
			{bucket_id, BucketId}, {user_id, UserId}, {sql, SQL}, {timestamp, Timestamp2}]}),
		    {timer, BucketId, Timer}
	    end
    end.
