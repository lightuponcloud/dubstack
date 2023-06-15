%%
%% This server updates contents of sqlite databases stored in every bucket.
%%
%% The DB contains tree of filesystem on client side.
%% Server and client databases can be compared by client app.
%%
-module(sqlite_server).

-behaviour(gen_server).

%% API
-export([start_link/0, create_pseudo_directory/4, task_create_pseudo_directory/4,
	 delete_pseudo_directory/4, lock_object/4, add_object/3, delete_object/3,
	 rename_object/5, rename_pseudo_directory/4]).

-export([integrity_check/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("log.hrl").
-include("riak.hrl").
-include("entities.hrl").

-define(SQLITE_DB_UPDATE_INTERVAL, 1000).  %% 1 second -- db updated every 1 second, if there were changes

%% sql_queue -- List of queued SQL statements (those that could not have been executed because of lock )
%% lock_timers -- list of timers, that are used for retry update of locked DBs
%%                ( stored in case they need to be cancelled )
-record(state, {sql_queue = [], lock_timers = [], update_db_timer = []}).


-spec(create_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      Name :: string(), User :: #user{}) -> ok).
create_pseudo_directory(BucketId, Prefix, Name, User)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_binary(Name) ->
    gen_server:cast(?MODULE, {add_task, BucketId, ?MODULE, task_create_pseudo_directory, [Prefix, Name, User]}).


%%
%% Mark directory as deleted in SQLite db
%%
-spec(delete_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      Name :: binary(), UserId :: string() ) -> ok).
delete_pseudo_directory(BucketId, Prefix, Name, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_binary(Name) andalso erlang:is_list(UserId) ->
    gen_server:cast(?MODULE, {add_task, BucketId, sql_lib, delete_pseudo_directory, [Prefix, Name]}).


-spec(rename_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      SrcKey :: string(), DstName :: binary()) -> ok).
rename_pseudo_directory(BucketId, Prefix, SrcKey, DstName)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(SrcKey) andalso erlang:is_binary(DstName) ->
    gen_server:cast(?MODULE, {add_task, BucketId, sql_lib, rename_pseudo_directory, [Prefix, SrcKey, DstName]}).


-spec(add_object(BucketId :: string(), Prefix :: string(), Obj :: #object{}) -> ok).
add_object(BucketId, Prefix, Obj) when erlang:is_list(BucketId) andalso
	    erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    gen_server:cast(?MODULE, {add_task, BucketId, sql_lib, add_object, [Prefix, Obj]}).


-spec(rename_object(BucketId :: string(), Prefix :: string(), SrcKey :: string(),
		    DstKey :: string(), DstName :: binary()) -> ok).
rename_object(BucketId, Prefix, SrcKey, DstKey, DstName) when erlang:is_list(BucketId) andalso
	    erlang:is_list(Prefix) orelse Prefix =:= undefined andalso erlang:is_list(SrcKey)
	    andalso erlang:is_list(DstKey) andalso erlang:is_binary(DstName) ->
    gen_server:cast(?MODULE, {add_task, BucketId, sql_lib, rename_object,
		    [Prefix, SrcKey, DstKey, DstName]}).


-spec(lock_object(BucketId :: string(), Prefix :: string(), Key :: string(),
		  Value :: boolean()) -> ok).
lock_object(BucketId, Prefix, Key, Value) ->
    gen_server:cast(?MODULE, {add_task, BucketId, sql_lib, lock_object, [Prefix, Key, Value]}).


-spec(delete_object(BucketId :: string(), Prefix :: string(), Key :: string()) -> ok).
delete_object(BucketId, Prefix, Key)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Key) ->
    gen_server:cast(?MODULE, {add_task, BucketId, sql_lib, delete_object, [Prefix, Key]}).


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
init([]) ->
    {ok, Tref0} = timer:send_after(?SQLITE_DB_UPDATE_INTERVAL, update_db),
    {ok, #state{update_db_timer = Tref0}}.


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
handle_cast({add_task, BucketId, Module, Func, Args}, #state{sql_queue = SqlQueue0} = State0) ->
    %% Adds record for periodic task processor
    SqlQueue1 =
	case proplists:is_defined(BucketId, SqlQueue0) of
	    false ->
		%% Add to queue
		BQ0 = [{BucketId, [{Module, Func, Args}]}],
		SqlQueue0 ++ BQ0;
	    true ->
		%% Change bucket queue
		lists:map(
		    fun(I) ->
			case element(1, I) of
			    BucketId ->
				BQ1 = element(2, I),
				{BucketId, BQ1 ++ [{Module, Func, Args}]};
			    _ -> I
			end
		    end, SqlQueue0)
	end,
    {noreply, State0#state{sql_queue = SqlQueue1}}.


%%
%% Adds pseudo-directory in SQLite db, if it do not exist yet
%%
task_create_pseudo_directory(Prefix, Name, User, DbName) ->
    %% Check if pseudo-directory exists first
    SQL0 = sql_lib:get_pseudo_directory(Prefix, Name),
    case sqlite3:sql_exec(DbName, SQL0) of
	[{columns, _}, {rows,[_OrigName]}] ->
	    %% Pseudo-directory is in DB already
	    [];
	[{columns, _}, {rows,[]}] ->
	    %% Create one
	    case sql_lib:create_pseudo_directory(Prefix, Name, User) of
		{error, Reason} ->
		    lager:error("[sqlite_server] Failed composing SQL for ~p ~p: ~p",
				[Prefix, Name, Reason]);
		SQL1 -> SQL1
	    end
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
handle_info(update_db, #state{sql_queue = SqlQueue0} = State) ->
    %% Go over per-bucket queues of tasks, download SQLite db files,
    %% execute SQL statements and upload DBs again
    SqlQueue1 =
	lists:map(
	    fun(I) ->
		BucketId = element(1, I),
		BucketQueue0 = element(2, I),
		BucketQueue1 =
		    case length(BucketQueue0) of
			0 -> [];  %% do nothing, try later
			_ ->
			    %% Acquire lock on db first
			    case lock_db(BucketId) of
				ok -> update_db(BucketId, BucketQueue0);
				locked ->
				    %% Skip it. The next check is in 3 seconds
				    BucketQueue0
			    end
		    end,
		{BucketId, BucketQueue1}
	    end, SqlQueue0),
    {ok, Tref0} = timer:send_after(?SQLITE_DB_UPDATE_INTERVAL, update_db),
    {noreply, State#state{update_db_timer = Tref0, sql_queue = SqlQueue1}};

handle_info(_Info, State) ->
    {noreply, State}.

update_db(BucketId, BucketQueue0) ->
    DbName = erlang:list_to_atom(riak_crypto:random_string()),
    case open_db(BucketId, DbName) of
	{error, _Reason} -> BucketQueue0;  %% leave queue as is
	{ok, TempFn, DbPid, Version0} ->
	    BucketQueue1 = lists:map(
		fun(J) ->
		    Module = element(1, J),
		    Func = element(2, J),
		    Args =
			case {Module, Func} of
			    {?MODULE, task_create_pseudo_directory} -> element(3, J) ++ [DbName];  %% add to args
			    _ -> element(3, J)
			end,
		    case erlang:function_exported(Module, Func, length(Args)) of
			true ->
			    SQL = apply(Module, Func, Args),
			    case SQL of
				[] -> [];
				_ ->
				    case sqlite3:sql_exec(DbName, SQL) of
					{error, Reason} ->
					    ?ERROR("[sqlite_server] SQL error: ~p", [Reason]),
					    {Module, Func, Args};  %% adding it back to queue
					_Result -> []
				    end
			    end;
			false ->
			    ?ERROR("[sqlite_server] not exported: ~p:~p/~p~p",
				   [Module, Func, length(Args), Args]),
			    []  %% removing from queue
		    end
		end, BucketQueue0),
	    %% Read SQLitedb as file and write it to Riak CS
	    sqlite3:close(DbPid),
	    Timestamp = erlang:round(utils:timestamp()/1000),
	    Version1 = indexing:increment_version(Version0, Timestamp, []),
	    {ok, Blob} = file:read_file(TempFn),
	    RiakOptions = [{acl, public_read}, {meta, [
		{"version", base64:encode(jsx:encode(Version1))},
		{"bytes", byte_size(Blob)}]}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, RiakOptions),
	    %% Remove lock object
	    %% remove temporary db file
	    riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME),
	    file:delete(TempFn),
	    lists:flatten(BucketQueue1)
    end.


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
terminate(_Reason, #state{lock_timers = LockTimers} = _State) ->
    %% Cancel lock timers
    lists:all(
	fun(I) ->
	    LockTimer = proplists:get_value(timer, I),
	    erlang:cancel_timer(LockTimer)
	end, LockTimers),
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
integrity_check(DbName, TempFn) ->
    {ok, _Pid} = sqlite3:open(DbName, [{file, TempFn}]),
    Result = sqlite3:sql_exec(DbName, "pragma integrity_check;"),
    IsConsistent =
	case proplists:get_value(rows, Result) of
	    [{<<"ok">>}] -> true;
	    _ -> false
	end,
    sqlite3:close(DbName),
    IsConsistent.

%%
%% Read DB from Riak CS.
%%
open_db(BucketId, DbName) ->
    TempFn0 = os:cmd("/bin/mktemp --suffix=.db"),
    TempFn1 = re:replace(TempFn0, "[\r\n]", "", [global, {return, list}]),
    case riak_api:head_object(BucketId, ?DB_VERSION_KEY) of
	not_found ->
	    %% No SQLite db found, create a new one
	    {ok, Pid0} = sqlite3:open(DbName, [{file, TempFn1}]),
	    case sql_lib:create_table_if_not_exist(DbName) of
		ok ->
		    %% Create a new version
		    Timestamp = erlang:round(utils:timestamp()/1000),
		    Version0 = indexing:increment_version(undefined, Timestamp, []),
		    file:delete(TempFn1),
		    {ok, TempFn1, Pid0, Version0};
		{error, Reason0} ->
		    ?ERROR("[sqlite_server] Failed to create table: ~p~n", [Reason0]),
		    file:delete(TempFn1),
		    {error, Reason0}
	    end;
	Metadata ->
	    %% Read db, then call exec_sql
	    Response = riak_api:get_object(BucketId, ?DB_VERSION_KEY),
	    Content = proplists:get_value(content, Response),
	    file:write_file(TempFn1, Content),
	    {ok, Pid1} = sqlite3:open(DbName, [{file, TempFn1}]),
	    Version1 = proplists:get_value("x-amz-meta-version", Metadata),
	    Version2 = jsx:decode(base64:decode(Version1)),
	    file:delete(TempFn1),
	    {ok, TempFn1, Pid1, Version2}
    end.

%%
%% Create lock object in Riak CS
%%
lock_db(BucketId) ->
    %% Check lock file first
    case riak_api:head_object(BucketId, ?DB_VERSION_LOCK_FILENAME) of
	not_found ->
	    %% Create lock file instantly
	    Timestamp0 = erlang:round(utils:timestamp()/1000),
	    LockMeta0 = [{"modified-utc", Timestamp0}],
	    Result0 = riak_api:put_object(BucketId, undefined, ?DB_VERSION_LOCK_FILENAME, <<>>,
					  [{acl, public_read}, {meta, LockMeta0}]),
	    case Result0 of
		{error, Reason0} -> {error, Reason0};
		_ -> ok
	    end;
	IndexLockMeta ->
	    %% Check for stale index
	    Timestamp1 = erlang:round(utils:timestamp()/1000),
	    DeltaSeconds =
		case proplists:get_value("x-amz-meta-modified-utc", IndexLockMeta) of
		    undefined -> 0;
		    T -> Timestamp1 - utils:to_integer(T)
		end,
	    case DeltaSeconds > ?DB_VERSION_LOCK_COOLOFF_TIME of
		true ->
		    %% Remove previous lock object, create a new one
		    case riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME) of
			{error, Reason} ->
			    lager:error("Failed removing lock ~p/~p", [BucketId, ?DB_VERSION_LOCK_FILENAME]),
			    {error, Reason};
			{ok, _} ->
			    LockMeta1 = [{"modified-utc", Timestamp1}],
			    Result1 = riak_api:put_object(BucketId, undefined, ?DB_VERSION_LOCK_FILENAME, <<>>,
							  [{acl, public_read}, {meta, LockMeta1}]),
			    case Result1 of
				{error, Reason1} -> {error, Reason1};
				_ -> ok
			    end
		    end;
		false -> locked
	    end
    end.
