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
         lock_object/5, add_object/3, delete_object/4]).

-export([integrity_check/1]).

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

-define(SERVER, ?MODULE).
%% sql_queue -- List of queued SQL statements (those that could not have been executed because of lock )
%% timers -- list of timers, stored in case they need to be cancelled
-record(state, {sql_queue = [], timers = []}).

-spec(create_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      Name :: string(), UserId :: string() ) -> ok | {error, any()}).
create_pseudo_directory(BucketId, Prefix, Name, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_binary(Name) andalso erlang:is_list(UserId) ->
    Timestamp = erlang:round(utils:timestamp()/1000),
    gen_server:cast(?MODULE, {create_pseudo_directory, BucketId, Prefix, Name, UserId, Timestamp}).


-spec(add_object(BucketId :: string(), Prefix :: string(), Obj :: #object{}) -> ok | {error, any()}).
add_object(BucketId, Prefix, Obj) when erlang:is_list(BucketId) andalso
	    erlang:is_list(Prefix) orelse Prefix =:= undefined ->
    case sql_lib:add_object(Prefix, Obj) of
	{error, Error} ->
	    lager:error("[sqlite_server] Error composing SQL for adding object: ~p/~p: ~p",
			[Prefix, Obj#object.key, Error]);
	SQL ->
	    Timestamp = erlang:round(utils:timestamp()/1000),
	    UserId = Obj#object.author_id,
	    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL, Timestamp})
    end.


-spec(delete_pseudo_directory(BucketId :: string(), Prefix :: string(),
			      Name :: binary(), UserId :: string() ) -> ok | {error, any()}).
delete_pseudo_directory(BucketId, Prefix, Name, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_binary(Name) andalso erlang:is_list(UserId) ->
    Timestamp = erlang:round(utils:timestamp()/1000),
    SQL = sql_lib:delete_pseudo_directory(Prefix, Name),
    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL, Timestamp}).


-spec(lock_object(BucketId :: string(), Prefix :: string(), Key :: string(),
		  Value :: boolean(), UserId :: string()) -> ok | {error, any()}).
lock_object(BucketId, Prefix, Key, Value, UserId) ->
    SQL = sql_lib:lock_object(Prefix, Key, Value),
    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL, undefined}).

-spec(delete_object(BucketId :: string(), Prefix :: string(),
		    Key :: string(), UserId :: string() ) -> ok | {error, any()}).
delete_object(BucketId, Prefix, Key, UserId)
	when erlang:is_list(BucketId) andalso erlang:is_list(Prefix) orelse Prefix =:= undefined
	    andalso erlang:is_list(Key) andalso erlang:is_list(UserId) ->
    Timestamp = erlang:round(utils:timestamp()/1000),
    SQL = sql_lib:delete_object(Prefix, Key),
    gen_server:cast(?MODULE, {exec_sql, BucketId, UserId, SQL, Timestamp}).


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
handle_cast({create_pseudo_directory, BucketId, Prefix, Name, UserId, Timestamp}, State0) ->
    %% Acquire lock on db first
    case lock_db(BucketId) of
	ok ->
	    DbName = db_files,
	    case open_db(BucketId, UserId, DbName, Timestamp) of
		{error, _Reason} -> {noreply, State0};
		{ok, DbPid, TempFn, Version0} ->
		    %% Check if pseudo-directory exists first
		    SQL0 = sql_lib:get_pseudo_directory(Prefix, Name),
		    case sqlite3:sql_exec(DbName, SQL0) of
			[{columns, _}, {rows,[_OrigName]}] ->
			    %% Pseudo-directory is in DB already
			    {noreply, State0};
			[{columns, _}, {rows,[]}] ->
			    %% Create one
			    case sql_lib:create_pseudo_directory(Prefix, Name) of
				{error, Reason} ->
				    lager:error("[sqlite_server] Failed composing SQL for ~p/~p ~p ~p ~p: ~p",
						[BucketId, Prefix, Name, UserId, Timestamp, Reason]);
				SQL1 ->
				    Version1 = indexing:increment_version(Version0, Timestamp, UserId),
				    update_db(DbPid, DbName, TempFn, BucketId, UserId, Version1, SQL1),
				    unlock_db(BucketId, TempFn),
				    {noreply, State0}
			    end
		    end
	    end;
	locked ->
	    %% Retry later if locked
	    Timer = erlang:send_after(1000, self(), {handle_cast, [
		{create_pseudo_directory, BucketId, Prefix, Name, UserId, Timestamp}]}),
	    Timers = State0#state.timers,
	    {noreply, State0#state{timers = Timers ++ [[{bucket_id, BucketId}, {timer, Timer}]]}}
    end;


handle_cast({exec_sql, BucketId, UserId, SQL, Timestamp}, State0) ->
    %% Acquire lock on db first
    case lock_db(BucketId) of
	ok ->
	    DbName = db_files,
	    case open_db(BucketId, UserId, DbName, Timestamp) of
		{error, _Reason} -> {noreply, State0};
		{ok, DbPid, TempFn, Version0} ->
		    Version1 = indexing:increment_version(Version0, Timestamp, UserId),
		    update_db(DbPid, DbName, TempFn, BucketId, UserId, Version1, SQL),
		    unlock_db(BucketId, TempFn),
		    {noreply, State0}
	    end;
	locked ->
	    %% Retry later if locked
	    Timer = erlang:send_after(1000, self(), {handle_cast, [
		{exec_sql, BucketId, UserId, SQL, Timestamp}]}),
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
handle_info({update_db, FuncArgs}, State0) ->
    Func = element(1, FuncArgs),
    Args = element(2, FuncArgs),
    Result = Func(Args, State0),
    {noreply, element(2, Result)};

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
%% Read DB from Riak CS.
%%
open_db(BucketId, UserId, DbName, Timestamp) ->
    TempFn0 = os:cmd("/bin/mktemp --suffix=.db"),
    TempFn1 = re:replace(TempFn0, "[\r\n]", "", [global, {return, list}]),
    case riak_api:head_object(BucketId, ?DB_VERSION_KEY) of
	not_found ->
	    %% No SQLite db found, create a new one
	    {ok, Pid0} = sqlite3:open(DbName, [{file, TempFn1}]),
	    case sql_lib:create_table_if_not_exist(DbName) of
		ok ->
		    %% Create a new version
		    Version0 = indexing:increment_version(undefined, Timestamp, UserId),
		    {ok, Pid0, TempFn1, Version0};
		{error, Reason0} ->
		    ?ERROR("[sqlite_server] Failed to create table: ~p~n", [Reason0]),
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
	    {ok, Pid1, TempFn1, Version2}
    end.

%%
%% Write db to Riak CS
%%
update_db(DbPid, DbName, TempFn, BucketId, UserId, Version, SQL) ->
    case sqlite3:sql_exec(DbName, SQL) of
	{rowid, _} ->
	    sqlite3:close(DbPid),
	    {ok, Blob} = file:read_file(TempFn),
	    RiakOptions = [{acl, public_read}, {meta, [
		{"version", base64:encode(jsx:encode(Version))},
		{"user_id", UserId},
		{"bytes", byte_size(Blob)}]}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, RiakOptions);
	ok ->
	    sqlite3:close(DbPid),
	    {ok, Blob} = file:read_file(TempFn),
	    RiakOptions = [{acl, public_read}, {meta, [
		{"version", base64:encode(jsx:encode(Version))},
		{"user_id", UserId},
		{"bytes", byte_size(Blob)}]}],
	    riak_api:put_object(BucketId, undefined, ?DB_VERSION_KEY, Blob, RiakOptions);
	Error -> ?ERROR("[sqlite_server] SQL error: ~p", [Error])
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

%%
%% Remove lock object
%$ remove temporary db file
%%
unlock_db(BucketId, TempFn) ->
    riak_api:delete_object(BucketId, ?DB_VERSION_LOCK_FILENAME),
    file:delete(TempFn),
    ok.
