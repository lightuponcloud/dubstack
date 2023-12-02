%%
%% This asyncronous process broadcasts messages to websockets.
%%
-module(events_server_sup).
-behaviour(gen_server).

-include("general.hrl").
-include("log.hrl").

%% API

-export([start_link/0, start/0, stop/0]).
-export([new_subscriber/4, logout_subscriber/1, send_message/3,
	 user_active/1, confirm_reception/3, get_subscribers/0]).

%% Behaviour callbacks.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

%% In case confirmation is not received within
%% MESSAGE_RETRY_INTERVAL * MESSAGE_RETRY_ATTEMPTS
%% we close the websocket connection.
-define(MESSAGE_RETRY_ATTEMPTS, 1).
-define(MESSAGE_RETRY_INTERVAL, 1000).  %% 1 second
%% process group name ( used to store groups of PIds )
-define(WS_PG, notifications).


%% type for subscriber entry: {UserId, WebsocketPid, SessionId}
-type(subscriber_entry() :: {string(), pid(), string()}).

%% The following defines a server state
%% Subscribers example: [{UserId, Pid, SessionId}]
%% atomic_ids is the list of message IDs that require confirmation
-record(state, {atomic_ids=[], subscribers=[]}).
%% Message entry, contains the counter ( number of attempts ),
%% pid ( to send terminate signal to ) and the message itself.
-record(message_entry, {
			counter = 0,
			session_id = undefined,
			user_id = undefined,
			bucket_id = undefined,
			atomic_id = undefined,
			pid = undefined,
			message = undefined
                       }).

%% Management API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).


stop() ->
    gen_server:call(?MODULE, stop).

%%
%% Registers a new subscriber.
%%
new_subscriber(UserId, Pid, SessionId, BucketIdList)
	when erlang:is_list(UserId) andalso erlang:is_list(SessionId) andalso
	     erlang:is_list(BucketIdList) ->
    gen_server:call(?MODULE, {new_subscriber, UserId, Pid, SessionId, BucketIdList}).

logout_subscriber(SessionId) ->
    %% send message to all nodes, to remove User's websocket pid.
    Pids = pg:get_members(?SCOPE_PG, ?WS_PG),
    [gen_server:call(SP, {logout_subscriber, SessionId}) || SP <- Pids].

get_subscribers() ->
    gen_server:call(?MODULE, get_subscribers).

%%
%% Check user has active channel
%%
-spec(user_active(UserId :: string()) -> boolean()).
user_active(UserId) when erlang:is_list(UserId) ->
    %% get servers from all nodes
    Pids = pg:get_members(?SCOPE_PG, ?WS_PG),
    user_active(UserId, Pids, []).


-spec(user_active(UserId :: string(), [pid()], [subscriber_entry()]) ->
             [subscriber_entry()]).
user_active(_UserId, [], Acc) -> Acc;

user_active(UserId, [Pid | T], Acc) ->
    Subs =  gen_server:call(Pid, {user_active, UserId}),
    user_active(UserId, T, lists:append(Subs, Acc)).


%%
%% Send message to user's session asyncronously.
%%
-spec send_message(BucketId :: string(), AtomicId :: string(), Msg :: binary()) -> ok.

send_message(BucketId, AtomicId, Msg) when erlang:is_list(BucketId) andalso erlang:is_list(AtomicId) ->
    Pids = pg:get_members(?SCOPE_PG, ?WS_PG),
    Subscribers = get_subscribers(),
    lists:foreach(
	fun(I) ->
	    UserId = element(1, I),
	    [gen_server:cast(Pid, {message, BucketId, UserId, AtomicId, Msg}) || Pid <- Pids]
	end, Subscribers),
    ok.


-spec confirm_reception(AtomicId :: string(), UserId :: string(), SessionId :: string()) -> ok.
confirm_reception(AtomicId, UserId, SessionId)
	when erlang:is_list(AtomicId) andalso erlang:is_list(UserId) andalso erlang:is_list(SessionId) ->
    gen_server:cast(?MODULE, {confirmation, AtomicId, UserId, SessionId}).

%% Callbacks.

init(_) ->
    process_flag(trap_exit, true),
    %% join global group, to be accessible from all nodes
    pg:join(?SCOPE_PG, ?WS_PG, self()),
    {ok, #state{atomic_ids=[], subscribers=[]}}.


terminate(_Reason, _State) -> ok.

%%
%% Stop the server
%%
handle_call(stop, _, State) ->
    {stop, normal, ok, State};

%%
%% Registers a new subscriber syncronously (UserId -> Pid mapping).
%%
handle_call({new_subscriber, UserId, Pid, SessionId, BucketIdList}, _, State0) ->
    State1 = add_subscriber(UserId, Pid, SessionId, BucketIdList, State0),
    ?INFO("[events_server_sup] Added subscriber ~p", [UserId]),
    {reply, ok, State1};

handle_call({logout_subscriber, SessionId}, _, State) ->
    %% Subscribers = [subscriber_entry(), ..]
    Subscribers = [S || S <- State#state.subscribers, element(3, S) =:= SessionId],
    case Subscribers of
	[] ->
	    ?INFO("[events_server_sup] Subscriber not found by SessionId: ~p", [SessionId]),
	    {reply, ok, State};
	_ ->
	    State1 = remove_subscriber(SessionId, State),
            {reply, ok, State1}
    end;
handle_call({user_active, UserId}, _, State) ->
    %% Subscribers = [subscriber_entry(), ..]
    Subscribers = [S || S <- State#state.subscribers, element(1, S) =:= UserId],
    {reply, Subscribers, State};
handle_call(get_subscribers, _, State) ->
    {reply, State#state.subscribers, State};
%% Other messages
handle_call(_Other, _, State) ->
    {reply, {error, request}, State}.

%%
%% Sends messages to connected websocket sessions asyncronously.
%%
handle_cast({message, BucketId, UserId, AtomicId, Msg}, State) ->
    ?INFO("[events_server_sup] to: ~s sending: ~p", [BucketId, UserId, Msg]),
    %% Subscribers = [subscriber_entry(), ..]
    Subscribers = [S || S <- State#state.subscribers, lists:member(BucketId, element(4, S))],
    ?INFO("[events_server_sup] Subscribers: ~p~n, user id: ~p~nbucket id: ~p~nresult: ~p",
	  [State#state.subscribers, UserId, BucketId, Subscribers]),
    lists:map(
      fun(S) ->
	Pid = element(2, S),
	SessionId = element(3, S),
	send_msg(Pid, jsx:encode([{id, utils:to_binary(AtomicId)}, {message, utils:to_binary(Msg)}])),

	%% Add task to check if message was delivered
	MessageEntry = #message_entry{
	    counter = 1,
	    session_id = SessionId,
	    user_id = UserId,
	    bucket_id = BucketId,
	    pid = Pid,
	    atomic_id = AtomicId,
	    message = Msg
	},
	erlang:send_after(?MESSAGE_RETRY_INTERVAL, ?MODULE,
			  {check_confirmation, MessageEntry})
      end, Subscribers),
    %% Save message IDs for retries
    AtomicIds = State#state.atomic_ids ++ [{AtomicId, UserId}],
    {noreply, State#state{atomic_ids = AtomicIds}};

%%
%% Confirmation on reception of message.
%%
handle_cast({confirmation, AtomicId, UserId, _SessionId}, State) ->
    %% Remove from the list of awaited confirmations
    AtomicIds = lists:delete({AtomicId, UserId}, State#state.atomic_ids),

    {noreply, State#state{atomic_ids = AtomicIds}};

handle_cast(_, State) ->
    {noreply, State}.

%%
%% Check if confirmation received. Terminate websocket connection
%% in case confirmation not received ``MESSAGE_RETRY_ATTEMPTS`` times.
%%
handle_info({check_confirmation,
             #message_entry{
                counter = Counter,
                pid = Pid,
                session_id = SessionId,
		user_id = UserId,
                atomic_id = AtomicId} = _MsgEntry}, State)
  when Counter > ?MESSAGE_RETRY_ATTEMPTS ->
    ?INFO("[events_server_sup] Terminating session ~s on timeout.", [SessionId]),
    Pid ! {events_server_sup, stop},
    AtomicIds = lists:delete({AtomicId, UserId}, State#state.atomic_ids),
    {noreply, State#state{atomic_ids = AtomicIds}};

handle_info({check_confirmation,
             #message_entry{
                counter = Counter,
                pid = Pid,
                atomic_id = AtomicId,
		user_id = UserId,
                message = Message} = MessageEntry0}, State) ->
    case lists:member({AtomicId, UserId}, State#state.atomic_ids) of
	false -> ok;
	true ->
	    %% Wait and try again, as message has not been confirmed
	    MessageEntry1 = MessageEntry0#message_entry{counter = Counter + 1},
	    erlang:send_after(?MESSAGE_RETRY_INTERVAL, ?MODULE,
                              {check_confirmation, MessageEntry1}),
	    send_msg(Pid, Message)
    end,
    {noreply, State};


%% Process has died
handle_info({'EXIT', _Pid, _}, State) ->
    {noreply, State};

%% Ignore unknown messages
handle_info(_, State) ->
    {noreply, State}.


code_change(_Vsn, State, _Extra) ->
    {ok, State}.

send_msg(Pid, Msg) ->
    ?INFO("[events_server_sup] Sending message: ~p", [Msg]),
    Pid ! {events_server_sup, utils:to_binary(Msg)}.

add_subscriber(UserId, Pid, SessionId, BucketIdList, State) ->
    Subscribers = State#state.subscribers ++ [{UserId, Pid, SessionId, BucketIdList}],
    ?INFO("[events_server_sup] Subscribers: ~p", [Subscribers]),
    State#state{subscribers = Subscribers}.

%%
%% Terminates user sessions on the same device and removes stale session
%%
remove_subscriber(SessionId, State) when erlang:is_list(SessionId) ->
    %% sending stop to all websocket notification servers
    [ element(2, S) ! {events_server_sup, stop}
      || S <- State#state.subscribers, element(3, S) =:= SessionId],

    Subscribers = [S || S <- State#state.subscribers, element(3, S) =/= SessionId],
    State#state{subscribers=Subscribers}.
