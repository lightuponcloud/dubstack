%%
%% Websocket handler pushes async messages from events_server
%% to subscribers.
%%
-module(ws_handler).

%% Cowboy callbacks.
-export([init/2, websocket_handle/2, websocket_info/2, websocket_init/1]).

-export([content_types_provided/2, to_json/2, terminate/3]).

-include("general.hrl").
-include("entities.hrl").
-include("log.hrl").

content_types_provided(Req, State) ->
    {[
      {{<<"application">>, <<"json">>, '*'}, to_json}
     ], Req, State}.


to_json(Req0, _State) -> {<<>>, Req0, []}.


init(Req0, _Opts) ->
    %% Set CORS headers
    Req1 = cowboy_req:set_resp_header(<<"access-control-allow-methods">>, <<"GET, OPTIONS">>, Req0),
    Req2 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>, <<"*">>, Req1),
    {cowboy_websocket, Req2, []}.


websocket_init(State) ->
    {ok, State}.

parse_message(<<"CONFIRM ", AtomicId/binary>>, State) ->
    case proplists:get_value(user_id, State) of
	undefined -> {ok, State};  %% not logged in
	UserId0 ->
	    case byte_size(AtomicId) > 0 of
		true ->
		    %% Delete message from queue only if session is present
		    SessionId = proplists:get_value(session_id, State),
		    events_server_sup:confirm_reception(utils:to_list(AtomicId), UserId0, SessionId),
		    {ok, State};
		false -> ok
	    end
    end;
parse_message(<<"Authorization ", Token0/binary>>, State) ->
    Token1 = utils:to_list(Token0),
    case login_handler:check_token(Token1) of
	not_found ->
	    ?INFO("[ws_handler] authentication failed for token ~p: not_found", [Token1]),
	    {ok, State};
	expired ->
	    ?INFO("[ws_handler] authentication failed for token ~p: expired", [Token1]),
	    {ok, State};
	User0 ->
	    ?INFO("[ws_handler] authentication passed"),
	    {ok, [{user_id, User0#user.id}, {session_id, Token1}]}
    end;
parse_message(<<"SUBSCRIBE ", BucketIdList0/binary>>, State) ->
    case proplists:get_value(user_id, State) of
	undefined -> {ok, State};  %% not logged in
	UserId ->
	    BucketIdList1 = binary:split(BucketIdList0, <<" ">>, [global]),
	    BucketIdList2 = [erlang:binary_to_list(B) || B <- BucketIdList1],
	    SessionId = proplists:get_value(session_id, State),
	    events_server_sup:new_subscriber(UserId, self(), SessionId, BucketIdList2),
	    {ok, State}
    end;
parse_message(_, State) -> {ok, State}.

%% Callback on received websockets data.
websocket_handle({text, Data}, State) ->
    parse_message(Data, State);

websocket_handle(_, State) ->
    {ok, State}.

%% Callback on message from erlang.
websocket_info({events_server_sup, stop}, State) ->
    {stop, State};
websocket_info({events_server_sup, Data0}, State) ->
    Data1 = jsx:encode(Data0),
    {reply, {binary, Data1}, State};
websocket_info(_, State) ->
    {ok, State}.


terminate(Reason, PartialReq, State) ->
    %% terminating, going to deregister from _sup
    case proplists:get_value(user_id, State) of
	undefined ->
	    ?INFO("[ws_handler] terminating, reason: ~p, req: ~p", [Reason, PartialReq]);
	UserId ->
	    ?INFO("[ws_handler] terminating for user ~s, reason: ~p, req: ~p",
		[UserId, Reason, PartialReq]),
	    SessionId = proplists:get_value(session_id, State),
	    events_server_sup:logout_subscriber(SessionId)
    end,
    ok.
