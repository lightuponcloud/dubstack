-module(riak_crypto_app).
-behaviour(application).

-export([start/2, stop/1, start/0, stop/0]).

start(_Type, _Args) ->
    riak_crypto:start_link().
stop(_State) -> ok.

start() ->
  application:start(riak_crypto_app).

stop() ->
  application:stop(riak_crypto_app).
