%%
%% Supervises the cryptographic
%%
-module(riak_crypto_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_) ->
    {ok, { {one_for_one, 5, 1}, []} }.