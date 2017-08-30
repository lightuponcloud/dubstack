-module(middleware_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
	ChildSpecs = [#{id => solr_api,
                    start => {solr_api, start_link, []},
                    restart => permanent,
                    shutdown => 10000,
                    type => worker,
                    modules => [solr_api]}],
	{ok, {SupFlags, ChildSpecs}}.
