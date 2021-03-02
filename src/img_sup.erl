%%
%% Supervises the image scaling gen_server
%%
-module(img_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init(_) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = [#{id => img,
		    start => {img, start_link, []},
		    restart => permanent,
		    shutdown => brutal_kill,
		    type => worker,
		    modules => []}
		],
    {ok, {SupFlags, ChildSpecs}}.
