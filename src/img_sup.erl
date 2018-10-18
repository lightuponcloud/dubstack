%%
%% Supervises the image scaling gen_server
%%
-module(img_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_) ->
    ImgSpec = {img,
                   {img, start_link, []},
                   permanent, brutal_kill, worker, [img]},

    {ok, {{one_for_one, 5, 1}, [ImgSpec]}}.
