%%
%% List all users.
%%
-module(list_users).

-export([main/0]).

-include("../include/entities.hrl").
-include("../include/riak.hrl").


main() ->
    inets:start(),

    Users = admin_users_handler:get_full_users_list(),
    UsersList = [admin_users_handler:user_to_proplist(U) || U <- Users, U =/= not_found], %% tenant might be deleted
    lists:foreach(
	fun(U) ->
	    io:fwrite("~p~n", [proplists:get_value(login, U)])
	end, UsersList).
