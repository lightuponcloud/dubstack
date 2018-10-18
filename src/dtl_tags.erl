%%
%% erlydtl tags, that are used from templates are defined here.
%%
-module(dtl_tags).
-behaviour(erlydtl_library).

-export([inventory/1, version/0, basename/1, get_value/2, even/1]).

-include("riak.hrl").

version() -> 1.

inventory(tags) -> [];
inventory(filters) -> [basename, get_value].

%% @doc Returns filepame without path
-spec basename(string()|binary()) -> string().

basename(ObjectName) when erlang:is_list(ObjectName) ->
    filename:basename(ObjectName);
basename(ObjectName) when erlang:is_binary(ObjectName) ->
    filename:basename(unicode:characters_to_list(ObjectName)).

%% @doc Returns value of proplist by key
-spec get_value(proplist(), string()) -> string().

get_value(Object, Attribute) when erlang:is_list(Object) ->
    proplists:get_value(utils:to_atom(Attribute), Object).

-spec even(list()) -> boolean().

even(Number) when erlang:is_list(Number) ->
    utils:even(utils:to_integer(Number)).
