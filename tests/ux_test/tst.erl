-module(tst).
-export([tst/0]).

get_source_from('process', Value) ->
    erlang:get(Value);
get_source_from('application', Value) ->
    application:get_env(Value);
get_source_from('node', Value) ->
    case erlang:whereis(ux_unidata_server) of
    undefined ->
        ux:start(),
        ux_unidata_server:get_default(Value);
    _ -> 
        ux_unidata_server:get_default(Value) 
    end.


%% @doc Return registred fun.
%% Check: the dict of client process, then application enviroment, 
%% then try get the default value from the server.
get_source(Parser, Type) ->
    Value = {Parser, Type},
    get_source(Value).


-spec get_source({Parser::atom(), Type::atom()}) -> fun() | undefined.

%% @doc Try retrieve the information about the data source:
%% Step 1: Check process dictionary.
%% Step 2: Check application enviroment.
%% Step 3: Use defaults.
get_source(Value) ->
    case get_source_from(process, Value) of            % step 1
    'undefined' -> 
        case get_source_from(application, Value) of    % step 2
        'undefined' -> get_source_from('node', Value); % step 3
        Fun -> Fun
        end;
    Fun -> Fun
    end.


-spec char_to_lower(char()) -> char(); 
        (skip_check) -> fun().

char_to_lower(C) -> 
    func(unidata, to_lower, C).


func(Parser, Type, Value) -> 
    F = ux_unidata_filelist:get_source(Parser, Type),
    F(Value).


-spec to_lower(char()) -> char(); 
        (skip_check) -> fun().

to_lower(V) -> 
    char_to_lower(V).


%% @doc Converts characters of a string to a lowercase format.
-spec to_lower(string()) -> string().

to_lower(Str) ->
    Fun = ux_char:to_lower(skip_check),
    lists:map(Fun, Str).

tst() ->
    Str = unicode:characters_to_list(<<"СЛОВО">>),
    to_lower(Str).
