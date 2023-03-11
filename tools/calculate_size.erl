%%
%% Calculates size of directories and buckets.
%%
-module(calculate_size).

-export([main/0]).

-include("../include/entities.hrl").
-include("../include/riak.hrl").

%%
%% Returns true, if any directory in hierarchy contains "-deleted-<timestamp>"
%%
is_deleted([H|T]) ->
    Name = erlang:binary_to_list(utils:unhex(H)),
    case string:str(Name, "-deleted-") of
	0 ->
	    %% "-deleted-" string was not found
	    is_deleted(T);
	_ -> true
    end;
is_deleted([]) -> false;

is_deleted(HexPrefix) when erlang:is_binary(HexPrefix) ->
    case erlang:byte_size(HexPrefix) > 0 of
	false -> false;
	true -> is_deleted(binary:split(HexPrefix, <<"/">>, [global]))
    end.


get_size(_Obj, true) -> 0;
get_size(Obj, _IsDeleted) ->
    proplists:get_value(bytes, Obj).


main() ->
    inets:start(),
    {ok, BucketArg} = init:get_argument(bucket),
    BucketId = lists:flatten(BucketArg),
    case BucketId of
	undefined -> throw("BucketId is expected");
	[] -> throw("BucketId is expected");
	_ -> ok
    end,

    io:fwrite("Measuring bucket ~p", [BucketId]),

    List0 = riak_api:recursively_list_pseudo_dir(BucketId, undefined),
    IndexName1 = erlang:list_to_binary(?RIAK_INDEX_FILENAME),

    TotalSize = lists:foldl(
	fun(Path, TotalSizeAcc) ->
	    case utils:ends_with(erlang:list_to_binary(Path), IndexName1) of
		true ->
		    DirPrefix = utils:dirname(Path),
		    IsDeleted0 =
			case DirPrefix of
			    undefined -> false;
			    _ -> is_deleted(erlang:list_to_binary(DirPrefix))
			end,
		    case riak_api:get_object(BucketId, Path) of
			{error, _Reason} -> ok;
			not_found -> ok;
			IndexObject ->
			    Index = erlang:binary_to_term(proplists:get_value(content, IndexObject)),
			    List1 = proplists:get_value(list, Index),

			    DirSize = lists:foldl(
				fun(I, SizeAcc) ->
				    IsDeleted1 =
					case IsDeleted0 of
					    true -> true;
					    false -> proplists:get_value(is_deleted, I)
					end,
				    SizeAcc + get_size(I, IsDeleted1)
				end, 0, List1),
			    io:fwrite("Path: ~p: ~p bytes~n", [Path, DirSize]),
			    TotalSizeAcc + DirSize
		    end;
		false -> TotalSizeAcc
	    end
	end, 0, List0),
    io:fwrite("Total: ~p~n", [TotalSize]).
