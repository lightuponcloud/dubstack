%%
%% Allows to move objects and pseudo-directories.
%%
-module(move_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, is_authorized/2, forbidden/2,
	 handle_post/2]).

-include("riak.hrl").
-include("user.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Returns callback 'handle_post()'
%% ( called after 'resource_exists()' )
%%
content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State}.

%%
%% Returns callback 'to_json()'
%% ( called after 'forbidden()' )
%%
content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

%%
%% Validates POST request and sends request to Riak CS to copy objects and delete them
%%
handle_post(Req0, State0) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    case copy_handler:validate_copy_parameters(State0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		State1 ->
		    %% Set uncommited flag, as it might take a while to copy objects.
		    %% Clients are expected to retrieve updated list later.
		    SrcBucketId = proplists:get_value(src_bucket_id, State1),
		    SrcPrefix = proplists:get_value(src_prefix, State1),
		    case indexing:update(SrcBucketId, SrcPrefix, [{uncommitted, true}]) of
			lock ->
			    Req1 = cowboy_req:reply(202, #{
				<<"content-type">> => <<"application/json">>
			    }, <<"[]">>, Req0),
			    {true, Req1, []};
			_ -> move(Req0, State1)
		    end
	    end;
	_ -> js_handler:bad_request(Req0, 16)
    end.

move(Req0, State) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State),
    SrcPrefix0 = proplists:get_value(src_prefix, State),
    SrcObjectKeys = proplists:get_value(src_object_keys, State),
    DstBucketId = proplists:get_value(dst_bucket_id, State),
    DstPrefix0 = proplists:get_value(dst_prefix, State),
    ObjectKeysToMove0 = lists:map(
       fun(N) ->
	    case utils:ends_with(N, <<"/">>) of
		true ->
		    ON = string:to_lower(N),  %% lowercase hex prefix
		    riak_api:recursively_list_pseudo_dir(SrcBucketId, utils:prefixed_object_key(SrcPrefix0, ON));
		false -> [utils:prefixed_object_key(SrcPrefix0, N)]
	    end
       end, SrcObjectKeys),
    ObjectKeysToMove1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], ObjectKeysToMove0),

    SrcIndexPath = utils:prefixed_object_key(SrcPrefix0, ?RIAK_INDEX_FILENAME),
    SrcIndexPaths = [IndexPath || IndexPath <- ObjectKeysToMove1, lists:suffix(?RIAK_INDEX_FILENAME, IndexPath)
			] ++ [SrcIndexPath],
    MovedObjects0 = [
	copy_handler:copy_objects(SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0, ObjectKeysToMove1, I)
	|| I <- SrcIndexPaths],
    MovedObjects1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], [element(2, I) || I <- MovedObjects0]),
    %% Delete objects in previous place
    lists:map(
	fun(I) ->
	    SrcPrefix = proplists:get_value(src_prefix, I),
	    OldKey = proplists:get_value(old_key, I),
	    PrefixedObjectKey = utils:prefixed_object_key(SrcPrefix, OldKey),
	    riak_api:delete_object(SrcBucketId, PrefixedObjectKey)
	end, MovedObjects1),
    %% Delete pseudo-directories in previous place
    lists:map(
	fun(I) ->
	    ActionLogPath = utils:prefixed_object_key(filename:dirname(I), ?RIAK_ACTION_LOG_FILENAME),
	    riak_api:delete_object(SrcBucketId, ActionLogPath),
	    riak_api:delete_object(SrcBucketId, I)
	end, lists:droplast(SrcIndexPaths)),
    %% If index update failed in copy module, it makes no sense to make an attempt
    %% at updating it here, as Riak CS is busy handling many requests at the moment.
    StatusCodes = [element(1, C) || C <- MovedObjects0],
    StatusCode1 =
	case lists:any(fun(C) -> C =/= 200 end, StatusCodes) of
	    false ->
		case indexing:update(SrcBucketId, SrcPrefix0) of
		    lock -> 202;
		    _ -> 200
		end;
	    true -> 202
	end,
    %%
    %% Add action log record
    %%
    MovedDirectories =
	case length(SrcIndexPaths) > 1 of
	    true ->
		lists:map(
		    fun(I) ->
			D = utils:unhex(erlang:list_to_binary(filename:basename(filename:dirname(I)))),
			[[" \""], unicode:characters_to_list(D), ["/\""]]
		    end, lists:droplast(SrcIndexPaths));
	    false -> [" "]
	end,
    MovedObjects2 = lists:map(
	fun(I) ->
	    case proplists:get_value(src_orig_name, I) =:= proplists:get_value(dst_orig_name, I) of
		true -> [[" \""], unicode:characters_to_list(proplists:get_value(dst_orig_name, I)), "\""];
		false -> [[" \""], unicode:characters_to_list(proplists:get_value(src_orig_name, I)), ["\""],
			 [" as \""], unicode:characters_to_list(proplists:get_value(dst_orig_name, I)), ["\""]]
	    end
	end, [J || J <- MovedObjects1, proplists:get_value(src_prefix, J) =:= filename:dirname(SrcIndexPath)]),
    User = proplists:get_value(user, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="copy",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()/1000])
    },
    SrcPrefix1 =
	case SrcPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(SrcPrefix0)) ++ ["/"]
	end,
    Summary0 = lists:flatten([["Moved"], MovedDirectories ++ MovedObjects2,
			     ["\" from \"", SrcPrefix1, "\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(DstBucketId, DstPrefix0, ActionLogRecord1),
    DstPrefix1 =
	case DstPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(DstPrefix0))++["/"]
	end,
    Summary1 = lists:flatten([["Moved"], MovedDirectories ++ MovedObjects2,
				 [" to \""], [DstPrefix1, "\"."]]),
    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
    action_log:add_record(SrcBucketId, SrcPrefix0, ActionLogRecord2),
    Req1 = cowboy_req:reply(StatusCode1, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode(MovedObjects1), Req0),
    {true, Req1, []}.

%%
%% Serializes response to json
%%
to_json(Req0, State) ->
    {<<"{\"status\": \"ok\"}">>, Req0, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, _State) ->
    utils:is_authorized(Req0).

%%
%% Checks if user has access
%% - To source bucket
%% - To destination bucket
%%
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    copy_handler:copy_forbidden(Req0, State).
