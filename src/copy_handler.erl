%%
%% Allows to copy objects and pseudo-directories.
%%
-module(copy_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, is_authorized/2, forbidden/2,
	 handle_post/2, copy_objects/6, copy_forbidden/2,
	 validate_copy_parameters/1]).

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
%% Checks if source object exists.
%%
-spec validate_src_object_key(list(), list(), binary()) -> list()|{error, integer()}.

validate_src_object_key(BucketId, SrcPrefix, ObjectKey0)
	when erlang:is_binary(ObjectKey0), erlang:is_list(BucketId),
	     erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined ->
    ObjectKey1 = utils:trim_spaces(ObjectKey0),
    case ObjectKey1 of
	<<>> -> {error, 30};
	_ ->
	    IndexContent = indexing:get_index(BucketId, SrcPrefix),
	    ExistingPrefixes = [proplists:get_value(prefix, P)
                        || P <- proplists:get_value(dirs, IndexContent, [])],
	    ExistingKeys = [proplists:get_value(object_key, O)
                         || O <- proplists:get_value(list, IndexContent, [])],
	    ObjectKey2 = erlang:binary_to_list(ObjectKey1),
	    PrefixedObjectKey = utils:prefixed_object_key(SrcPrefix, ObjectKey2),
	    case utils:ends_with(ObjectKey1, <<"/">>) of
		true ->
		    case lists:member(erlang:list_to_binary(PrefixedObjectKey), ExistingPrefixes) of
			false -> {error, 32};
			true -> ObjectKey2
		    end;
		false ->
		    case lists:member(ObjectKey1, ExistingKeys) of
			false -> {error, 30};
			true -> ObjectKey2
		    end
	    end
    end.

%%
%% Checks if every key in src_object_keys exists and ther'a no duplicates
%%
validate_src_object_keys(_BucketId, _SrcPrefix, null) -> [];
validate_src_object_keys(_BucketId, _SrcPrefix, undefined) -> [];
validate_src_object_keys(_BucketId, _SrcPrefix, <<>>) -> {error, 15};
validate_src_object_keys(BucketId, SrcPrefix, SrcObjectKeys0) when erlang:is_binary(SrcObjectKeys0),
	erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined ->
    SrcObjectKeys1 = [K || K <- binary:split(SrcObjectKeys0, <<",">>, [global]), erlang:byte_size(K) > 0],
    validate_src_object_keys(BucketId, SrcPrefix, SrcObjectKeys1);
validate_src_object_keys(BucketId, SrcPrefix, SrcObjectKeys0)
	when erlang:is_list(SrcObjectKeys0), erlang:is_list(SrcPrefix) orelse SrcPrefix =:= undefined ->
    SrcObjectKeys1 = [validate_src_object_key(BucketId, SrcPrefix, K) || K <- SrcObjectKeys0],
    Error = lists:keyfind(error, 1, SrcObjectKeys1),
    case Error of
	{error, Number} -> {error, Number};
	_ ->
	    case utils:has_duplicates(SrcObjectKeys1) of
		true -> {error, 31};
		false -> SrcObjectKeys1
	    end
    end.

-spec validate_copy_parameters(proplist()) -> any().

validate_copy_parameters(State0) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State0),
    DstBucketId = proplists:get_value(dst_bucket_id, State0),
    SrcPrefix0 = proplists:get_value(src_prefix, State0),
    DstPrefix0 = proplists:get_value(dst_prefix, State0),
    SrcObjectKeys0 = proplists:get_value(src_object_keys, State0),

    SrcPrefix1 = list_handler:validate_prefix(SrcBucketId, SrcPrefix0),
    DstPrefix1 = list_handler:validate_prefix(DstBucketId, DstPrefix0),
    SrcObjectKeys1 = validate_src_object_keys(SrcBucketId, SrcPrefix1, SrcObjectKeys0),

    Error = lists:keyfind(error, 1, [SrcPrefix1, DstPrefix1, SrcObjectKeys1]),
    case Error of
	{error, Number} -> {error, Number};
	_ ->
	    SrcObjectKeys2 = lists:filter(
		fun(N) ->
		    PN0 = utils:prefixed_object_key(SrcPrefix1, N),
		    PN1 = erlang:list_to_binary(PN0),
		    case utils:starts_with(DstPrefix1, PN1) of
			false -> true;
			true -> false
		    end
		end, SrcObjectKeys1),
	    %% The destination directory might be subdirectory of the source directory.
	    case length(SrcObjectKeys2) =:= 0 orelse DstPrefix1 =:= SrcPrefix1 of
		true -> {error, 13};
		false ->
		    User = proplists:get_value(user, State0),
		    [{src_bucket_id, SrcBucketId},
			      {dst_bucket_id, DstBucketId},
			      {src_prefix, SrcPrefix1},
			      {dst_prefix, DstPrefix1},
			      {src_object_keys, SrcObjectKeys2},
			      {user, User}]
	    end
    end.

%%
%% Validates POST request and sends request to Riak CS to copy object or pseudo-directory
%%
handle_post(Req0, State0) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    case validate_copy_parameters(State0) of
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
			_ -> copy(Req0, State1)
		    end
	    end;
	_ -> js_handler:bad_request(Req0, 16)
    end.

%%
%% Copies object.
%%
%% The following function do not perform copy in the following cases.
%%	- Object is marked as deleted
%%	- Riak CS returned error
%%
-spec do_copy(string(), string(), string(), string(), string(), list()) -> list().

do_copy(SrcBucketId, DstBucketId, PrefixedObjectKey0, DstPrefix0, SrcIndexContent, DstIndexContent) ->
    ObjectKey0 = filename:basename(PrefixedObjectKey0),
    ObjectRecord = indexing:get_object_record(SrcIndexContent, ObjectKey0),
    OrigName0 = proplists:get_value(orig_name, ObjectRecord),
    Bytes = proplists:get_value(bytes, ObjectRecord),

    case proplists:get_value(is_deleted, ObjectRecord) of
	true -> undefined;
	undefined -> undefined;
	false ->
	    %% Determine destination object name
	    ModifiedTime = proplists:get_value(last_modified_utc, ObjectRecord),
	    MD5 = proplists:get_value(md5, ObjectRecord),
	    {ObjectKey1, OrigName1, _} = riak_api:pick_object_key(DstBucketId, DstPrefix0,
		OrigName0, ModifiedTime, DstIndexContent),
	    PrefixedObjectKey1 = utils:prefixed_object_key(DstPrefix0, ObjectKey1),
	    CopyResult = riak_api:copy_object(DstBucketId, PrefixedObjectKey1, SrcBucketId,
		PrefixedObjectKey0, [{acl, public_read}]),
	    case proplists:get_value(content_length, CopyResult, 0) == 0 of
		true -> undefined;
		false ->
		    OldKey = filename:basename(PrefixedObjectKey0),
		    NewKey = filename:basename(PrefixedObjectKey1),
		    IsRenamed = OrigName1 =/= OrigName0 orelse OldKey =/= NewKey,
		    [
			{src_prefix, filename:dirname(PrefixedObjectKey0)},
			{dst_prefix, DstPrefix0},
			{old_key, OldKey},
			{new_key, NewKey},
			{src_orig_name, OrigName0},
			{dst_orig_name, OrigName1},
			{bytes, Bytes},
			{renamed, IsRenamed},
			{md5, MD5}  %% Riak CS COPY operation changes MD5, we need to preserve it
		    ]
	    end
    end.

%%
%% The following function returns list of objects that were copied.
%%
copy_objects(SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0, ObjectKeysToCopy, SrcIndexPrefixPath) ->
    CurrentSrcPrefix0 = filename:dirname(SrcIndexPrefixPath),
    CurrentSrcPrefix1 =
	case CurrentSrcPrefix0 of
	    "." -> undefined;
	    _ -> CurrentSrcPrefix0++"/"
	end,
    ShortenSrcPrefix =
	case CurrentSrcPrefix0 of
	    "." -> "";
	    _ ->
		case SrcPrefix0 of
		    undefined -> CurrentSrcPrefix0++"/";
		    _ -> re:replace(CurrentSrcPrefix0++"/", "^"++SrcPrefix0, "", [{return, list}])
		end
	end,
    CurrentDstPrefix = utils:prefixed_object_key(DstPrefix0, ShortenSrcPrefix),
    SrcIndexContent = indexing:get_index(SrcBucketId, CurrentSrcPrefix1),
    DstIndexContent = indexing:get_index(DstBucketId, CurrentDstPrefix),
    CopiedObjects0 = [do_copy(SrcBucketId, DstBucketId, PrefixedObjectKey, CurrentDstPrefix,
	SrcIndexContent, DstIndexContent) || PrefixedObjectKey <- ObjectKeysToCopy,
	utils:is_hidden_object([{key, PrefixedObjectKey}]) =/= true andalso
	filename:dirname(PrefixedObjectKey) =:= CurrentSrcPrefix0],
    CopiedObjects1 = [I || I <- CopiedObjects0, I =/= undefined],

    %% Copy action log if it absent in destination directory
    lists:map(
	fun(PrefixedObjectKey) ->
	    case filename:basename(PrefixedObjectKey) =:= ?RIAK_ACTION_LOG_FILENAME andalso
		    filename:dirname(PrefixedObjectKey) =:= CurrentSrcPrefix0 of
		true ->
		    DstActionLog = utils:prefixed_object_key(CurrentDstPrefix, ?RIAK_ACTION_LOG_FILENAME),
		    case riak_api:head_object(DstBucketId, DstActionLog) of
			not_found -> riak_api:copy_object(DstBucketId, DstActionLog, SrcBucketId,
							  PrefixedObjectKey, [{acl, public_read}]);
			_ -> ok
		    end;
		false -> ok
	    end
	end, ObjectKeysToCopy),
    %% Update indices for pseudo-sub-directories, as some of objects were renamed.
    %% Rename map should be updated in index file.

    %% Return copied objects list, so client can resume operation.
    case indexing:update(DstBucketId, CurrentDstPrefix, [{copy_from, [
			     {bucket_id, SrcBucketId},
			     {prefix, CurrentSrcPrefix1},
			     {copied_names, CopiedObjects1}]}]) of
	lock -> {202, CopiedObjects1};
	_ ->
	    %% Update parent directory
	    ParentDir0 =
		case filename:dirname(CurrentDstPrefix) of
		    "." -> undefined;
		    ParentDir1 -> ParentDir1++"/"
		end,
	    case filename:dirname(CurrentDstPrefix) =:= "." of
		true -> {200, CopiedObjects1};
		false ->
		    case indexing:update(DstBucketId, ParentDir0) of
			lock -> {202, CopiedObjects1};
			_ -> {200, CopiedObjects1}
		    end
	    end
    end.

copy(Req0, State) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State),
    SrcPrefix0 = proplists:get_value(src_prefix, State),
    SrcObjectKeys = proplists:get_value(src_object_keys, State),
    DstBucketId = proplists:get_value(dst_bucket_id, State),
    DstPrefix0 = proplists:get_value(dst_prefix, State),
    ObjectKeysToCopy0 = lists:map(
	fun(N) ->
	    case utils:ends_with(N, <<"/">>) of
		true ->
		    ON = string:to_lower(N),  %% lowercase hex prefix
		    riak_api:recursively_list_pseudo_dir(SrcBucketId, utils:prefixed_object_key(SrcPrefix0, ON));
		false -> [utils:prefixed_object_key(SrcPrefix0, N)]
	    end
	end, SrcObjectKeys),

    ObjectKeysToCopy1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], ObjectKeysToCopy0),

    SrcIndexPath = utils:prefixed_object_key(SrcPrefix0, ?RIAK_INDEX_FILENAME),
    SrcIndexPaths = [IndexPath || IndexPath <- ObjectKeysToCopy1, lists:suffix(?RIAK_INDEX_FILENAME, IndexPath)
			] ++ [SrcIndexPath],
    CopiedObjects0 = [
	copy_objects(SrcBucketId, DstBucketId, SrcPrefix0, DstPrefix0, ObjectKeysToCopy1, I)
	|| I <- SrcIndexPaths],
    CopiedObjects1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], [element(2, I) || I <- CopiedObjects0]),
    %%
    %% Add action log record
    %%
    CopiedDirectories =
	case length(SrcIndexPaths) > 1 of
	    true ->
		    lists:map(
			fun(I) ->
			    D = utils:unhex(erlang:list_to_binary(filename:basename(filename:dirname(I)))),
			    [[" \""], unicode:characters_to_list(D), ["/\""]]
			end, lists:droplast(SrcIndexPaths));
	    false -> [" "]
	end,
    CopiedObjects1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], [element(2, I) || I <- CopiedObjects0]),
    CopiedObjects2 = lists:map(
	fun(I) ->
	    case proplists:get_value(src_orig_name, I) =:= proplists:get_value(dst_orig_name, I) of
		true -> [[" \""], unicode:characters_to_list(proplists:get_value(dst_orig_name, I)), "\""];
		false -> [[" \""], unicode:characters_to_list(proplists:get_value(src_orig_name, I)), ["\""],
			  [" as \""], unicode:characters_to_list(proplists:get_value(dst_orig_name, I)), ["\""]]
	    end
	end, [J || J <- CopiedObjects1, proplists:get_value(src_prefix, J) =:= filename:dirname(SrcIndexPath)]),
    User = proplists:get_value(user, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="copy",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()])
    },
    SrcPrefix1 =
	case SrcPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(SrcPrefix0)) ++ ["/"]
	end,
    Summary0 = lists:flatten([["Copied"], CopiedDirectories ++ CopiedObjects2,
			     ["\" from \"", SrcPrefix1, "\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(DstBucketId, DstPrefix0, ActionLogRecord1),
    DstPrefix1 =
	case DstPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(DstPrefix0))++["/"]
	end,
    Summary1 = lists:flatten([["Copied"], CopiedDirectories ++ CopiedObjects2,
			     [" to \""], [DstPrefix1, "\"."]]),
    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
    action_log:add_record(SrcBucketId, SrcPrefix0, ActionLogRecord2),

    StatusCodes = [element(1, C) || C <- CopiedObjects0],
    StatusCode1 =
	case lists:any(fun(C) -> C =/= 200 end, StatusCodes) of
	    true -> 200;
	    false -> 202
	end,
    Req1 = cowboy_req:reply(StatusCode1, #{
	<<"content-type">> => <<"application/json">>
    }, jsx:encode(CopiedObjects1), Req0),
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
    case utils:check_token(Req0) of
	undefined -> {{false, <<"Token">>}, Req0, []};
	not_found -> {{false, <<"Token">>}, Req0, []};
	expired -> {{false, <<"Token">>}, Req0, []};
	User -> {true, Req0, [{user, User}]}
    end.

copy_forbidden(Req0, State) ->
    SrcBucketId =
	case cowboy_req:binding(src_bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    User = proplists:get_value(user, State),
    TenantId = User#user.tenant_id,
    UserBelongsToGroup =
	case utils:is_valid_bucket_id(SrcBucketId, TenantId) of
	    true -> lists:any(fun(Group) ->
		    utils:is_bucket_belongs_to_group(SrcBucketId, TenantId, Group#group.id) end,
		    User#user.groups);
	    false -> false
	end,
    case UserBelongsToGroup of
	true ->
	    {ok, Body, Req1} = cowboy_req:read_body(Req0),
	    case jsx:is_json(Body) of
		{error, badarg} -> js_handler:bad_request(Req1, 21);
		false -> js_handler:bad_request(Req1, 21);
		true ->
		    FieldValues = jsx:decode(Body),
		    DstBucketId0 =
			case proplists:get_value(<<"dst_bucket_id">>, FieldValues) of
			    undefined -> undefined;
			    DstBucketId1 -> unicode:characters_to_list(DstBucketId1)
			end,
		    User = proplists:get_value(user, State),
		    TenantId = User#user.tenant_id,
		    DstBucketCanBeModified =
			case utils:is_valid_bucket_id(DstBucketId0, TenantId) of
			    true ->
				lists:any(fun(Group) ->
				    utils:is_bucket_belongs_to_group(DstBucketId0, TenantId, Group#group.id) end,
				    User#user.groups);
			    false -> false
			end,
		    case DstBucketCanBeModified of
			false ->
			    Req2 = cowboy_req:set_resp_body(jsx:encode([{error, 26}]), Req1),
			    {true, Req2, []};
			true -> {false, Req1, [
				    {user, User},
				    {src_bucket_id, SrcBucketId},
				    {dst_bucket_id, DstBucketId0},
				    {src_prefix, proplists:get_value(<<"src_prefix">>, FieldValues)},
				    {dst_prefix, proplists:get_value(<<"dst_prefix">>, FieldValues)},
				    {src_object_keys, proplists:get_value(<<"src_object_keys">>, FieldValues)}
				]}
		    end
	    end;
	false ->
	    Req3 = cowboy_req:set_resp_body(jsx:encode([{error, 27}]), Req0),
	    {true, Req3, []}
    end.

%%
%% Checks if user has access
%% - To source bucket
%% - To destination bucket
%%
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    copy_forbidden(Req0, State).
