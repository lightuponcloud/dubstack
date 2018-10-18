%%
%% Allows to move objects and pseudo-directories.
%%
-module(move_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, is_authorized/2, forbidden/2,
	 handle_post/2, update_pseudo_directory_index/6]).

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
	    SrcBucketId = proplists:get_value(src_bucket_id, State0),
	    DstBucketId = proplists:get_value(dst_bucket_id, State0),
	    SrcPrefix0 = proplists:get_value(src_prefix, State0),
	    DstPrefix0 = proplists:get_value(dst_prefix, State0),
	    SrcObjectNames0 = proplists:get_value(src_object_names, State0),
	    User = proplists:get_value(user, State0),

	    case copy_handler:validate_post(SrcPrefix0, DstPrefix0, SrcObjectNames0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		State1 ->
		    State2 = State1 ++ [
			{src_bucket_id, SrcBucketId},
			{dst_bucket_id, DstBucketId},
			{user, User}
		    ],
		    move(Req0, State2)
	    end;
	_ -> js_handler:bad_request(Req0, 16)
    end.

copy_delete(SrcBucketId, DstBucketId, ObjectName, SrcPrefix, DstPrefix) ->
    CopiedNames0 = copy_handler:do_copy(SrcBucketId, DstBucketId, ObjectName, SrcPrefix, DstPrefix),
    %% Delete regular object
    riak_api:delete_object(SrcBucketId, ObjectName),
    CopiedNames0.

%%
%% Copies renaming map from the old pseudo-directory to the new directory.
%% Updates indices in new pseudo-directory.
%% Deletes old pseudo-directory index.
%%
update_pseudo_directory_index(SrcBucketId, DstBucketId, PrefixedSrcObjectName,
	PrefixedDstObjectName, PrefixedDstDirectoryName, CopiedNames) ->
    copy_handler:update_pseudo_directory_index(SrcBucketId, DstBucketId,
	PrefixedSrcObjectName, PrefixedDstObjectName, PrefixedDstDirectoryName, CopiedNames),
    %% Delte old pseudo-directory index object
    riak_api:delete_object(SrcBucketId, PrefixedSrcObjectName),
    ok.

move(Req0, State) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State),
    SrcPrefix0 = proplists:get_value(src_prefix, State),
    SrcObjectNames = proplists:get_value(src_object_names, State),
    DstBucketId = proplists:get_value(dst_bucket_id, State),
    DstPrefix0 = proplists:get_value(dst_prefix, State),
    ObjectNamesToMove0 = lists:map(
       fun(N) ->
	    case utils:ends_with(N, <<"/">>) of
		true ->
		    ON = string:to_lower(erlang:binary_to_list(N)),  %% lowercase hex prefix
		    riak_api:recursively_list_pseudo_dir(SrcBucketId, utils:prefixed_object_name(SrcPrefix0, ON));
		false -> [utils:prefixed_object_name(SrcPrefix0, erlang:binary_to_list(N))]
	    end
       end, SrcObjectNames),
    ObjectNamesToMove1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], ObjectNamesToMove0),

    MovedNames0 = [copy_delete(SrcBucketId, DstBucketId, PrefixedObjectName, SrcPrefix0, DstPrefix0)
	|| PrefixedObjectName <- ObjectNamesToMove1,
	lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectName) =/= true],

    % Update indices for pseudo-sub-directories
    List0 = [update_pseudo_directory_index(SrcBucketId, DstBucketId, PrefixedObjectName,
		copy_handler:shorten_pseudo_directory_name(PrefixedObjectName, SrcPrefix0, DstPrefix0),
		DstPrefix0,
               [{proplists:get_value(old_name, CN), proplists:get_value(new_name, CN)}
                   || CN <- MovedNames0, proplists:get_value(dst_prefix, CN) =:= DstPrefix0]
	     ) || PrefixedObjectName <- ObjectNamesToMove1,
	     lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectName) =:= true],

    %% If no directories moved, just update indices.
    %% Code below saves a lot of requests to Riak CS.
    List1 =
	case length(List0) of
	    0 ->
		%% No sub-directories were moved.
		CopiedNames1 = [{proplists:get_value(old_name, IN), proplists:get_value(new_name, IN)}
                   || IN <- MovedNames0],
		riak_index:update(DstBucketId, DstPrefix0,
                   [{copy_from, [{bucket_id, SrcBucketId}, {prefix, SrcPrefix0},
                                 {copied_names, CopiedNames1}]}]);
	    _ -> []
	end,
    %% Update source directory index
    riak_index:update(SrcBucketId, SrcPrefix0),
    %%
    %% Add record to action log
    %%
    User = proplists:get_value(user, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="move",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()])
    },
    SrcPrefix1 =
	case SrcPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(SrcPrefix0)) ++ ["/"]
	end,
    ReadableList0 = lists:map(
       fun(N) ->
	    case utils:ends_with(N, <<"/">>) of
               true -> unicode:characters_to_list(utils:unhex(N)) ++ ["/"];
               false ->
                   [unicode:characters_to_list(proplists:get_value(orig_name, R))
                    || R <- List1, proplists:get_value(object_name, R) =:= N]

           end
       end, SrcObjectNames),
    Summary0 = lists:flatten([["Moved \""], [ReadableList0], ["\" from \"", SrcPrefix1, "\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(DstBucketId, DstPrefix0, ActionLogRecord1),
    DstPrefix1 =
	case DstPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(DstPrefix0))++["/"]
	end,
    Summary1 = lists:flatten([["Moved \""], [ReadableList0], ["\" to \""], [DstPrefix1, "\"."]]),
    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
    action_log:add_record(SrcBucketId, SrcPrefix0, ActionLogRecord2),
    {true, Req0, []}.

%%
%% Serializes response to json
%%
to_json(Req0, State) ->
    {"{\"status\": \"ok\"}", Req0, State}.

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

%%
%% Checks if user has access
%% - To source bucket
%% - To destination bucket
%%
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    SrcBucketId = erlang:binary_to_list(cowboy_req:binding(src_bucket_id, Req0)),
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
				    {src_object_names, proplists:get_value(<<"src_object_names">>, FieldValues)}
				]}
		    end
	    end;
	false ->
	    Req3 = cowboy_req:set_resp_body(jsx:encode([{error, 27}]), Req0),
	    {true, Req3, []}
    end.
