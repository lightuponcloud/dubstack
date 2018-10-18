%%
%% Allows to copy objects and pseudo-directories.
%%
-module(copy_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, is_authorized/2, forbidden/2,
	 validate_post/3, handle_post/2, do_copy/5,
	 shorten_pseudo_directory_name/3, update_pseudo_directory_index/5,
	 update_pseudo_directory_index/6]).

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

-spec validate_post(list(), list(), list()) -> proplist()|{error, integer()}.

validate_post(SrcPrefix0, DstPrefix0, SrcObjectNames0) ->
    case erlang:is_list(SrcObjectNames0) of
	true ->
	    case (utils:is_valid_hex_prefix(SrcPrefix0)
		    andalso utils:is_valid_hex_prefix(DstPrefix0)) of
		true ->
		    SrcPrefix1 = utils:to_lower(SrcPrefix0),
		    DstPrefix1 = utils:to_lower(DstPrefix0),
		    SrcObjectNames1 =
			[N || N <- SrcObjectNames0,
			    utils:starts_with(DstPrefix0,
					      utils:prefixed_object_name(SrcPrefix0, N)) =:= false],
		    case length(SrcObjectNames1) =:= 0 orelse DstPrefix1 =:= SrcPrefix1 of
			true -> {error, 13};
			false ->
			    [{src_prefix, SrcPrefix1},
			     {src_object_names, SrcObjectNames1},
			     {dst_prefix, DstPrefix1}]
		    end;
		false -> {error, 14}
	    end;
	false -> {error, 15}
    end.

%%
%% Validates POST request and sends request to Riak CS to copy object or pseudo-directory
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
	    case validate_post(SrcPrefix0, DstPrefix0, SrcObjectNames0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		State1 ->
		    State2 = State1 ++ [
			{src_bucket_id, SrcBucketId},
			{dst_bucket_id, DstBucketId},
			{user, User}
		    ],
		    copy(Req0, State2)
	    end;
	_ -> js_handler:bad_request(Req0, 16)
    end.

%%
%% Adjusts prefix and object name of object.
%% Returns object name that do not yet exist in destination pseudo-directory
%%
shorten_prefix(BucketId, ObjectName0, SrcPrefix0, DstPrefix0) ->
    case SrcPrefix0 of
	undefined ->
	    %% Copying from root directory
	    ObjectName1 = utils:prefixed_object_name(DstPrefix0, ObjectName0),
	    DstPrefix1 =
		case filename:dirname(ObjectName1) of
		    "." -> undefined;
		    DstPrefix2 -> DstPrefix2 ++ "/"
		end,
	    ObjectName2 = riak_api:pick_object_name(BucketId, DstPrefix1,
		erlang:list_to_binary(filename:basename(ObjectName1))),
	    utils:prefixed_object_name(DstPrefix1, ObjectName2);
	_ ->
	    ObjectName1 = re:replace(ObjectName0, "^" ++ SrcPrefix0, "", [{return, list}]),
	    DstPrefix3 =
		case filename:dirname(ObjectName1) of
		    "." -> DstPrefix0;
		    DstPrefix4 ->
			utils:prefixed_object_name(DstPrefix0, DstPrefix4 ++ "/")
		end,
	    ObjectName2 = riak_api:pick_object_name(BucketId, DstPrefix3,
		erlang:list_to_binary(filename:basename(ObjectName1))),
	    utils:prefixed_object_name(DstPrefix3, ObjectName2)
    end.

%%
%% Removes part of source prefix
%% Returns directory name that do not yet exist in destination pseudo-directory
%%
shorten_pseudo_directory_name(ObjectName0, SrcPrefix0, DstPrefix) ->
    ObjectName1 =
	case SrcPrefix0 of
	    undefined -> ObjectName0;
	    SrcPrefix1 ->
		re:replace(ObjectName0, "^" ++ SrcPrefix1, "", [{return, list}])
	end,
    utils:prefixed_object_name(DstPrefix, ObjectName1).

%%
%% Copies renaming map from the old pseudo-directory to the new directory.
%% Updates indices in new pseudo-directory.
%%
%% Returns list of objects in destination directory ( contains renamed objects map )
%%
-spec update_pseudo_directory_index(string(), string(), string(), string(), string()) -> proplist().

update_pseudo_directory_index(SrcBucketId, DstBucketId, PrefixedSrcObjectName,
	PrefixedDstObjectName, PrefixedDstDirectoryName) ->
    update_pseudo_directory_index(SrcBucketId, DstBucketId, PrefixedSrcObjectName,
	PrefixedDstObjectName, PrefixedDstDirectoryName, []).

update_pseudo_directory_index(SrcBucketId, DstBucketId, PrefixedSrcObjectName,
	PrefixedDstObjectName, PrefixedDstDirectoryName, CopiedNames0) ->
    SrcObjectPrefix =
	case filename:dirname(PrefixedSrcObjectName) of
	    "." -> undefined;
	    P0 -> P0++"/"
	end,
    DstObjectPrefix =
	case filename:dirname(PrefixedDstObjectName) of
	    "." -> undefined;
	    P1 -> P1++"/"
	end,
    CopiedNames1 = [{filename:basename(element(1, CN)), filename:basename(element(2, CN))}
		    || CN <- CopiedNames0],
    %% Update index for copied directory ( nested one )
    riak_index:update(DstBucketId, DstObjectPrefix,
		[{copy_from, [{bucket_id, SrcBucketId},
		 {prefix, SrcObjectPrefix},
		 {copied_names, CopiedNames1}]}]),
    %% Update destination directory
    riak_index:update(DstBucketId, PrefixedDstDirectoryName),
    ok.

%%
%% Copies object and returns 
%%
%% {previous object name with new prefix: new object name without prefix}
%%
-spec do_copy(string(), string(), string(), string(), string()) -> list().

do_copy(SrcBucketId, DstBucketId, PrefixedObjectName, SrcPrefix, DstPrefix) ->
    %% short prefix is used to update index ( list of objects )
    ShortPrefixedObjectName = shorten_prefix(DstBucketId, PrefixedObjectName, SrcPrefix, DstPrefix),
    %% TODO: check if user has access to DST bucket
    %%       add error handling in case some objects were not copied
    %%       restrict READ access
    _CopyResult = riak_api:copy_object(DstBucketId, ShortPrefixedObjectName, SrcBucketId,
	PrefixedObjectName, [{acl, public_read}]),
    %% TODO: report unsuccessful status of copy: 
    %% proplists:get_value(content_length, CopyResult, 0) == 0
    [
	{dst_prefix, DstPrefix},
	{old_name, filename:basename(PrefixedObjectName)},
	{new_name, filename:basename(ShortPrefixedObjectName)}
    ].

copy(Req0, State) ->
    SrcBucketId = proplists:get_value(src_bucket_id, State),
    SrcPrefix0 = proplists:get_value(src_prefix, State),
    SrcObjectNames = proplists:get_value(src_object_names, State),
    DstBucketId = proplists:get_value(dst_bucket_id, State),
    DstPrefix0 = proplists:get_value(dst_prefix, State),
    ObjectNamesToCopy0 = lists:map(
	fun(N) ->
	    case utils:ends_with(N, <<"/">>) of
		true ->
		    ON = string:to_lower(erlang:binary_to_list(N)),  %% lowercase hex prefix
		    riak_api:recursively_list_pseudo_dir(SrcBucketId, utils:prefixed_object_name(SrcPrefix0, ON));
		false -> [utils:prefixed_object_name(SrcPrefix0, erlang:binary_to_list(N))]
	    end
	end, SrcObjectNames),
    ObjectNamesToCopy1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], ObjectNamesToCopy0),
    CopiedNames0 = [do_copy(SrcBucketId, DstBucketId, PrefixedObjectName, SrcPrefix0, DstPrefix0)
	|| PrefixedObjectName <- ObjectNamesToCopy1, utils:is_hidden_object([{key, PrefixedObjectName}]) =/= true],
    %% Update indices for pseudo-sub-directories
    %% Some of objects might have been renamed,
    %% therefore rename map should be updated in index file.
    List0 = [update_pseudo_directory_index(SrcBucketId, DstBucketId, PrefixedObjectName,
		shorten_pseudo_directory_name(PrefixedObjectName, SrcPrefix0, DstPrefix0),
		DstPrefix0,
		[{proplists:get_value(old_name, CN), proplists:get_value(new_name, CN)}
		    || CN <- CopiedNames0, proplists:get_value(dst_prefix, CN) =:= DstPrefix0]
	     ) || PrefixedObjectName <- ObjectNamesToCopy1,
	     lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectName) =:= true],

    %% If no directories copied, just update indices.
    List1 =
	case length(List0) of
	    0 ->
		CopiedNames1 = [{proplists:get_value(old_name, IN), proplists:get_value(new_name, IN)}
		    || IN <- CopiedNames0],
		riak_index:update(DstBucketId, DstPrefix0,
		    [{copy_from, [{bucket_id, SrcBucketId}, {prefix, SrcPrefix0},
				  {copied_names, CopiedNames1}]}]);
	    _ -> []
	end,
    %%
    %% Add action log record
    %%
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
    ReadableList0 = lists:map(
	fun(N) ->
	    case utils:ends_with(N, <<"/">>) of
		true -> unicode:characters_to_list(utils:unhex(N)) ++ ["/"];
		false ->
		    [unicode:characters_to_list(proplists:get_value(orig_name, R))
		     || R <- List1, proplists:get_value(object_name, R) =:= N]
	    end
	end, SrcObjectNames),
    Summary0 = lists:flatten([["Copied \""], [ReadableList0], ["\" from \"", SrcPrefix1, "\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(DstBucketId, DstPrefix0, ActionLogRecord1),
    DstPrefix1 =
	case DstPrefix0 of
	    undefined -> "/";
	    _ -> unicode:characters_to_list(utils:unhex_path(DstPrefix0))++["/"]
	end,
    Summary1 = lists:flatten([["Copied \""], [ReadableList0], ["\" to \""], [DstPrefix1, "\"."]]),
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
