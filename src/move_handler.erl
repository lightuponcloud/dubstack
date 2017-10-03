-module(move_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, forbidden/2, error_response/2,
	 resource_exists/2, previously_existed/2,
	 validate_post/5, handle_post/2]).

-include("riak.hrl").
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

error_response(Req0, ErrorCode) ->
    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, ErrorCode}]), Req0),
    {true, Req1, []}.

validate_post(Req, UserName, TenantName, SrcBucketName, FieldValues) ->
    SrcPrefix = proplists:get_value(<<"src_prefix">>, FieldValues),
    DstBucketName0 =
	case proplists:get_value(<<"dst_bucket_name">>, FieldValues) of
	    undefined -> undefined;
	    DstBucketName1 ->
		binary_to_list(unicode:characters_to_binary(DstBucketName1))
	end,
    DstPrefix = proplists:get_value(<<"dst_prefix">>, FieldValues),
    SrcObjectNames0 = proplists:get_value(<<"src_object_names">>, FieldValues),
    case (utils:is_valid_bucket_name(DstBucketName0, TenantName)
	    andalso utils:is_bucket_belongs_to_user(DstBucketName0, UserName, TenantName)
	    andalso erlang:is_list(SrcObjectNames0)) of
	true ->
	    case (utils:is_valid_hex_prefix(SrcPrefix)
		    andalso utils:is_valid_hex_prefix(DstPrefix)) of
		true ->
		    SrcObjectNames1 = [binary_to_list(unicode:characters_to_binary(N)) || N <- SrcObjectNames0],
		    [
			{src_bucket_name, SrcBucketName},
			{src_prefix, SrcPrefix},
			{src_object_names, SrcObjectNames1},
			{dst_bucket_name, DstBucketName0},
			{dst_prefix, DstPrefix}
		    ];
		false ->
		    error_response(Req, 11)
	    end;
	false ->
	    error_response(Req, 7)
    end.

%%
%% Validates provided content range values and calls 'upload_to_riak()'
%%
handle_post(Req0, State0) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    {ok, Body, Req1} = cowboy_req:read_body(Req0),
	    UserName = proplists:get_value(user_name, State0),
	    TenantName = proplists:get_value(tenant_name, State0),
	    FieldValues = jsx:decode(Body),
	    SrcBucketName = proplists:get_value(src_bucket_name, State0),
	    case validate_post(Req1, UserName, TenantName, SrcBucketName, FieldValues) of
		{true, Req3, []} ->
		    {true, Req3, []};  % error
		State1 ->
		    State2 = State1 ++ State0,
		    move(Req1, State2)
	    end;
	_ ->
	    error_response(Req0, 2)
    end.

copy_delete(SrcBucketName, DstBucketName, ObjectName, SrcPrefix, DstPrefix) ->
    case utils:is_hidden_object([{key, ObjectName}]) of
	true ->
	    %% Delete index/action log object
	    riak_api:delete_object(SrcBucketName, ObjectName),
	    undefined;
	false ->
	    %% Delete regular object
	    CopiedObjectName = copy_handler:do_conditional_copy(
		SrcBucketName, DstBucketName, ObjectName, SrcPrefix, DstPrefix),
	    riak_api:delete_object(SrcBucketName, ObjectName),
	    CopiedObjectName
    end.

move(Req0, State) ->
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),
    SrcBucketName = proplists:get_value(src_bucket_name, State),
    SrcPrefix0 =
	case proplists:get_value(src_prefix, State) of
	    undefined -> undefined;
	    SrcPrefix1 ->
		binary_to_list(SrcPrefix1)
	end,
    SrcObjectNames = proplists:get_value(src_object_names, State),
    DstBucketName = proplists:get_value(dst_bucket_name, State),
    DstPrefix0 =
	case proplists:get_value(dst_prefix, State) of
	    undefined -> undefined;
	    DstPrefix1 -> binary_to_list(DstPrefix1)
	end,
    ObjectNamesToMove0 = [
     case string:sub_string(N, length(N), length(N)) =:= "/" of
	true -> riak_api:recursively_list_pseudo_dir(SrcBucketName, N);
	false -> [N]
     end || N <- SrcObjectNames],
    ObjectNamesToMove1 = lists:foldl(fun(X, Acc) -> X ++ Acc end, [], ObjectNamesToMove0),

    MovedObjectNames0 = [copy_delete(SrcBucketName, DstBucketName, ObjectName, SrcPrefix0, DstPrefix0) ||
	ObjectName <- ObjectNamesToMove1],
    MovedObjectNames1 = [O || O <- MovedObjectNames0, O =/= undefined],

    ActionLogRecord0 = #riak_action_log_record{
	action="move",
	user_name=UserName,
	tenant_name=TenantName,
	timestamp=io_lib:format("~p", [utils:timestamp()])
	},

    ReadableList0 = lists:flatten(utils:join_list_with_separator(MovedObjectNames1, ", ", [])),

    Summary0 = lists:flatten([["Moved from: "], [ReadableList0]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(DstBucketName, DstPrefix0, ActionLogRecord1),

    case SrcPrefix0 of
	undefined -> ok;
	_ ->
	    ReadableList1 = lists:flatten(["/"]++utils:join_list_with_separator(
		[DstBucketName, utils:unhex_path(DstPrefix0)], "/", [])),
	    Summary1 = lists:flatten([["Moved "], [ReadableList0], [" to: "], [ReadableList1]]),

	    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
	    action_log:add_record(SrcBucketName, SrcPrefix0, ActionLogRecord2)
    end,
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
%%
forbidden(Req0, _State) ->
    Token = case cowboy_req:binding(token, Req0) of
	undefined -> undefined;
	TokenValue -> binary_to_list(TokenValue)
    end,
    case keystone_api:check_token(Token) of
	not_found ->
	    {true, Req0, []};
	Ids ->
	    {false, Req0, Ids}
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    SrcBucketName = binary_to_list(cowboy_req:binding(src_bucket_name, Req0)),
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),

    case (utils:is_bucket_belongs_to_user(SrcBucketName, UserName, TenantName)
	    andalso utils:is_valid_bucket_name(SrcBucketName, TenantName)) of
	true ->
	    {true, Req0, State ++ [{src_bucket_name, SrcBucketName}]};
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
