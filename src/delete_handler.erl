-module(delete_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2, forbidden/2,
	 resource_exists/2, previously_existed/2]).

-include("riak.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

delete(_State, _BucketName, "", "/") ->
    ok;
delete(State, BucketName, Prefix, ObjectName0) ->
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),

    case utils:is_valid_bucket_name(BucketName, TenantName) of
	true ->
	    PrefixedObjectName = case Prefix of
		undefined ->
		    ObjectName0;
		_ ->
		    utils:prefixed_object_name(Prefix, ObjectName0)
	    end,
	    ActionLogRecord0 = #riak_action_log_record{
		action="delete",
		user_name=UserName,
		tenant_name=TenantName,
		timestamp=io_lib:format("~p", [utils:timestamp()])
	    },

	    case string:sub_string(ObjectName0, length(ObjectName0), length(ObjectName0)) =:= "/" of
		true ->
		    %% deleting pseudo-directory means deleting all files inside that directory
		    [riak_api:delete_object(BucketName, N) || N <-
			riak_api:recursively_list_pseudo_dir(BucketName, PrefixedObjectName)],

		    UnicodeObjectName = utils:unhex_path(ObjectName0)++["/"],
		    Summary0 = lists:flatten([["Directory "], [UnicodeObjectName], [" was deleted"]]),
		    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
		    action_log:add_record(BucketName, Prefix, ActionLogRecord1);
		_ ->
		    %% Learn original name of object first
		    ObjectMeta = riak_api:head_object(BucketName, PrefixedObjectName),
		    %% Delete object
		    riak_api:delete_object(BucketName, PrefixedObjectName),

		    %% Leave record in action log using object's original name
		    case ObjectMeta of
			not_found ->
			    ok;
			_ ->
			    UnicodeObjectName0 = proplists:get_value("x-amz-meta-orig-filename", ObjectMeta),
			    UnicodeObjectName1 = unicode:characters_to_list(list_to_binary(UnicodeObjectName0)),
			    Summary0 = lists:flatten([["File "], [UnicodeObjectName1], [" was deleted"]]),
			    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
			    action_log:add_record(BucketName, Prefix, ActionLogRecord1)
		    end
	    end;
	false ->
	    ok
    end.

to_json(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    ObjectName = proplists:get_value(object_name, State),
    Prefix = proplists:get_value(prefix, State),
    delete(State, BucketName, Prefix, ObjectName),
    {"{\"status\": \"ok\"}", Req0, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

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
    BucketName = binary_to_list(cowboy_req:binding(bucket_name, Req0)),
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),

    case utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName) of
	true ->
	    ParsedQs = cowboy_req:parse_qs(Req0),
	    Prefix0 = case proplists:get_value(<<"prefix">>, ParsedQs) of
		undefined -> undefined;
		    Prefix1 ->
			case utils:is_valid_hex_prefix(Prefix1) of
			    true ->
				binary_to_list(unicode:characters_to_binary(Prefix1));
			    false ->
				undefined
			end
	    end,
	    case proplists:get_value(<<"object_name">>, ParsedQs) of
		undefined ->
		    {false, Req0, []};
		ObjectName0 ->
		    ObjectName1 = binary_to_list(unicode:characters_to_binary(ObjectName0)),
		    {true, Req0, [{user_name, UserName},
				  {tenant_name, TenantName},
				  {bucket_name, BucketName},
				  {prefix, Prefix0},
				  {object_name, ObjectName1}]}
	    end;
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
