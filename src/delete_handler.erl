-module(delete_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2, forbidden/2,
	 resource_exists/2, previously_existed/2]).

-include("riak.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

delete(_State, _BucketName, "/") ->
    ok;
delete(State, BucketName, ObjectName0) ->
    TenantName = proplists:get_value(tenant_name, State),
    case utils:is_valid_bucket_name(BucketName, TenantName) of
	true ->
	    case string:sub_string(ObjectName0, length(ObjectName0), length(ObjectName0)) =:= "/" of
		true ->
		    %% deleting pseudo-directory means deleting all files inside that directory
		    [riak_api:delete_object(BucketName, N) ||
			N <- riak_api:recursively_list_pseudo_dir(BucketName, ObjectName0)];
		_ ->
		    riak_api:delete_object(BucketName, ObjectName0)
	    end
    end.

to_json(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    PrefixedObjectName = proplists:get_value(object_name, State),
    delete(State, BucketName, PrefixedObjectName),
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
	    case proplists:get_value(<<"object_name">>, ParsedQs) of
		undefined ->
		    {false, Req0, []};
		ObjectName0 ->
		    ObjectName1 = binary_to_list(unicode:characters_to_binary(ObjectName0)),
		    {true, Req0, [{user_name, UserName},
				  {tenant_name, TenantName},
				  {bucket_name, BucketName},
				  {object_name, ObjectName1}]}
	    end;
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
