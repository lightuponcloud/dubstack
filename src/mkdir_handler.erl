-module(mkdir_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 allowed_methods/2, to_json/2, forbidden/2, error_response/2,
	 create_pseudo_directory/2, validate_post/2, handle_post/2,
	 resource_exists/2, previously_existed/2]).

-include("riak.hrl").
-include("general.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Returns callback 'create_pseudo_directory()'
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
%% Serializes response to json
%%
to_json(Req0, State) ->
    {jsx:encode(State), Req0, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

error_response(Req0, ErrorCode) ->
    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, ErrorCode}]), Req0),
    {true, Req1, []}.

%%
%% Checks if directory name and prefix are correct
%%
validate_post(Req, FieldValues) ->
    Prefix0 = 
	case proplists:get_value(<<"prefix">>, FieldValues) of
	    undefined -> undefined;
	    Prefix ->
		case utils:is_valid_hex_prefix(Prefix) of
		    true ->
			binary_to_list(unicode:characters_to_binary(Prefix));
		    false -> undefined
		end
	end,

    case proplists:get_value(<<"object_name">>, FieldValues) of
	undefined ->
	    error_response(Req, 8);
	DirectoryName0 ->
	    case utils:is_valid_name(DirectoryName0) of
		true ->
		    DirectoryName1 = unicode:characters_to_binary(DirectoryName0),
		    HexDirectoryName = binary_to_list(utils:hex(DirectoryName1)),
		    PrefixedDirectoryName = utils:prefixed_object_name(
			Prefix0, HexDirectoryName),
		    {DirectoryName1, PrefixedDirectoryName};
		false ->
		    error_response(Req, 9)
	    end
    end.

%%
%% Slugifies object name, uploads ".riak_index.html" hidden object
%% and assigns it a metadata attribute with original object name.
%%
%% For example: "blah blah" is uploaded as blah-blah/.riak_index.html
%%
-spec create_pseudo_directory(any(), proplist()) -> any().

create_pseudo_directory(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    DirectoryName = proplists:get_value(directory_name, State),
    PrefixedDirectoryName = proplists:get_value(prefixed_directory_name, State),
    case riak_api:head_bucket(BucketName) of
    	not_found ->
	    riak_api:create_bucket(BucketName);
    	_ -> ok
	end,
	PrefixedIndexFilename = utils:prefixed_object_name(
	    PrefixedDirectoryName, ?RIAK_INDEX_FILENAME),
	ExistingIndexObject = riak_api:head_object(BucketName,
	    PrefixedIndexFilename),
	case ExistingIndexObject of
	    not_found ->
		riak_api:update_index(BucketName, PrefixedDirectoryName, DirectoryName),
		{true, Req0, []};
	    _ ->
		error_response(Req0, 10)
	end.

handle_post(Req0, State) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    {ok, Body, Req1} = cowboy_req:read_body(Req0),
	    FieldValues = jsx:decode(Body),
	    case validate_post(Req1, FieldValues) of
		{true, Req2, []} ->
		    {true, Req2, []};  % error
		{DirectoryName, PrefixedDirectoryName} ->
		    NewState = [{directory_name, DirectoryName},
				{prefixed_directory_name, PrefixedDirectoryName}
			       ] ++ State,
		    create_pseudo_directory(Req1, NewState)
	    end;
	_ ->
	    error_response(Req0, 2)
    end.

%%
%% Checks if provided token is correct
%% ( called after 'allowed_methods()' )
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

    case utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName)
	    andalso utils:is_valid_bucket_name(BucketName, TenantName) of
	true ->
	    {true, Req0, [{user_name, UserName},
			  {tenant_name, TenantName},
			  {bucket_name, BucketName}]};
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
