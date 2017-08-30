-module(object_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2,
    forbidden/2, resource_exists/2, previously_existed/2]).

-include("riak.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

to_json(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    ParsedQs = cowboy_req:parse_qs(Req0),
    ObjectName0 = proplists:get_value(<<"object-name">>, ParsedQs),
    Output =
	case proplists:get_value(<<"prefix">>, ParsedQs) of
	    undefined ->
		riak_api:head_object(BucketName, ObjectName0);
	    Prefix0 ->
		case utils:is_valid_hex_prefix(Prefix0) of
		    true ->
			Prefix1 = binary_to_list(unicode:characters_to_binary(Prefix0)),
			ObjectName1 = binary_to_list(unicode:characters_to_binary(ObjectName0)),
			RiakResponse = riak_api:head_object(BucketName,
			    utils:prefixed_object_name(Prefix1, ObjectName1)),
			LastModified = proplists:get_value(last_modified, RiakResponse),
			TimeStamp = calendar:datetime_to_gregorian_seconds(LastModified) - 62167219200,
			[
			    prefix, Prefix1,
			    key, ObjectName1,
			    orig_name, proplists:get_value("x-amz-meta-orig-filename", RiakResponse),
			    last_modified, TimeStamp,
			    content_length, proplists:get_value(content_length, RiakResponse)
			];
		    false -> []
		end
	end,
	{jsx:encode(Output), Req0, []}.

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
	KeystoneAttrs ->
	    {false, Req0, KeystoneAttrs}
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    BucketName = binary_to_list(cowboy_req:binding(bucket_name, Req0)),
    TenantName = proplists:get_value(tenant_name, State),

    case (utils:is_valid_bucket_name(BucketName, TenantName)
	    andalso utils:is_bucket_belongs_to_tenant(BucketName, TenantName)) of
	true ->
	    {true, Req0, [{bucket_name, BucketName}]};
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
