-module(list_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2,
    forbidden/2, resource_exists/2, previously_existed/2]).

-include("riak.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

get_riak_object(ObjInfo) ->
    Metadata = proplists:get_value(metadata, ObjInfo),
    ObjectName = proplists:get_value(key, ObjInfo),
    OrigName = proplists:get_value("x-amz-meta-orig-filename", Metadata, ObjectName),
    LastModified = proplists:get_value(last_modified_timestamp, ObjInfo, ""),
    TimeStamp = calendar:datetime_to_gregorian_seconds(LastModified) - 62167219200,
    Bytes =
	case proplists:get_value(content_length, Metadata, undefined) of
	    undefined -> 0;
	    Value -> utils:to_integer(Value)
	end,
    ContentType = proplists:get_value(content_type, Metadata, 0),
    Etag = proplists:get_value(etag, Metadata, ""),
    Md5 = string:strip(Etag, both, $"),

    [{name, ObjectName},
     {orig_name,  unicode:characters_to_list(list_to_binary(OrigName))},
     {bytes, Bytes},
     {content_type, ContentType},
     {last_modified, TimeStamp},
     {md5, Md5}].

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

to_json(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),
    ParsedQs = cowboy_req:parse_qs(Req0),
    RiakResponse =
	case proplists:get_value(<<"prefix">>, ParsedQs) of
	    undefined -> riak_api:list_objects(BucketName);
	    Prefix0 ->
		case utils:is_valid_hex_prefix(Prefix0) of
		    true ->
			Prefix1 = binary_to_list(unicode:characters_to_binary(Prefix0)),
			riak_api:list_objects(BucketName, [{prefix, Prefix1}]);
		    false ->
			[{contents, []}, {common_prefixes, []}]
		end
	end,
    case RiakResponse of
	not_found ->
	    case (riak_api:head_bucket(BucketName) =:= not_found
		    andalso utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName)) of
    		true ->
		    riak_api:create_bucket(BucketName);
    		false -> ok
	    end,
	    {jsx:encode([{list, []}, {dirs, []}]), Req0, []};
	_ ->
	    Contents = proplists:get_value(contents, RiakResponse),
	    MetadataList = [
		[{
		    metadata, riak_api:get_object_metadata(BucketName, proplists:get_value(key, ObjInfo))
		},
		{
		    last_modified_timestamp, proplists:get_value(last_modified, ObjInfo)
		},
		{
		    key, proplists:get_value(key, ObjInfo)
		}] || ObjInfo <- Contents,
		lists:suffix(?RIAK_INDEX_FILENAME, proplists:get_value(key, ObjInfo)) =/= true
	    ],
	    Output = jsx:encode([
		{list, [get_riak_object(ObjInfo) || ObjInfo <- MetadataList]},
		{dirs, [proplists:get_value(prefix, I) || I <- proplists:get_value(common_prefixes, RiakResponse)]}
	    ]),
	    {Output, Req0, []}
    end.

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
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),

    case utils:is_valid_bucket_name(BucketName, TenantName) of
	true ->
	    {true, Req0, [{user_name, UserName},
			  {tenant_name, TenantName},
			  {bucket_name, BucketName}]};
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
