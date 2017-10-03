-module(search_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2, allowed_methods/2, forbidden/2,
	 resource_exists/2, previously_existed/2]).

-include("riak.hrl").
-include("log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%% Returns Bucket Name, Prefix and Object Name
parse_url(RiakURL) ->
    FirstSlashOccurrence = string:chr(RiakURL, $/),
    LastSlashOccurrence = string:rchr(RiakURL, $/),
    BucketName = string:substr(RiakURL, 1, FirstSlashOccurrence-1),
    Prefix =
	case FirstSlashOccurrence =/= LastSlashOccurrence of
	    true ->
		string:substr(RiakURL, FirstSlashOccurrence+1, LastSlashOccurrence-FirstSlashOccurrence);
	    false ->
		undefined
	    end,
    ObjectName = string:substr(RiakURL, LastSlashOccurrence+1),
    [{bucket_name, BucketName}, {prefix, Prefix}, {object_name, ObjectName}].

get_riak_object(RiakURL) ->
    URLInfo = parse_url(RiakURL),
    BucketName = proplists:get_value(bucket_name, URLInfo),
    Prefix = proplists:get_value(prefix, URLInfo),
    ObjectName = proplists:get_value(object_name, URLInfo),

    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    Metadata = riak_api:get_object_metadata(BucketName, PrefixedObjectName),

    OrigName = proplists:get_value("x-amz-meta-orig-filename", Metadata, ObjectName),
    LastModified = proplists:get_value(last_modified, Metadata, ""),
    %% It would be better to parse string date, rather than making another query
    LastModifiedTimetamp = calendar:datetime_to_gregorian_seconds(ec_date:parse(LastModified)) - 62167219200,
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
     {last_modified, LastModifiedTimetamp},
     {md5, Md5}].

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.


search(BucketName, Term) ->
    search(BucketName, undefined, Term).
%% todo: take into account prefix and bucket name
search(_BucketName, _Prefix, Term0) ->
    Term1 = erlcloud_http:url_encode_loose(Term0),
    SolrURL = lists:flatten(io_lib:format("http://127.0.0.1:8093/internal_solr/binary_objects/select?q=~s&wt=json&indent=false", [Term1])),
    Config = #riak_api_config{s3_proxy_host=undefined, s3_proxy_port=undefined},
    case riak_api:request_httpc(SolrURL, get, [], <<>>, Config) of
	{ok, {_,_,ResponseBody}} ->
	    SearchResult = proplists:get_value(<<"response">>, jsx:decode(ResponseBody)),
	    RiakURLs = [binary_to_list(proplists:get_value(<<"_yz_id">>, I, <<"">>)) || I <- proplists:get_value(<<"docs">>, SearchResult)],
	    [get_riak_object(URL) || URL <- RiakURLs,
		utils:is_hidden_object(URL) =/= true];
	{error, {http_error, _, StatusLine,Body,_}} ->
	    ?WARN("Riak ~p: ~p~n", [StatusLine, Body]),
	    [];
	_ ->
	    ?WARN("Riak request failed", []),
	    []
    end.

to_json(Req0, State) ->
    BucketName = proplists:get_value(bucket_name, State),
    ParsedQs = cowboy_req:parse_qs(Req0),
    case proplists:get_value(<<"q">>, ParsedQs) of
	undefined ->
	    {[], Req0, State};
	Term ->
	    RiakResponse =
		case proplists:get_value(<<"prefix">>, ParsedQs) of
		    undefined ->
			search(BucketName, Term);
		    Prefix0 ->
			Prefix1 = binary_to_list(unicode:characters_to_binary(Prefix0)),
			case utils:is_valid_hex_prefix(Prefix1) of
			    true ->
				search(BucketName, Prefix1, Term);
			    false ->
				[]
			end
		end,
	    Output = jsx:encode([
		{list, RiakResponse},
		{dirs, []}
	    ]),
	    {Output, Req0, State}
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

    case string:sub_string(BucketName, 37, 68) =:= TenantName of
	true ->
	    {true, Req0, [{user_name, UserName},
			  {tenant_name, TenantName},
			  {bucket_name, BucketName}]};
	false ->
	    {false, Req0, []}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
