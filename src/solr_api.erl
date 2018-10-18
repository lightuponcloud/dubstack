%%
%% This module sends data to Solr for indexation.
%%
-module(solr_api).
-behavior(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, start_link/0]).

-include("riak.hrl").
-include("solr.hrl").
-include("log.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, []}.

handle_cast(Message, State) ->
    BucketId = proplists:get_value(bucket_id, Message),
    Prefix = proplists:get_value(prefix, Message),
    ObjectName = proplists:get_value(object_name, Message),
    TotalBytes = proplists:get_value(total_bytes, Message),
    ContentType = get_content_type(ObjectName),
    case ContentType of
	undefined -> ok;
	_ ->
	    case (TotalBytes =< ?MAXIMUM_FILE_SIZE) of
		true ->
		    index(BucketId, Prefix, ObjectName, ContentType);
		false -> ok
	    end
    end,
    {noreply, State}.

handle_info(Info, State) ->
    ?WARN("unexpected info request ~p~n", [Info]),
    {noreply, State}.

handle_call(Req, _, S) ->
    ?WARN("unexpected call request ~p~n", [Req]),
    {noreply, S}.

code_change(_, S, _) ->
    {ok, S}.

terminate(_Reason, _S) ->
    ok.

-spec get_content_type(string()) -> boolean().

get_content_type(ObjectName) when erlang:is_list(ObjectName) ->
    case filename:extension(ObjectName) of
	[] -> undefined;
	Extension ->
	    LookupKey = string:substr(Extension, 2),
	    case proplists:get_value(LookupKey, ?SOLR_MIME_TYPES) of
		undefined -> undefined;
		ContentType -> ContentType
	    end
    end.

%%
%% Retrieves file from object storage and submits it to Solr's extractor
%%
index(BucketId, Prefix, ObjectName, ContentType) ->
    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    EncodedPrefixedObjectName = erlcloud_http:url_encode_loose(PrefixedObjectName),
    UniqueId = lists:flatten(io_lib:format("~s/~s", [BucketId, EncodedPrefixedObjectName])),
    ObjectURL = riak_api:get_object_url(BucketId, EncodedPrefixedObjectName),
    Config0 = #riak_api_config{},
    %% Download object from Riak CS
    Response = riak_api:request_httpc(ObjectURL, get, [{"content-type", "*/*"}], <<>>, Config0),
    case Response of
	{error, _} ->
	    {error, "Object not found"};
	{ok, {_Status, _Headers, RequestBody}} ->
	    Headers = [{"content-type", ContentType}],
	    %% Upload object to Solr
	    Config1 = #riak_api_config{s3_proxy_host=undefined, s3_proxy_port=undefined},
	    MetaData = riak_api:get_object_metadata(BucketId, PrefixedObjectName),
	    OrigName = 
		case erlcloud_http:url_encode(proplists:get_value("x-amz-meta-orig-filename", MetaData, ObjectName)) of
		    undefined -> ObjectName;
		    Name -> Name
		end,
	    SolrURL = lists:flatten(io_lib:format("http://127.0.0.1:~s~s/~s~s?wt=json&literal._yz_id=~s&literal.bucket_id=~s&literal.orig_name=~s&commit=true&resource.name=~s",
		[integer_to_list(?SOLR_PORT), ?SOLR_HOST_CONTEXT, ?SOLR_INDEX_NAME, "/update/extract", UniqueId, BucketId, OrigName, OrigName])),
	    case riak_api:request_httpc(SolrURL, post, Headers, RequestBody, Config1) of
		{ok, {_,_,ResponseBody}} ->
		    ?INFO("Solr response: ~p~n", [ResponseBody]);
		{error, {http_error, _, StatusLine,Body,_}} ->
		    ?INFO("Solr ~p: ~p~n", [StatusLine, Body]);
		_ ->
		    ?INFO("Solr request failed", [])
	    end
    end.
