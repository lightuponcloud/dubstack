-module(riak_api).

-export([create_bucket/1, create_bucket/2,
         delete_bucket/1,
         head_bucket/1,
         list_objects/1, list_objects/2,
         copy_object/4, copy_object/5,
         delete_object/2,
         head_object/2, head_object/3,
         get_object_metadata/2, get_object_metadata/3,
         put_object/4, put_object/5,
         start_multipart/2, start_multipart/4,
         upload_part/5,
         complete_multipart/4,
         abort_multipart/3,
         get_object_url/2,
	 s3_xml_request/8,
	 validate_upload_id/4,
	 pick_object_name/3,
	 s3_request/8,
	 request_httpc/5,
	 recursively_list_pseudo_dir/2
        ]).

-include("riak.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-type s3_bucket_acl() :: private
                       | public_read
                       | public_read_write
                       | authenticated_read
                       | bucket_owner_read
                       | bucket_owner_full_control.

-define(XMLNS_S3, "http://s3.amazonaws.com/doc/2006-03-01/").
-define(LIST_OBJECTS_MAX_KEYS, 10000).
-define(DEFAULT_TIMEOUT, 10000).

-spec copy_object(string(), string(), string(), string()) -> proplist().

copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName) ->
    copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, []).

-spec copy_object(string(), string(), string(), string(), proplist()) -> proplist().
copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, Options) ->
    SrcVersion = case proplists:get_value(version_id, Options) of
                     undefined -> "";
                     VersionID -> ["?versionId=", VersionID]
                 end,
    RequestHeaders =
        [{"x-amz-copy-source", [SrcBucketName, $/, SrcKeyName, SrcVersion]},
         {"x-amz-metadata-directive", proplists:get_value(metadata_directive, Options)},
         {"x-amz-copy-source-if-match", proplists:get_value(if_match, Options)},
         {"x-amz-copy-source-if-none-match", proplists:get_value(if_none_match, Options)},
         {"x-amz-copy-source-if-unmodified-since", proplists:get_value(if_unmodified_since, Options)},
         {"x-amz-copy-source-if-modified-since", proplists:get_value(if_modified_since, Options)},
         {"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}],
    Config = #riak_api_config{timeout=480000},  % 480000 == 8 minutes
    {ok, {_Status, Headers, _Body}} = s3_request(put, DestBucketName, [$/|DestKeyName], "", [], <<>>, RequestHeaders, Config),
    [{content_length, proplists:get_value("content-length", Headers, "0")}].

-spec create_bucket(string()) -> ok.

create_bucket(BucketName) ->
    create_bucket(BucketName, private).

-spec create_bucket(string(), s3_bucket_acl()) -> ok.

create_bucket(BucketName0, ACL)
  when is_list(BucketName0), is_atom(ACL) ->
    Headers = case ACL of
                  private -> [];  %% private is the default
                  _       -> [{"x-amz-acl", encode_acl(ACL)}]
              end,
    POSTData = <<>>,
    BucketName1 = string:to_lower(BucketName0),
    s3_simple_request(put, BucketName1, "/", "", [], POSTData, Headers).

encode_acl(undefined)                 -> undefined;
encode_acl(private)                   -> "private";
encode_acl(public_read)               -> "public-read";
encode_acl(public_read_write)         -> "public-read-write";
encode_acl(authenticated_read)        -> "authenticated-read";
encode_acl(bucket_owner_read)         -> "bucket-owner-read";
encode_acl(bucket_owner_full_control) -> "bucket-owner-full-control".

-spec delete_bucket(string()) -> ok.

delete_bucket(BucketName) ->
    s3_simple_request(delete, BucketName, "/", "", [], <<>>, []).

-spec delete_object(string(), string()) -> proplist().

delete_object(BucketName, Key)
  when is_list(BucketName), is_list(Key) ->
    Config = #riak_api_config{},
    {ok, {_Status, Headers, _Body}} = s3_request(delete, BucketName, [$/|Key], "", [], <<>>, [], Config),
    Marker = proplists:get_value("x-amz-delete-marker", Headers, "false"),
    Id = proplists:get_value("x-amz-version-id", Headers, "null"),
    [{delete_marker, list_to_existing_atom(Marker)},
     {version_id, Id}].


-spec list_objects(string()) -> proplist().

list_objects(BucketName) ->
    list_objects(BucketName, []).

-spec list_objects(string(), proplist()) -> proplist().

list_objects(BucketName, Options)
  when is_list(BucketName), is_list(Options) ->
    Params = [{"delimiter", proplists:get_value(delimiter, Options, "/")},
              {"marker", proplists:get_value(marker, Options)},
              {"max-keys", proplists:get_value(max_keys, Options, ?LIST_OBJECTS_MAX_KEYS)},
              {"prefix", proplists:get_value(prefix, Options)}],
    Config = #riak_api_config{},
    case s3_xml_request(get, BucketName, "/", "", Params, <<>>, [], Config) of
	{ok, Doc} ->
	    Attributes = [{name, "Name", text},
                  {prefix, "Prefix", text},
                  {marker, "Marker", text},
                  {next_marker, "NextMarker", text},
                  {delimiter, "Delimiter", text},
                  {max_keys, "MaxKeys", integer},
                  {is_truncated, "IsTruncated", boolean},
                  {common_prefixes, "CommonPrefixes", fun extract_prefixes/1},
                  {contents, "Contents", fun extract_contents/1}],
	    erlcloud_xml:decode(Attributes, Doc);
	not_found ->
	    not_found
    end.

-spec recursively_list_pseudo_dir(string(), string()) -> list().

recursively_list_pseudo_dir(BucketName, Prefix) ->
    RiakResponse = riak_api:list_objects(BucketName, [{prefix, Prefix}]),
    Contents = [proplists:get_value(key, I) || I <- proplists:get_value(contents, RiakResponse)],
    Dirs = [proplists:get_value(prefix, I) || I <- proplists:get_value(common_prefixes, RiakResponse)],
    lists:foldl(fun(X, Acc) -> X ++ Acc end, Contents, [recursively_list_pseudo_dir(BucketName, P) || P <- Dirs]).

extract_prefixes(Nodes) ->
    Attributes = [{prefix, "Prefix", text}],
    [erlcloud_xml:decode(Attributes, Node) || Node <- Nodes].

extract_contents(Nodes) ->
    Attributes = [{key, "Key", text},
                  {last_modified, "LastModified", time},
                  {etag, "ETag", text},
                  {size, "Size", integer},
                  {storage_class, "StorageClass", text},
                  {owner, "Owner", fun extract_user/1}],
    [erlcloud_xml:decode(Attributes, Node) || Node <- Nodes].

extract_user([]) ->
    [];
extract_user([Node]) ->
    Attributes = [{id, "ID", optional_text},
                  {display_name, "DisplayName", optional_text},
                  {uri, "URI", optional_text}
                 ],
    erlcloud_xml:decode(Attributes, Node).

%%
%% head_bucket function is used to check if bucket exists
%%
-spec head_bucket(string()) -> proplist() | not_found.

head_bucket(BucketName) when is_list(BucketName) ->
    Config = #riak_api_config{},
    case s3_xml_request(get, BucketName, "/", "acl", [], <<>>, [], Config) of
	{ok, Doc} ->
	    Attributes = [{owner, "Owner", fun extract_user/1}],
	    erlcloud_xml:decode(Attributes, Doc);
	not_found ->
	    not_found
    end.

-spec head_object(string(), string()) -> proplist()|not_found.

head_object(BucketName, Key) ->
    head_object(BucketName, Key, []).

-spec head_object(string(), string(), proplist()) -> proplist()|not_found.

head_object(BucketName, Key, Options) when is_list(BucketName), is_list(Key) ->
    RequestHeaders = [{"Range", proplists:get_value(range, Options)},
                      {"If-Modified-Since", proplists:get_value(if_modified_since, Options)},
                      {"If-Unmodified-Since", proplists:get_value(if_unmodified_since, Options)},
                      {"If-Match", proplists:get_value(if_match, Options)},
                      {"If-None-Match", proplists:get_value(if_none_match, Options)},
                      {"x-amz-server-side-encryption-customer-algorithm", proplists:get_value(server_side_encryption_customer_algorithm, Options)},
                      {"x-amz-server-side-encryption-customer-key", proplists:get_value(server_side_encryption_customer_key, Options)},
                      {"x-amz-server-side-encryption-customer-key-md5", proplists:get_value(server_side_encryption_customer_key_md5, Options)}],
    Subresource = case proplists:get_value(version_id, Options) of
                      undefined -> "";
                      Version   -> ["versionId=", Version]
                  end,
    Config = #riak_api_config{},
    case s3_request(head, BucketName, [$/|Key], Subresource, [], <<>>, RequestHeaders, Config) of
	{ok, {_Status, Headers, _Body}} ->
	    [{last_modified, proplists:get_value("last-modified", Headers)},
	    {etag, proplists:get_value("etag", Headers)},
	    {content_length, proplists:get_value("content-length", Headers)},
	    {content_type, proplists:get_value("content-type", Headers)},
	    {content_encoding, proplists:get_value("content-encoding", Headers)},
	    {delete_marker, list_to_existing_atom(proplists:get_value("x-amz-delete-marker", Headers, "false"))},
	    {version_id, proplists:get_value("x-amz-version-id", Headers, "null")}|extract_metadata(Headers)];
	{error, _} -> not_found
    end.

%%
%% Returns unique object name for provided bucket and prefix
%%
-spec pick_object_name(string(), binary(), string()) -> string().

pick_object_name(BucketName, Prefix, FileName)
    when is_list(BucketName), is_binary(FileName),
         is_list(Prefix) orelse Prefix =:= undefined ->
    % Call slugify or pick a random name
    ObjectName =
	case utils:slugify_object_name(FileName) of
	    [] -> utils:slugify_object_name();
	    "." -> utils:slugify_object_name();
	    _ObjectName -> _ObjectName
	end,
    % check if object exists in storage
    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    Response = head_object(BucketName, PrefixedObjectName),
    case Response of
	not_found -> ObjectName;
	_ -> pick_object_name(BucketName, Prefix, utils:increment_filename(FileName))
    end.

-spec get_object_metadata(string(), string()) -> proplist().

get_object_metadata(BucketName, Key) ->
    get_object_metadata(BucketName, Key, []).

-spec get_object_metadata(string(), string(), proplist()) -> proplist().

get_object_metadata(BucketName, Key, Options) ->
    RequestHeaders = [{"If-Modified-Since", proplists:get_value(if_modified_since, Options)},
                      {"If-Unmodified-Since", proplists:get_value(if_unmodified_since, Options)},
                      {"If-Match", proplists:get_value(if_match, Options)},
                      {"If-None-Match", proplists:get_value(if_none_match, Options)}],
    Subresource = case proplists:get_value(version_id, Options) of
                      undefined -> "";
                      Version   -> ["versionId=", Version]
                  end,
    Config = #riak_api_config{},
    {ok, {_Status, Headers, _Body}} = s3_request(head, BucketName, [$/|Key], Subresource, [], <<>>, RequestHeaders, Config),

    [{last_modified, proplists:get_value("last-modified", Headers)},
     {etag, proplists:get_value("etag", Headers)},
     {content_length, proplists:get_value("content-length", Headers)},
     {content_type, proplists:get_value("content-type", Headers)},
     {content_encoding, proplists:get_value("content-encoding", Headers)},
     {delete_marker, list_to_existing_atom(proplists:get_value("x-amz-delete-marker", Headers, "false"))},
     {version_id, proplists:get_value("x-amz-version-id", Headers, "false")}|extract_metadata(Headers)].

extract_metadata(Headers) ->
    [{Key, Value} || {Key = "x-amz-meta-" ++ _, Value} <- Headers].

%%
%% put_object function should be used for uploading small objects
%%
-spec put_object(string(), string(), string(), binary(), proplist()) -> string().
put_object(BucketName, Prefix, ObjectName, BinaryData)
	when is_list(BucketName),
	     is_list(Prefix) orelse Prefix =:= undefined,
	     is_list(ObjectName), is_binary(BinaryData) ->
    Options = [{acl, public_read}],
    put_object(BucketName, Prefix, ObjectName, BinaryData, Options).

put_object(BucketName, Prefix, ObjectName, BinaryData, Options)
	when is_list(BucketName),
	     is_list(Prefix) orelse Prefix =:= undefined,
	     is_list(ObjectName), is_binary(BinaryData),
	     is_list(Options) ->
    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    MimeType = utils:mime_type(ObjectName),
    HTTPHeaders = [{"content-type", MimeType}],
	       %% TBD: {"x-riak-index-short-url", }

    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{"x-amz-meta-" ++ string:to_lower(MKey), MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    Config = #riak_api_config{},
    {ok, {_Status, _Headers, _Body}} = s3_request(put, BucketName, [$/|PrefixedObjectName], "", [],
                                  BinaryData, RequestHeaders, Config),
    PrefixedObjectName.

-spec get_object_url(string(), string()) -> string().

get_object_url(BucketName, Key) ->
    Config = #riak_api_config{},
    lists:flatten([Config#riak_api_config.s3_scheme, BucketName, ".", Config#riak_api_config.s3_host, port_spec(Config), "/", Key]).

-spec start_multipart(string(), string()) -> {ok, proplist()} | {error, any()}.
start_multipart(BucketName, Key)
  when is_list(BucketName), is_list(Key) ->
    start_multipart(BucketName, Key, [], []).

-spec start_multipart(string(), string(), proplist(), [{string(), string()}]) -> {ok, proplist()} | {error, any()}.
start_multipart(BucketName, Key, Options, HTTPHeaders)
  when is_list(BucketName), is_list(Key),
       is_list(Options), is_list(HTTPHeaders) ->

    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{"x-amz-meta-" ++ string:to_lower(MKey), MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    POSTData = <<>>,
    Config = #riak_api_config{},
    case s3_xml_request2(post, BucketName, [$/|Key], "uploads", [], POSTData, RequestHeaders, Config) of
        {ok, Doc} ->
            Attributes = [{uploadId, "UploadId", text}],
            {ok, erlcloud_xml:decode(Attributes, Doc)};
        Error ->
            Error
    end.

%%
%% Queries Riak CS on a subject of provided upload id.
%%
-spec validate_upload_id(string(), string(), string(), string()) -> {ok, proplist()} | not_found | {error, any()}.

validate_upload_id(BucketName, Prefix, ObjectName0, UploadId0)
	when is_list(BucketName), is_list(ObjectName0),
	     is_list(Prefix) orelse Prefix =:= undefined,
	     is_list(UploadId0) ->
    Config = #riak_api_config{},
    case s3_xml_request2(get, BucketName, [$/|ObjectName0], [], [{"uploadId", UploadId0}], <<>>, [], Config) of
        {ok, Doc} ->
            Attributes = [{uploadId, "UploadId", text}],
            [{uploadId, UploadId1}] = erlcloud_xml:decode(Attributes, Doc),
	    case UploadId1 =:= UploadId0 of
		true -> ok;
		false -> not_found
	    end;
	{error, {http_error,_,_,_,_}} -> not_found;
	{error, Reason} -> {error, Reason}
    end.

-spec upload_part(string(), string(), string(), integer(), iodata()) -> {ok, proplist()} | {error, any()}.
upload_part(BucketName, Key, UploadId, PartNumber, Value) ->
    upload_part(BucketName, Key, UploadId, PartNumber, Value, []).

-spec upload_part(string(), string(), string(), integer(), iodata(), [{string(), string()}]) -> {ok, proplist()} | {error, any()}.
upload_part(BucketName, Key, UploadId, PartNumber, Value, HTTPHeaders)
  when is_list(BucketName), is_list(Key), is_list(UploadId),
       is_integer(PartNumber),
       is_binary(Value),
       is_list(HTTPHeaders) ->
    POSTData = erlang:iolist_to_binary(Value),
    Config = #riak_api_config{},
    case s3_request(put, BucketName, [$/|Key], [], [{"uploadId", UploadId},
                    {"partNumber", integer_to_list(PartNumber)}],
                    POSTData, HTTPHeaders, Config) of
        {ok, {_Status, Headers, _Body}} ->
            {ok, [{etag, proplists:get_value("etag", Headers)}]};
        Error ->
            Error
    end.

-spec complete_multipart(string(), string(), string(), [{binary(), binary()}]) -> {ok, proplist()} | {error, any()}.
complete_multipart(BucketName, Key, UploadId, ETags)
  when is_list(BucketName), is_list(Key), is_list(UploadId), is_list(ETags) ->
    complete_multipart(BucketName, Key, UploadId, ETags, []).

-spec complete_multipart(string(), string(), string(), [{binary(), binary()}], [{string(), string()}]) -> ok | {error, any()}.
complete_multipart(BucketName, Key, UploadId, ETags, HTTPHeaders)
  when is_list(BucketName), is_list(Key), is_list(UploadId),
       is_list(ETags), is_list(HTTPHeaders) ->

    POSTData = list_to_binary(xmerl:export_simple([{'CompleteMultipartUpload',
                                                    [{'Part',
                                                      [{'PartNumber', [integer_to_list(Num)]},
                                                       {'ETag', [ETag]}] } || {Num, ETag} <- ETags]}], xmerl_xml)),
    Config = #riak_api_config{},
    case s3_request(post, BucketName, [$/|Key], [], [{"uploadId", UploadId}],
                     POSTData, HTTPHeaders, Config) of
        {ok, {_Status, _Headers, _Body}} ->
            ok;
        Error ->
            Error
    end.

-spec abort_multipart(string(), string(), string()) -> ok | {error, any()}.
abort_multipart(BucketName, Key, UploadId)
  when is_list(BucketName), is_list(Key), is_list(UploadId) ->
    abort_multipart(BucketName, Key, UploadId, [], []).

-spec abort_multipart(string(), string(), string(), proplist(), [{string(), string()}]) -> ok | {error, any()}.
abort_multipart(BucketName, Key, UploadId, Options, HTTPHeaders)
  when is_list(BucketName), is_list(Key), is_list(UploadId), is_list(Options),
       is_list(HTTPHeaders) ->
    Config = #riak_api_config{},
    case s3_request(delete, BucketName, [$/|Key], [], [{"uploadId", UploadId}],
                     <<>>, HTTPHeaders, Config) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

s3_simple_request(Method, Host, Path, Subresource, Params, POSTData, Headers) ->
    Config = #riak_api_config{},
    case s3_request(Method, Host, Path, Subresource, Params, POSTData, Headers, Config) of
        {ok, {_Status, _Headers, <<>>}} -> ok;
        {ok, {_Status, _Headers, Body}} ->
            XML = element(1,xmerl_scan:string(binary_to_list(Body))),
            case XML of
                #xmlElement{name='Error'} ->
                    ErrCode = erlcloud_xml:get_text("/Error/Code", XML),
                    ErrMsg = erlcloud_xml:get_text("/Error/Message", XML),
                    erlang:error({s3_error, ErrCode, ErrMsg});
                _ ->
                    ok
            end
    end.

s3_xml_request(Method, Host, Path, Subresource, Params, POSTData, Headers, Config) ->
    case s3_request(Method, Host, Path, Subresource, Params, POSTData, Headers, Config) of
	{ok, {_Status, _Headers, Body}} ->
	    XML = element(1,xmerl_scan:string(binary_to_list(Body))),
	    case XML of
    		#xmlElement{name='Error'} ->
        	    ErrCode = erlcloud_xml:get_text("/Error/Code", XML),
        	    ErrMsg = erlcloud_xml:get_text("/Error/Message", XML),
        	    erlang:error({s3_error, ErrCode, ErrMsg});
    		_ ->
        	    {ok, XML}
	    end;
	{error, {http_error, 404,_,_,_}} ->
	    not_found
    end.

-spec s3_request(atom(), string(), string(), string(), proplist(), binary(), proplist(), riak_api_config()) ->
    {ok, {proplist(), binary()}} | {atom(), {atom(), integer(), string(), binary(), proplist()}}.

s3_request(Method, Bucket, Path, Subresource, Params, POSTData, Headers, Config) ->
    %% s3_request returns {ok, Body} or {error, Reason}

    ContentType = proplists:get_value("content-type", Headers, ""),
    FParams = [Param || {_, Value} = Param <- Params, Value =/= undefined],
    FHeaders = [Header || {_, Val} = Header <- Headers, Val =/= undefined],

    QueryParams = case Subresource of
        "" ->
            FParams;
        _ ->
            [{Subresource, ""} | FParams]
    end,

    S3Host = Config#riak_api_config.s3_host,
    EscapedPath = erlcloud_http:url_encode_loose(Path),
    HostName = lists:flatten(
	[case Bucket of "" -> ""; _ -> [Bucket, $.] end, S3Host]),

    RequestHeaders = riak_crypto:sign_v4(
        Method, EscapedPath,
        [{"host", HostName} | FHeaders ],
        POSTData,
        "US",
        "s3", QueryParams),

    RequestURI = lists:flatten([
        Config#riak_api_config.s3_scheme,
        HostName, port_spec(Config),
        EscapedPath,
        case Subresource of "" -> ""; _ -> [$?, Subresource] end,
        if
            FParams =:= [] -> "";
            Subresource =:= "" ->
              [$?, erlcloud_http:make_query_string(FParams, no_assignment)];
            true ->
              [$&, erlcloud_http:make_query_string(FParams, no_assignment)]
        end]),

    {RequestHeaders2, RequestBody} = case Method of
                                         M when M =:= get orelse M =:= head orelse M =:= delete ->
                                             {RequestHeaders, <<>>};
                                         _ ->
                                             Headers2 = case lists:keyfind("content-type", 1, RequestHeaders) of
                                                            false ->
                                                                [{"content-type", ContentType} | RequestHeaders];
                                                            _ ->
                                                                RequestHeaders
                                                        end,
                                             {Headers2, POSTData}
                                     end,
    request_httpc(RequestURI, Method, RequestHeaders2, RequestBody, Config).


s3_xml_request2(Method, Host, Path, Subresource, Params, POSTData, Headers, Config) ->
    case s3_request(Method, Host, Path, Subresource, Params, POSTData, Headers, Config) of
        {ok, {_Status, _Headers, Body}} ->
            XML = element(1,xmerl_scan:string(binary_to_list(Body))),
            case XML of
                #xmlElement{name='Error'} ->
                    ErrCode = erlcloud_xml:get_text("/Error/Code", XML),
                    ErrMsg = erlcloud_xml:get_text("/Error/Message", XML),
                    {error, {s3_error, ErrCode, ErrMsg}};
                _ ->
                    {ok, XML}
            end;
        Error ->
            Error
    end.

port_spec(#riak_api_config{s3_port=80}) ->
    "";
port_spec(#riak_api_config{s3_port=Port}) ->
    [":", erlang:integer_to_list(Port)].

%% Guard clause protects against empty bodied requests from being
%% unable to find a matching httpc:request call.
request_httpc(URL, Method, Hdrs, <<>>, Config)
    when (Method =:= options) orelse 
         (Method =:= get) orelse 
         (Method =:= head) orelse 
         (Method =:= delete) orelse 
         (Method =:= trace) ->
    HdrsStr = [{utils:to_list(K), utils:to_list(V)} || {K, V} <- Hdrs],

    Timeout = get_timeout(Config),
io:fwrite("!!!!!!!!!!!!!!!!!!!! ~p~n", [Timeout]),
    maybe_set_proxy(Config),
    response_httpc(httpc:request(Method, {URL, HdrsStr},
	[{timeout, Timeout}], [{body_format, binary}]));

request_httpc(URL, Method, Hdrs0, Body, Config) ->
    Hdrs1 =
	case lists:keyfind("content-type", 1, Hdrs0) of
            false ->
		[{"content-type", "*/*"} | Hdrs0];
            _ ->
		Hdrs0
        end,
    Hdrs2 = [{utils:to_list(K), utils:to_list(V)} || {K, V} <- Hdrs1],
    Hdrs3 = [{"connection", "close"} | Hdrs2],

    {"content-type", ContentType} = lists:keyfind("content-type", 1, Hdrs2),
    Timeout = get_timeout(Config),
    maybe_set_proxy(Config),
    response_httpc(httpc:request(Method,
                                 {URL, Hdrs3,
                                  ContentType, Body},
                                 [{timeout, Timeout}],
                                 [{body_format, binary}])).

response_httpc({ok, {{_HTTPVer, Status, StatusLine}, Headers, Body}}) ->
    case Status of
	_Status when _Status >= 500
		orelse _Status == 400
		orelse _Status == 409
		orelse _Status == 404
		orelse _Status == 405 ->
	    {error, {http_error, _Status, StatusLine, Body, Headers}};
	_ ->
	    {ok, {{Status, StatusLine}, [{string:to_lower(H), V} || {H, V} <- Headers], Body}}
    end;
response_httpc({error, _} = Error) ->
    Error.

get_timeout(#riak_api_config{timeout = undefined}) ->
    ?DEFAULT_TIMEOUT;
get_timeout(#riak_api_config{timeout = Timeout}) ->
    Timeout.

httpc_reset_proxy() ->
  httpc_manager:set_options([{proxy, {undefined, []}}], httpc:profile_name(default)).

maybe_set_proxy(Config) ->
    ProxyHost = Config#riak_api_config.s3_proxy_host,
    ProxyPort = Config#riak_api_config.s3_proxy_port,
    case ProxyHost of
	undefined ->
	    httpc_reset_proxy();
	_ ->
	    case ProxyPort of
		undefined ->
		    httpc_reset_proxy();
		_ ->
		    httpc:set_options([{proxy, {{ProxyHost, ProxyPort}, ["localhost"]}}])
	    end
    end.
