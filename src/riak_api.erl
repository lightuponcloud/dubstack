-module(riak_api).

-export([create_bucket/1, create_bucket/2,
         delete_bucket/1,
         head_bucket/1,
         list_objects/1, list_objects/2,
         copy_object/4, copy_object/5,
         delete_object/2,
         get_object/2, head_object/2,
         get_object_metadata/2, get_object_metadata/3,
         put_object/4, put_object/5,
         start_multipart/2, start_multipart/4,
         upload_part/5,
         complete_multipart/4,
         abort_multipart/3,
         get_object_url/2,
	 s3_xml_request/8,
	 validate_upload_id/3,
	 increment_filename/1,
	 pick_object_key/5,
	 s3_request/8,
	 request_httpc/5,
	 recursively_list_pseudo_dir/2
        ]).

-include("riak.hrl").
-include("general.hrl").
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

-spec copy_object(DestBucketId, DestKeyName, SrcBucketId, SrcKeyName) -> proplist() when
    DestBucketId :: string(),
    DestKeyName :: string(),
    SrcBucketId :: string(),
    SrcKeyName :: string().

copy_object(DestBucketId, DestKeyName, SrcBucketId, SrcKeyName) ->
    copy_object(DestBucketId, DestKeyName, SrcBucketId, SrcKeyName, []).

-spec copy_object(DestBucketId, DestKeyName, SrcBucketId, SrcKeyName, Options) -> proplist() when
    DestBucketId :: string(),
    DestKeyName :: string(),
    SrcBucketId :: string(),
    SrcKeyName :: string(),
    Options :: proplist().

copy_object(DestBucketId, DestKeyName, SrcBucketId, SrcKeyName, Options) ->
    SrcVersion =
	case proplists:get_value(version_id, Options) of
            undefined -> "";
            VersionID -> ["?versionId=", VersionID]
        end,
    RequestHeaders =
        [{"x-amz-copy-source", [SrcBucketId, $/, SrcKeyName, SrcVersion]},
         {"x-amz-metadata-directive", proplists:get_value(metadata_directive, Options)},
         {"x-amz-copy-source-if-match", proplists:get_value(if_match, Options)},
         {"x-amz-copy-source-if-none-match", proplists:get_value(if_none_match, Options)},
         {"x-amz-copy-source-if-unmodified-since", proplists:get_value(if_unmodified_since, Options)},
         {"x-amz-copy-source-if-modified-since", proplists:get_value(if_modified_since, Options)},
         {"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}],
    Config = #riak_api_config{timeout=480000},  % 480000 == 8 minutes
    {ok, {_Status, Headers, _Body}} = s3_request(put, DestBucketId, [$/|DestKeyName], "", [], <<>>, RequestHeaders, Config),
    [{content_length, proplists:get_value("content-length", Headers, "0")}].

-spec create_bucket(string()) -> ok.

create_bucket(BucketId) ->
    create_bucket(BucketId, private).

-spec create_bucket(string(), s3_bucket_acl()) -> ok.

create_bucket(BucketId0, ACL) when erlang:is_list(BucketId0), erlang:is_atom(ACL) ->
    Headers = case ACL of
                  private -> [];  %% private is the default
                  _       -> [{"x-amz-acl", encode_acl(ACL)}]
              end,
    POSTData = <<>>,
    BucketId1 = string:to_lower(BucketId0),
    s3_simple_request(put, BucketId1, "/", "", [], POSTData, Headers).

encode_acl(undefined)                 -> undefined;
encode_acl(private)                   -> "private";
encode_acl(public_read)               -> "public-read";
encode_acl(public_read_write)         -> "public-read-write";
encode_acl(authenticated_read)        -> "authenticated-read";
encode_acl(bucket_owner_read)         -> "bucket-owner-read";
encode_acl(bucket_owner_full_control) -> "bucket-owner-full-control".

-spec delete_bucket(string()) -> ok.

delete_bucket(BucketId) ->
    s3_simple_request(delete, BucketId, "/", "", [], <<>>, []).

-spec delete_object(string(), string()) -> proplist().

delete_object(BucketId, Key) when erlang:is_list(BucketId), erlang:is_list(Key) ->
    Config = #riak_api_config{},
    {ok, {_Status, Headers, _Body}} = s3_request(delete, BucketId, [$/|Key], "", [], <<>>, [], Config),
    Marker = proplists:get_value("x-amz-delete-marker", Headers, "false"),
    Id = proplists:get_value("x-amz-version-id", Headers, "null"),
    [{delete_marker, list_to_existing_atom(Marker)},
     {version_id, Id}].


-spec list_objects(string()) -> proplist().

list_objects(BucketId) ->
    list_objects(BucketId, []).

-spec list_objects(string(), proplist()) -> proplist().

list_objects(BucketId, Options) when erlang:is_list(BucketId), erlang:is_list(Options) ->
    Params = [{"delimiter", proplists:get_value(delimiter, Options, "/")},
              {"marker", proplists:get_value(marker, Options)},
              {"max-keys", proplists:get_value(max_keys, Options, ?LIST_OBJECTS_MAX_KEYS)},
              {"prefix", proplists:get_value(prefix, Options)}],
    Config = #riak_api_config{},
    case s3_xml_request(get, BucketId, "/", "", Params, <<>>, [], Config) of
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
	not_found -> not_found
    end.

-spec recursively_list_pseudo_dir(string(), string()) -> list().

recursively_list_pseudo_dir(BucketId, Prefix) ->
    RiakResponse = list_objects(BucketId, [{prefix, Prefix}]),
    Contents = [proplists:get_value(key, I) || I <- proplists:get_value(contents, RiakResponse)],
    Dirs = [proplists:get_value(prefix, I) || I <- proplists:get_value(common_prefixes, RiakResponse)],
    lists:foldl(fun(X, Acc) -> X ++ Acc end, Contents, [recursively_list_pseudo_dir(BucketId, P) || P <- Dirs]).

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

head_bucket(BucketId) when erlang:is_list(BucketId) ->
    Config = #riak_api_config{},
    case s3_xml_request(get, BucketId, "/", "acl", [], <<>>, [], Config) of
	{ok, Doc} ->
	    Attributes = [{owner, "Owner", fun extract_user/1}],
	    erlcloud_xml:decode(Attributes, Doc);
	not_found -> not_found
    end.

-spec head_object(string(), string()) -> proplist()|not_found.

head_object(BucketId, Key) when erlang:is_list(BucketId), erlang:is_list(Key) ->
    fetch_object(head, BucketId, Key, []).

-spec get_object(string(), string()) -> proplist().

get_object(BucketId, Key) when erlang:is_list(BucketId), erlang:is_list(Key) ->
    fetch_object(get, BucketId, Key, []).

-spec fetch_object(MethodName, BucketId, Key, Options) -> proplist()|not_found when
    MethodName :: atom(),
    BucketId :: string(),
    Key :: string(),
    Options :: proplist().

fetch_object(MethodName, BucketId, Key, Options)
	when erlang:is_atom(MethodName), erlang:is_list(BucketId), erlang:is_list(Key) ->
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
    case s3_request(MethodName, BucketId, [$/|Key], Subresource, [], <<>>, RequestHeaders, Config) of
	{ok, {_Status, Headers, Body}} ->
	    [{last_modified, proplists:get_value("last-modified", Headers)},
	     {etag, proplists:get_value("etag", Headers)},
	     {content_length, proplists:get_value("content-length", Headers)},
	     {content_type, proplists:get_value("content-type", Headers)},
	     {content_encoding, proplists:get_value("content-encoding", Headers)},
	     {delete_marker, list_to_existing_atom(proplists:get_value("x-amz-delete-marker", Headers, "false"))},
	     {content, Body},
	     {version_id, proplists:get_value("x-amz-version-id", Headers, "null")}|extract_metadata(Headers)
	    ];
	{error, _} -> not_found
    end.

%%
%% Returns filename-N, where N is added integer number
%%
-spec increment_filename(binary()) -> binary().

increment_filename(FileName) when erlang:is_binary(FileName) ->
    {RootName, Extension} = {filename:rootname(FileName), filename:extension(FileName)},

    case binary:matches(RootName, <<"-">>) of
	[] -> << RootName/binary,  <<"-1">>/binary, Extension/binary >>;
	HyphenPositions ->
	    {LastHyphenPos, _} = lists:last(HyphenPositions),
	    V = binary:part(RootName, size(RootName), -(size(RootName)-LastHyphenPos-1)),
	    try binary_to_integer(V) of
		N ->
		    NamePart = binary:part(RootName, 0, LastHyphenPos+1),
		    IncrementedValue = integer_to_binary(N+1),
		    << NamePart/binary, IncrementedValue/binary, Extension/binary >>
	    catch error:badarg ->
		<< RootName/binary,  <<"-1">>/binary, Extension/binary >>
	    end
    end.

%%
%% Checks if version of existing object is older than new version.
%%
is_new_version(_OldVersion, undefined) -> true;
is_new_version(undefined, _NewVersion) -> false;
is_new_version(OldVersion, NewVersion) when OldVersion < NewVersion -> true;
is_new_version(_, _) -> false.

%%
%% Returns unique object name for provided bucket/prefix.
%% It takes into account existing pseudo-directory names.
%%
%% Returns the following tuple.
%%	{
%%	    unique object key,
%%	    unique original name,
%%	    whether new object is a previous version flag
%%	}
%%
-spec pick_object_key(BucketId, Prefix, FileName, ModifiedTime, IndexContent) -> {string(), binary(), boolean()}
    when
    BucketId :: string(),
    Prefix :: string(),
    FileName :: binary(),
    ModifiedTime :: integer()|undefined,
    IndexContent :: list().

pick_object_key(BucketId, Prefix, FileName, ModifiedTime, IndexContent)
    when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined,
	 erlang:is_binary(FileName),
	 erlang:is_integer(ModifiedTime) orelse ModifiedTime =:= undefined ->
    ExistingPrefixes = [proplists:get_value(prefix, P) || P <- proplists:get_value(dirs, IndexContent, [])],
    ExistingList = proplists:get_value(list, IndexContent, []),
    ExistingOrigNames = [proplists:get_value(orig_name, O) || O <- ExistingList],
    pick_object_key(BucketId, Prefix, FileName, ModifiedTime,
		     ExistingList, ExistingPrefixes, ExistingOrigNames, undefined, false).

pick_object_key(BucketId, Prefix, FileName0, ModifiedTime0,
		ExistingList, ExistingPrefixes, ExistingOrigNames, Uniq1, IsPreviousVersion0)
    when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined,
	 erlang:is_binary(FileName0),
	 erlang:is_integer(ModifiedTime0) orelse ModifiedTime0 =:= undefined,
	 erlang:is_list(ExistingList), erlang:is_list(ExistingPrefixes),
	 erlang:is_list(ExistingOrigNames), erlang:is_boolean(IsPreviousVersion0),
	 erlang:is_binary(Uniq1) orelse Uniq1 =:= undefined ->
    ObjectKey0 =
	case Uniq1 of
	    undefined -> FileName0;
	    _ -> Uniq1
	end,
    %% Transliterate filename
    ObjectKey1 =
	case utils:slugify_object_key(ObjectKey0) of
	    [] -> utils:hex(ObjectKey0);
	    "." -> utils:hex(ObjectKey0);
	    ".." -> utils:hex(ObjectKey0);
	    ON -> ON
	end,
    ObjectKey2 = erlang:list_to_binary(ObjectKey1),
    %% Check if key exists
    ExistingObject =
	lists:filter(fun(I) -> proplists:get_value(object_key, I) =:= ObjectKey2 end, ExistingList),
    case ExistingObject of
	[] ->
	    %% Check if pseudo-directory with this name exists
	    HexObjectKey0 = erlang:list_to_binary(utils:hex(ObjectKey0)),
	    HexObjectKey1 = << HexObjectKey0/binary, <<"/">>/binary >>,
	    case lists:member(HexObjectKey1, ExistingPrefixes) of
		true -> pick_object_key(BucketId, Prefix, FileName0, ModifiedTime0,
					 ExistingList, ExistingPrefixes, ExistingOrigNames,
					 increment_filename(ObjectKey0), IsPreviousVersion0);
		false ->
		    %% Check if orig_name exists, as object could have been renamed
		    case lists:member(ObjectKey0, ExistingOrigNames) of
			true -> pick_object_key(BucketId, Prefix, FileName0, ModifiedTime0,
						 ExistingList, ExistingPrefixes, ExistingOrigNames,
						 increment_filename(ObjectKey0), IsPreviousVersion0);
			false -> {ObjectKey1, ObjectKey0, IsPreviousVersion0}
		    end
	    end;
	[ObjectMeta] ->
	    IsNewVersion = is_new_version(proplists:get_value(last_modified_utc, ObjectMeta), ModifiedTime0),
	    case IsNewVersion of
		true -> {ObjectKey1, ObjectKey0, false};  %% Just replace existing object with its newer version
		false -> {ObjectKey1, ObjectKey0, true}
	    end
    end.

-spec get_object_metadata(string(), string()) -> proplist().

get_object_metadata(BucketId, Key) ->
    get_object_metadata(BucketId, Key, []).

-spec get_object_metadata(BucketId, Key, Options) -> proplist() when
    BucketId :: string(),
    Key :: string(),
    Options :: proplist().

get_object_metadata(BucketId, Key, Options)
	when erlang:is_list(BucketId), erlang:is_list(Key), erlang:is_list(Options) ->
    RequestHeaders = [{"If-Modified-Since", proplists:get_value(if_modified_since, Options)},
                      {"If-Unmodified-Since", proplists:get_value(if_unmodified_since, Options)},
                      {"If-Match", proplists:get_value(if_match, Options)},
                      {"If-None-Match", proplists:get_value(if_none_match, Options)}],
    Subresource =
	case proplists:get_value(version_id, Options) of
	    undefined -> "";
	    Version   -> ["versionId=", Version]
        end,
    Config = #riak_api_config{},
    {ok, {_Status, Headers, _Body}} = s3_request(head, BucketId, [$/|Key], Subresource, [], <<>>, RequestHeaders, Config),

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
-spec put_object(BucketId, Prefix, ObjectKey, BinaryData) -> string() when
    BucketId :: string(),
    Prefix :: string(),
    ObjectKey :: string(),
    BinaryData :: binary().

put_object(BucketId, Prefix, ObjectKey, BinaryData)
	when erlang:is_list(BucketId),
	     erlang:is_list(Prefix) orelse Prefix =:= undefined,
	     erlang:is_list(ObjectKey), erlang:is_binary(BinaryData) ->
    Options = [{acl, public_read}],
    put_object(BucketId, Prefix, ObjectKey, BinaryData, Options).

-spec put_object(BucketId, Prefix, ObjectKey, BinaryData, Options) -> string() when
    BucketId :: string(),
    Prefix :: string(),
    ObjectKey :: string(),
    BinaryData :: binary(),
    Options :: proplist().

put_object(BucketId, Prefix, ObjectKey, BinaryData, Options)
	when erlang:is_list(BucketId),
	     erlang:is_list(Prefix) orelse Prefix =:= undefined,
	     erlang:is_list(ObjectKey), erlang:is_binary(BinaryData),
	     erlang:is_list(Options) ->
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey),
    MimeType = utils:mime_type(ObjectKey),
    HTTPHeaders = [{"content-type", MimeType}],
	       %% TBD: {"x-riak-index-short-url", }

    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{"x-amz-meta-" ++ string:to_lower(MKey), MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    Config = #riak_api_config{},
    {ok, {_Status, _Headers, _Body}} = s3_request(put, BucketId, [$/|PrefixedObjectKey], "", [],
                                  BinaryData, RequestHeaders, Config),
    PrefixedObjectKey.

-spec get_object_url(string(), string()) -> string().

get_object_url(BucketId, Key) ->
    Config = #riak_api_config{},
    lists:flatten([Config#riak_api_config.s3_scheme, BucketId, ".", Config#riak_api_config.s3_host, port_spec(Config), "/", Key]).

-spec start_multipart(BucketId, Key) -> {ok, proplist()} | {error, any()} when
    BucketId :: string(),
    Key :: string().

start_multipart(BucketId, Key) when erlang:is_list(BucketId), erlang:is_list(Key) ->
    start_multipart(BucketId, Key, [], []).

-spec start_multipart(BucketId, Key, Options, HTTPHeaders) -> {ok, proplist()} | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    Options :: proplist(),
    HTTPHeaders :: [{string(), string()}].

start_multipart(BucketId, Key, Options, HTTPHeaders)
	when erlang:is_list(BucketId), erlang:is_list(Key),
	     erlang:is_list(Options), erlang:is_list(HTTPHeaders) ->
    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{"x-amz-meta-" ++ string:to_lower(MKey), MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    POSTData = <<>>,
    Config = #riak_api_config{},
    case s3_xml_request2(post, BucketId, [$/|Key], "uploads", [], POSTData, RequestHeaders, Config) of
        {ok, Doc} ->
            Attributes = [{uploadId, "UploadId", text}],
            {ok, erlcloud_xml:decode(Attributes, Doc)};
        Error -> Error
    end.

%%
%% Queries Riak CS on a subject of provided upload id.
%%
-spec validate_upload_id(BucketId, ObjectKey0, UploadId0) -> {ok, proplist()} | not_found | {error, any()} when
    BucketId :: string(),
    ObjectKey0 :: string(),
    UploadId0 :: string().

validate_upload_id(BucketId, ObjectKey0, UploadId0)
	when erlang:is_list(BucketId), erlang:is_list(ObjectKey0),
	     erlang:is_list(UploadId0) ->
    Config = #riak_api_config{},
    case s3_xml_request2(get, BucketId, [$/|ObjectKey0], [], [{"uploadId", UploadId0}], <<>>, [], Config) of
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

-spec upload_part(BucketId, Key, UploadId, PartNumber, Value) -> {ok, proplist()} | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    UploadId :: string(),
    PartNumber :: integer(),
    Value :: iodata().

upload_part(BucketId, Key, UploadId, PartNumber, Value) ->
    upload_part(BucketId, Key, UploadId, PartNumber, Value, []).

-spec upload_part(BucketId, Key, UploadId, PartNumber, Value, HTTPHeaders) -> {ok, proplist()} | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    UploadId :: string(),
    PartNumber :: integer(),
    Value :: iodata(),
    HTTPHeaders :: [{string(), string()}].

upload_part(BucketId, Key, UploadId, PartNumber, Value, HTTPHeaders)
  when erlang:is_list(BucketId), erlang:is_list(Key), erlang:is_list(UploadId),
       erlang:is_integer(PartNumber), erlang:is_binary(Value), erlang:is_list(HTTPHeaders) ->
    POSTData = erlang:iolist_to_binary(Value),
    Config = #riak_api_config{},
    case s3_request(put, BucketId, [$/|Key], [], [{"uploadId", UploadId},
                    {"partNumber", integer_to_list(PartNumber)}],
                    POSTData, HTTPHeaders, Config) of
        {ok, {_Status, Headers, _Body}} ->
            {ok, [{etag, proplists:get_value("etag", Headers)}]};
        Error ->
            Error
    end.

-spec complete_multipart(BucketId, Key, UploadId, ETags) -> {ok, proplist()} | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    UploadId :: string(),
    ETags :: [{binary(), binary()}].

complete_multipart(BucketId, Key, UploadId, ETags)
	when erlang:is_list(BucketId), erlang:is_list(Key), erlang:is_list(UploadId), erlang:is_list(ETags) ->
    complete_multipart(BucketId, Key, UploadId, ETags, []).

-spec complete_multipart(BucketId, Key, UploadId, ETags, HTTPHeaders) -> {ok, proplist()} | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    UploadId :: string(),
    ETags :: [{binary(), binary()}],
    HTTPHeaders :: [{string(), string()}].

complete_multipart(BucketId, Key, UploadId, ETags, HTTPHeaders)
  when erlang:is_list(BucketId), erlang:is_list(Key), erlang:is_list(UploadId),
       erlang:is_list(ETags), erlang:is_list(HTTPHeaders) ->

    POSTData = list_to_binary(xmerl:export_simple([{'CompleteMultipartUpload',
                                                    [{'Part',
                                                      [{'PartNumber', [integer_to_list(Num)]},
                                                       {'ETag', [ETag]}] } || {Num, ETag} <- ETags]}], xmerl_xml)),
    Config = #riak_api_config{},
    case s3_request(post, BucketId, [$/|Key], [], [{"uploadId", UploadId}],
                     POSTData, HTTPHeaders, Config) of
        {ok, {_Status, _Headers, _Body}} ->
            ok;
        Error ->
            Error
    end.

-spec abort_multipart(BucketId, Key, UploadId) -> ok | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    UploadId :: string().

abort_multipart(BucketId, Key, UploadId)
	when erlang:is_list(BucketId), erlang:is_list(Key), erlang:is_list(UploadId) ->
    abort_multipart(BucketId, Key, UploadId, [], []).

-spec abort_multipart(BucketId, Key, UploadId, Options, HTTPHeaders) -> ok | {error, any()} when
    BucketId :: string(),
    Key :: string(),
    UploadId :: string(),
    Options :: proplist(),
    HTTPHeaders :: [{string(), string()}].

abort_multipart(BucketId, Key, UploadId, Options, HTTPHeaders)
	when erlang:is_list(BucketId), erlang:is_list(Key), erlang:is_list(UploadId),
	     erlang:is_list(Options), erlang:is_list(HTTPHeaders) ->
    Config = #riak_api_config{},
    case s3_request(delete, BucketId, [$/|Key], [], [{"uploadId", UploadId}],
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
	    file:write_file("/tmp/.xml", Body),
	    XML = element(1,xmerl_scan:string(binary_to_list(Body))),
	    case XML of
    		#xmlElement{name='Error'} ->
        	    ErrCode = erlcloud_xml:get_text("/Error/Code", XML),
        	    ErrMsg = erlcloud_xml:get_text("/Error/Message", XML),
        	    erlang:error({s3_error, ErrCode, ErrMsg});
    		_ -> {ok, XML}
	    end;
	{error, {http_error, 404,_,_,_}} -> not_found;
	{error, timeout} -> erlang:error({s3_error, timeout, "Timeout"})
    end.

-spec s3_request(Method, Bucket, Path, Subresource, Params, POSTData, Headers, Config) ->
	{ok, {proplist(), binary()}} | {atom(), {atom(), integer(), string(), binary(), proplist()}} when
    Method :: atom(),
    Bucket :: string(),
    Path :: string(),
    Subresource :: string(),
    Params :: proplist(),
    POSTData :: binary(),
    Headers :: proplist(),
    Config :: riak_api_config().

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
		orelse _Status =:= 400
		orelse _Status =:= 409
		orelse _Status =:= 404
		orelse _Status =:= 405 ->
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
