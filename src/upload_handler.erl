-module(upload_handler).

-export([init/2, resource_exists/2, content_types_accepted/2, handle_post/2,
	 allowed_methods/2, previously_existed/2, allow_missing_post/2,
	 content_types_provided/2, forbidden/2, to_json/2]).

-include("riak.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Returns callback 'handle_post()'
%% ( called after 'resource_exists()' )
%%
content_types_accepted(Req, State) ->
    {[{{<<"multipart">>, <<"form-data">>, '*'}, handle_post}], Req, State}.

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

parse_field_values(Body, Boundary) ->
    case cow_multipart:parse_headers(Body, Boundary) of
	{ok, Header, Rest0} ->
	    FormDatadName = proplists:get_value(<<"content-disposition">>, Header),
	    FieldName = case FormDatadName of
		<<"form-data; name=\"etags[]\"">> -> "etags";
		<<"form-data; name=\"object_name\"">> -> "object_name";
		<<"form-data; name=\"prefix\"">> -> "prefix"
	    end,
	    {done, FieldValue, Rest1} = cow_multipart:parse_body(Rest0, Boundary),
	    [{FieldName, FieldValue}] ++ parse_field_values(Rest1, Boundary);
	{done, _} -> []
    end.

validate_post(Req, State, DataSize, Prefix) ->
    StartByte = proplists:get_value(start_byte, State),
    EndByte = proplists:get_value(end_byte, State),

    case (EndByte - StartByte + 1 =/= DataSize) of
	true ->
	    error_response(Req, 1);
	false ->
	    case utils:is_valid_hex_prefix(Prefix) of
		false ->
		    error_response(Req, 1);
		true ->
		    case Prefix of
			undefined -> undefined;
			_ -> binary_to_list(Prefix)
		    end
	    end
    end.

add_action_log_record(State) ->
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),
    BucketName = proplists:get_value(bucket_name, State),
    Prefix = proplists:get_value(prefix, State),
    %% When you upload a single file, before the stage of parsing POST request,
    %% object_name is unknown. It becomes available later, so ther'a two object_name
    %% entries in State.
    FileName = proplists:get_value(file_name, State),
    TotalBytes = proplists:get_value(total_bytes, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="upload",
	user_name=UserName,
	tenant_name=TenantName,
	timestamp=io_lib:format("~p", [utils:timestamp()])
	},

    UnicodeObjectName = unicode:characters_to_list(FileName),
    Summary = lists:flatten([["Uploaded "], [UnicodeObjectName], [io_lib:format(" ( ~p B )", [TotalBytes])]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary},
    action_log:add_record(BucketName, Prefix, ActionLogRecord1).

%%
%% Validates provided content range values and calls 'upload_to_riak()'
%%
handle_post(Req0, State) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    {ok, Headers, Req1} = cowboy_req:read_part(Req0),
	    {ok, Data, Req2} = cowboy_req:read_part_body(Req1,
		#{length => ?FILE_UPLOAD_CHUNK_SIZE + 5000}),

	    {file, <<"files[]">>, FileName0, _, _} = cow_multipart:form_data(
		Headers),
	    {ok, {Boundary, Body}} = maps:find(multipart, Req2),

	    FieldValues = parse_field_values(Body, Boundary),
	    ObjectName =
		case proplists:get_value("object_name", FieldValues) of
		    undefined -> undefined;
		    _ObjectName -> binary_to_list(_ObjectName)  % it should be an ascii string by now
		end,
	    Prefix0 = 
		case proplists:get_value("prefix", FieldValues) of
		    undefined -> undefined;
		    _Prefix -> unicode:characters_to_binary(_Prefix)
		end,
	    Etags = proplists:get_value("etags", FieldValues),

	    case validate_post(Req0, State, size(Data), Prefix0) of
		{true, Req3, []} ->
		    {true, Req3, []};  % error
		Prefix1 ->
		    FileName1 = unicode:characters_to_binary(FileName0),
		    NewState = [
			{object_name, ObjectName},
			{etags, Etags},
			{prefix, Prefix1},
			{file_name, FileName1}
		    ] ++ State,
		    upload_to_riak(Req2, NewState, Data)
	    end;
	_ ->
	    error_response(Req0, 2)
    end.

%%
%% Picks object name and uploads file to Riak CS
%%
upload_to_riak(Req0, State, BinaryData) ->
    BucketName = proplists:get_value(bucket_name, State),
    Prefix = proplists:get_value(prefix, State),
    PartNumber = proplists:get_value(part_number, State),
    IsBig = proplists:get_value(is_big, State),
    TotalBytes = proplists:get_value(total_bytes, State),
    FileName = proplists:get_value(file_name, State),

    case IsBig of
	true ->
	    case (PartNumber > 1) of
		true ->
		    check_part(Req0, State, BinaryData);
		false ->
		    start_upload(Req0, State, BinaryData)
	    end;
	false ->
	    ObjectName = riak_api:pick_object_name(BucketName, Prefix, FileName),
	    Options = [{acl, public_read}, {meta, [{"orig-filename", FileName}]}],
	    PrefixedObjectName = riak_api:put_object(BucketName, Prefix, ObjectName, BinaryData, Options),
	    Req1 = cowboy_req:set_resp_body(jsx:encode([{object_name, PrefixedObjectName}]), Req0),
	    %% update Solr index if file supported
	    gen_server:abcast(solr_api, [{bucket_name, BucketName},
		{prefix, Prefix}, {object_name, ObjectName},
		{total_bytes, TotalBytes}]),

	    add_action_log_record(State),
	    {true, Req1, []}
    end.

check_part(Req0, State, BinaryData) ->
    %%
    %% Checks if upload with provided ID exists and uploads BinaryData
    %%
    UploadId = proplists:get_value(upload_id, State),
    BucketName = proplists:get_value(bucket_name, State),
    Prefix = proplists:get_value(prefix, State),
    ObjectName = proplists:get_value(object_name, State),

    case (ObjectName =:= undefined) of
	true ->
	    error_response(Req0, 3);
	false ->
	    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
	    case riak_api:validate_upload_id(BucketName, Prefix, PrefixedObjectName, UploadId) of
		not_found ->
		    error_response(Req0, 4);
		{error, Reason} ->
		    error_response(Req0, term_to_binary(Reason));
		_ ->
		    upload_part(Req0, State, BinaryData)
	    end
    end.

%% @todo: get rid of utils:to_list() by using proper xml binary serialization
parse_etags([K,V | T]) -> [{
	utils:to_integer(K),
	utils:to_list(<<  <<$">>/binary, V/binary, <<$">>/binary >>)
    } | parse_etags(T)];
parse_etags([]) -> [].

upload_part(Req0, State, BinaryData) ->
    UploadId = proplists:get_value(upload_id, State),
    BucketName = proplists:get_value(bucket_name, State),
    Prefix = proplists:get_value(prefix, State),
    ObjectName = proplists:get_value(object_name, State),
    PartNumber = proplists:get_value(part_number, State),
    Etags0 = proplists:get_value(etags, State),
    EndByte = proplists:get_value(end_byte, State),
    TotalBytes = proplists:get_value(total_bytes, State),

    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    case riak_api:upload_part(BucketName, PrefixedObjectName, UploadId, PartNumber, BinaryData) of
	{ok, [{_, NewEtag}]} ->
	    case (EndByte+1 =:= TotalBytes) of
		true -> 
		    case Etags0 =:= undefined of
			true ->
			    error_response(Req0, 5);
			false ->
			    %% parse etags from request to complete upload
			    Etags1 = parse_etags(binary:split(Etags0, <<$,>>, [global])),
			    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
			    riak_api:complete_multipart(BucketName, PrefixedObjectName, UploadId, Etags1),
			    Response = [{upload_id, UploadId}, {object_name, ObjectName},
				{end_byte, EndByte}, {md5, NewEtag}],
			    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
			    %% update Solr index if file supported
			    gen_server:abcast(solr_api, [{bucket_name, BucketName},
				{prefix, Prefix}, {object_name, ObjectName},
				{total_bytes, TotalBytes}]),

			    add_action_log_record(State),
			    {true, Req1, []}
		    end;
		false ->
		    Response = [{upload_id, UploadId}, {object_name, ObjectName},
			{end_byte, EndByte}, {md5, NewEtag}],
		    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
		    {true, Req1, []}
	    end;
	{error, _} ->
	    error_response(Req0, 6)
    end.

%%
%% Creates identifier and uploads first part of data
%%
start_upload(Req0, State, BinaryData) ->
    %% Create a bucket if not exist
    BucketName = proplists:get_value(bucket_name, State),
    Prefix = proplists:get_value(prefix, State),
    EndByte = proplists:get_value(end_byte, State),
    FileName = proplists:get_value(file_name, State),

    case riak_api:head_bucket(BucketName) of
    	not_found ->
	    riak_api:create_bucket(BucketName);
    	_ -> ok
    end,
    ObjectName = riak_api:pick_object_name(BucketName, Prefix, FileName),
    Options = [{acl, public_read}, {meta, [{"orig-filename", FileName}]}],

    MimeType = utils:mime_type(ObjectName),
    Headers = [{"content-type", MimeType}],

    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    {ok, [{_, UploadId1}]} = riak_api:start_multipart(BucketName,
	PrefixedObjectName, Options, Headers),
    {ok, [{_, Etag}]} = riak_api:upload_part(BucketName, PrefixedObjectName,
	UploadId1, 1, BinaryData),

    Response = [{upload_id, UploadId1}, {object_name, ObjectName},
	{end_byte, EndByte}, {md5, Etag}],

    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
    {true, Req1, []}.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    PartNumber = proplists:get_value(part_number, State),
    TotalBytes = proplists:get_value(total_bytes, State),
    UploadId = proplists:get_value(upload_id, State),
    UserName = proplists:get_value(user_name, State),
    TenantName = proplists:get_value(tenant_name, State),
    BucketName = proplists:get_value(bucket_name, State),
    IsValidBucketName = utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName),

    %% Check if file size do not exceed the limit, then
    %% start multipart upload
    case (TotalBytes > ?FILE_MAXIMUM_SIZE) andalso (IsValidBucketName =/= true) of
	true -> {false, Req0, []};
	false -> 
	    case (PartNumber > 1) andalso (UploadId =:= undefined) of
		true -> 
		    {false, Req0, []};
		false -> 
		    {true, Req0, State}
	    end
    end.

%%
%% Checks if bucket name, prefix and part number parameters are correct.
%% Then checks if content-range request header is specified.
%% ( called after 'allowed_methods()' )
%%
%% TODO: authentication
%%
forbidden(Req0, State) ->
    UploadId =
	case cowboy_req:binding(upload_id, Req0) of
	    undefined -> undefined;
	    _UploadId -> binary_to_list(_UploadId)
	end,
    BucketName = binary_to_list(cowboy_req:binding(bucket_name, Req0)),
    PartNumber = try utils:to_integer(cowboy_req:binding(part_num, Req0)) of
		    N -> N catch error:_ -> 1 end,
    {StartByte, EndByte, TotalBytes} =
	case cowboy_req:header(<<"content-range">>, Req0) of
	    undefined -> {undefined, undefined, undefined};
	    Value -> 
		{bytes, Start, End, Total} = cow_http_hd:parse_content_range(Value),
		{Start, End, Total}
	end,
    IsBig =
	case TotalBytes =:= undefined of
	    true -> false;
	    false -> (TotalBytes > ?FILE_UPLOAD_CHUNK_SIZE)
	end,
    Token = case cowboy_req:binding(token, Req0) of
	undefined -> undefined;
	TokenValue -> binary_to_list(TokenValue)
    end,
    KeystoneAttrs = keystone_api:check_token(Token),
    UserName = case KeystoneAttrs of
	not_found -> not_found;
	_ -> proplists:get_value(user_name, KeystoneAttrs)
    end,
    TenantName = case KeystoneAttrs of
	not_found -> not_found;
	_ -> proplists:get_value(tenant_name, KeystoneAttrs)
    end,
    MaximumPartNumber = (?FILE_MAXIMUM_SIZE div ?FILE_UPLOAD_CHUNK_SIZE),
    case utils:is_valid_bucket_name(BucketName, TenantName)
	    andalso UserName =/= not_found
	    andalso utils:is_bucket_belongs_to_user(BucketName, UserName, TenantName)
	    andalso (PartNumber < MaximumPartNumber)
	    andalso (PartNumber >= 1) of
	true ->
	    case TotalBytes of
		undefined -> {true, Req0, State};
		_ ->
		    {false, Req0, [{upload_id, UploadId},
			{bucket_name, BucketName}, {part_number, PartNumber},
			{start_byte, StartByte}, {end_byte, EndByte},
			{total_bytes, TotalBytes}, {is_big, IsBig},
			{user_name, UserName}, {tenant_name, TenantName}]}
	    end;
	false ->
	    {true, Req0, []}
    end.

previously_existed(Req0, State) ->
    {false, Req0, State}.

allow_missing_post(Req0, State) ->
    {false, Req0, State}.
