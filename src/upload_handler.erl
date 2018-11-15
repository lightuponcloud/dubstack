%%
%% Allows to upload objects to Riak CS.
%%
-module(upload_handler).

-export([init/2, resource_exists/2, content_types_accepted/2, handle_post/2,
	 allowed_methods/2, previously_existed/2, allow_missing_post/2,
	 content_types_provided/2, is_authorized/2, forbidden/2, to_json/2, extract_rfc2231_filename/1]).

-include("riak.hrl").
-include("user.hrl").
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

%%
%% Checks if content-range header matches size of uploaded data
%%
validate_data_size(DataSize, StartByte, EndByte) ->
    case EndByte of
	undefined -> ok;  % content-range header is not required for small files
	_ ->
	    case (EndByte - StartByte + 1 =/= DataSize) of
		true -> {error, 1};
		false -> true
	    end
    end.

%%
%% Checks if provided modification time is a positive integer
%%
validate_modified_time(ModifiedTime0) ->
    case ModifiedTime0 of
	undefined -> {error, 22};  % modified_utc is required
	_ ->
	    try
		ModifiedTime1 = utils:to_integer(ModifiedTime0),
		ModifiedTime2 = calendar:gregorian_seconds_to_datetime(ModifiedTime1),
		{{Year, Month, Day}, {Hour, Minute, Second}} = ModifiedTime2,
		calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}})
	    catch error:badarg -> 
		{error, 22}
	    end
    end.


add_action_log_record(State) ->
    User = proplists:get_value(user, State),
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),
    %% When you upload a single file, before the stage of parsing POST request,
    %% object_name is unknown. It becomes available later, so ther'a two object_name
    %% entries in State.
    FileName = proplists:get_value(file_name, State),
    TotalBytes = proplists:get_value(total_bytes, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="upload",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()])
	},
    UnicodeObjectName = unicode:characters_to_list(FileName),
    Summary = lists:flatten([["Uploaded \""], [UnicodeObjectName],
	[io_lib:format("\" ( ~p B )", [TotalBytes])]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary},
    action_log:add_record(BucketId, Prefix, ActionLogRecord1).

%%
%% .Net sends UTF-8 filename in "filename*" field, when "filename" contains garbage.
%%
%% Params:
%%
%% [{<<"name">>,<<"files[]">>},
%% {<<"filename">>,
%%  <<"Something.random">>},
%% {<<"filename*">>,
%%  <<"utf-8''Something.random">>}]
%%
extract_rfc2231_filename(FormDataParams) ->
    case proplists:get_value(<<"filename*">>, FormDataParams) of
	undefined -> proplists:get_value(<<"filename">>, FormDataParams);  %% TODO: remove that
	FileName2 ->
	    FileNameByteSize = byte_size(FileName2),
	    if FileNameByteSize < 8 -> undefined;
		true ->
		    case binary:part(FileName2, {0, 7}) of
			<<"utf-8''">> ->
			    FileName4 = binary:part(FileName2, {7, FileNameByteSize-7}),
			    unicode:characters_to_list(cow_qs:urldecode(FileName4));
			_ -> undefined
		    end
	    end
    end.

acc_multipart(Req0, Acc) ->
    case cowboy_req:read_part(Req0) of
	{ok, Headers0, Req1} ->
    {ok, Body, Req2} = stream_body(Req1, <<>>),
	    Headers1 = maps:to_list(Headers0),
	    {_, DispositionBin} = lists:keyfind(<<"content-disposition">>, 1, Headers1),
	    {<<"form-data">>, Params} = cow_multipart:parse_content_disposition(DispositionBin),
	    {_, FieldName0} = lists:keyfind(<<"name">>, 1, Params),
	    FieldName1 =
		case FieldName0 of
		    <<"object_name">> -> object_name;
		    <<"modified_utc">> -> modified_utc;
		    <<"etags[]">> -> etags;
		    <<"prefix">> -> prefix;
		    <<"files[]">> -> blob;
		    _ -> undefined
		end,
	    case FieldName1 of
		blob ->
		    Filename = extract_rfc2231_filename(Params),
		    acc_multipart(Req2, [{blob, Body}, {filename, Filename}|Acc]);
		undefined -> acc_multipart(Req2, Acc);
		_ -> acc_multipart(Req2, [{FieldName1, Body}|Acc])
	    end;
        {done, Req} -> {lists:reverse(Acc), Req}
    end.

stream_body(Req0, Acc) ->
    case cowboy_req:read_part_body(Req0, #{length => ?FILE_UPLOAD_CHUNK_SIZE + 5000}) of
        {more, Data, Req} -> stream_body(Req, << Acc/binary, Data/binary >>);
        {ok, Data, Req} -> {ok, << Acc/binary, Data/binary >>, Req}
    end.

%%
%% Validates provided content range values and calls 'upload_to_riak()'
%%
handle_post(Req0, State) ->
    case cowboy_req:method(Req0) of
	<<"POST">> ->
	    {FieldValues, Req1} = acc_multipart(Req0, []),
	    FileName0 = proplists:get_value(filename, FieldValues),
	    ObjectName =
		case proplists:get_value(object_name, FieldValues) of
		    undefined -> undefined;
		    ON ->
			%% ObjectName should be an ascii string by now
			erlang:binary_to_list(ON)
		end,
	    Etags = proplists:get_value(etags, FieldValues),

	    BucketId = proplists:get_value(bucket_id, State),
	    Prefix0 = list_handler:validate_prefix(BucketId, proplists:get_value(prefix, FieldValues)),
	    %% Current server UTC time
	    %% It is used by desktop client. TODO: use DVV instead
	    ModifiedTime = validate_modified_time(proplists:get_value(modified_utc, FieldValues)),
	    Blob = proplists:get_value(blob, FieldValues),
	    StartByte = proplists:get_value(start_byte, State),
	    EndByte = proplists:get_value(end_byte, State),
	    DataSizeOK =
		case Blob of
		    undefined -> true;
		    _ -> validate_data_size(size(Blob), StartByte, EndByte)
		end,
	    case lists:keyfind(error, 1, [Prefix0, ModifiedTime, DataSizeOK]) of
		{error, Number} -> js_handler:bad_request(Req1, Number);
		false ->
		    FileName1 = unicode:characters_to_binary(FileName0),
		    NewState = [
			{object_name, ObjectName},
			{etags, Etags},
			{prefix, Prefix0},
			{file_name, FileName1},
			{modified_utc, ModifiedTime}
		    ] ++ State,
		    upload_to_riak(Req1, NewState, Blob)
	    end;
	_ -> js_handler:bad_request(Req0, 2)
    end.

%%
%% Picks object name and uploads file to Riak CS
%%
upload_to_riak(Req0, State0, BinaryData) ->
    BucketId = proplists:get_value(bucket_id, State0),
    Prefix = proplists:get_value(prefix, State0),
    PartNumber = proplists:get_value(part_number, State0),
    IsBig = proplists:get_value(is_big, State0),
    _TotalBytes = proplists:get_value(total_bytes, State0),
    FileName = proplists:get_value(file_name, State0),
    ModifiedTime0 = proplists:get_value(modified_utc, State0),
    case riak_api:head_bucket(BucketId) of
	not_found -> riak_api:create_bucket(BucketId);
	_ -> ok
    end,
    IndexContent = riak_index:get_index(BucketId, Prefix),
    {ObjectKey, OrigName, IsPreviousVersion} = riak_api:pick_object_name(BucketId, Prefix, FileName, ModifiedTime0,
									  IndexContent),
    %% Check modified time to determine newest object
    PrefixedObjectKey0 = utils:prefixed_object_name(Prefix, ObjectKey),
    case IsPreviousVersion of
	true ->
	    Req1 = cowboy_req:reply(304, #{
		<<"content-type">> => <<"application/json">>
	    }, <<>>, Req0),
	    {true, Req1, []};
	false ->
	    State1 = [{prefixed_key, PrefixedObjectKey0}, {orig_name, OrigName}],
	    case IsBig of
		true ->
		    case (PartNumber > 1) of
			true -> check_part(Req0, State0 ++ State1, BinaryData);
			false -> start_upload(Req0, State0 ++ State1, BinaryData)
		    end;
		false ->
		    Options = [{acl, public_read}, {meta, [{"orig-filename", OrigName},
							   {"modified-utc", utils:to_list(ModifiedTime0)}]}],
		    PrefixedObjectKey1 = riak_api:put_object(BucketId, Prefix, ObjectKey, BinaryData, Options),
		    Req1 = cowboy_req:set_resp_body(jsx:encode([
			{object_name, unicode:characters_to_binary(PrefixedObjectKey1)},
			{modified_utc, ModifiedTime0}
		    ]), Req0),
		    %% Update index
		    riak_index:update(BucketId, Prefix),
		    %% Update Solr index if file supported
		    %% TODO: uncomment the following
		    %%gen_server:abcast(solr_api, [{bucket_id, BucketId},
		    %%	{prefix, Prefix}, {object_name, ObjectKey},
		    %%	{total_bytes, TotalBytes}]),
		    add_action_log_record(State0),
		    {true, Req1, []}
	    end
    end.

check_part(Req0, State, BinaryData) ->
    %%
    %% Checks if upload with provided ID exists and uploads `BinaryData`
    %%
    UploadId = proplists:get_value(upload_id, State),
    BucketId = proplists:get_value(bucket_id, State),
    PrefixedObjectKey = proplists:get_value(prefixed_key, State),
    case proplists:get_value(object_name, State) of
	undefined -> js_handler:bad_request(Req0, 8);
	_ ->
	    case riak_api:validate_upload_id(BucketId, PrefixedObjectKey, UploadId) of
		not_found -> js_handler:bad_request(Req0, 4);
		{error, _Reason} -> js_handler:bad_request(Req0, 5);
		_ -> upload_part(Req0, State, BinaryData)
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
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),
    ObjectName = proplists:get_value(object_name, State),
    PartNumber = proplists:get_value(part_number, State),
    Etags0 = proplists:get_value(etags, State),
    EndByte = proplists:get_value(end_byte, State),
    TotalBytes = proplists:get_value(total_bytes, State),

    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    case riak_api:upload_part(BucketId, PrefixedObjectName, UploadId, PartNumber, BinaryData) of
	{ok, [{_, NewEtag0}]} ->
	    case (EndByte+1 =:= TotalBytes) of
		true ->
		    case Etags0 =:= undefined of
			true -> js_handler:bad_request(Req0, 5);
			false ->
			    %% parse etags from request to complete upload
			    Etags1 = parse_etags(binary:split(Etags0, <<$,>>, [global])),
			    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
			    riak_api:complete_multipart(BucketId, PrefixedObjectName, UploadId, Etags1),
			    <<_:1/binary, NewEtag1:32/binary, _:1/binary>> = unicode:characters_to_binary(NewEtag0),
			    Response = [{upload_id, unicode:characters_to_binary(UploadId)},
				{object_name, unicode:characters_to_binary(ObjectName)},
				{end_byte, EndByte}, {md5, NewEtag1}],
			    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),

			    %% Update index
			    riak_index:update(BucketId, Prefix),
			    %% Update Solr index if file supported
			    %% TODO: uncomment the following
			    %%gen_server:abcast(solr_api, [{bucket_id, BucketId},
			    %%	{prefix, Prefix}, {object_name, ObjectName},
			    %%	{total_bytes, TotalBytes}]),

			    add_action_log_record(State),
			    {true, Req1, []}
		    end;
		false ->
		    <<_:1/binary, NewEtag1:32/binary, _:1/binary>> = unicode:characters_to_binary(NewEtag0),
		    Response = [{upload_id, unicode:characters_to_binary(UploadId)},
			{object_name, unicode:characters_to_binary(ObjectName)},
			{end_byte, EndByte},
			{md5, NewEtag1}],
		    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
		    {true, Req1, []}
	    end;
	{error, _} -> js_handler:bad_request(Req0, 6)
    end.

%%
%% Creates identifier and uploads first part of data
%%
start_upload(Req0, State, BinaryData) ->
    %% Create a bucket if not exist
    BucketId = proplists:get_value(bucket_id, State),
    EndByte = proplists:get_value(end_byte, State),
    PrefixedObjectKey = proplists:get_value(prefixed_key, State),
    OrigName = proplists:get_value(orig_name, State),
    ModifiedTime = proplists:get_value(modified_utc, State),

    Options = [{acl, public_read}, {meta, [{"orig-filename", OrigName},
					   {"modified-utc", utils:to_list(ModifiedTime)}]}],
    ObjectKey = filename:basename(PrefixedObjectKey),
    MimeType = utils:mime_type(ObjectKey),
    Headers = [{"content-type", MimeType}],

    {ok, [{_, UploadId1}]} = riak_api:start_multipart(BucketId, PrefixedObjectKey, Options, Headers),
    {ok, [{_, Etag0}]} = riak_api:upload_part(BucketId, PrefixedObjectKey, UploadId1, 1, BinaryData),

    <<_:1/binary, Etag1:32/binary, _:1/binary>> = unicode:characters_to_binary(Etag0),
    Response = [{upload_id, unicode:characters_to_binary(UploadId1)},
	{object_name, unicode:characters_to_binary(ObjectKey)},
	{end_byte, EndByte}, {md5, Etag1}],
    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
    {true, Req1, []}.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, _State) ->
    case utils:check_token(Req0) of
	undefined -> {{false, <<"Token">>}, Req0, []};
	not_found -> {{false, <<"Token">>}, Req0, []};
	expired -> {{false, <<"Token">>}, Req0, []};
	User -> {true, Req0, User}
    end.

validate_content_range(Req) ->
    PartNumber =
	try utils:to_integer(cowboy_req:binding(part_num, Req)) of
	    N -> N
	catch error:_ -> 1
	end,
    UploadId0 =
	case cowboy_req:binding(upload_id, Req) of
	    undefined -> undefined;
	    UploadId1 -> erlang:binary_to_list(UploadId1)
	end,
    {StartByte0, EndByte0, TotalBytes0} =
	case cowboy_req:header(<<"content-range">>, Req) of
	    undefined -> {undefined, undefined, undefined};
	    Value ->
		{bytes, Start, End, Total} = cow_http_hd:parse_content_range(Value),
		{Start, End, Total}
	end,
    case TotalBytes0 of
	undefined ->
	    [{part_number, PartNumber},
	     {upload_id, UploadId0},
	     {start_byte, undefined},
	     {end_byte, undefined},
	     {total_bytes, undefined}];
	TotalBytes1 ->
	    case TotalBytes1 > ?FILE_MAXIMUM_SIZE of
		true -> {error, 24};
		false ->
		    case PartNumber > 1 andalso UploadId0 =:= undefined of
			true -> {error, 25};
			false -> [{part_number, PartNumber}, {upload_id, UploadId0},
				  {start_byte, StartByte0}, {end_byte, EndByte0},
				  {total_bytes, TotalBytes0}]
		    end
	    end
    end.

%%
%% Checks the following.
%% - User has access
%% - Bucket ID is correct
%% - Prefix is correct
%% - Part number is correct
%% - content-range request header is specified
%% - file size do not exceed the limit
%%
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, State0) ->
    BucketId = erlang:binary_to_list(cowboy_req:binding(bucket_id, Req0)),
    case utils:is_valid_bucket_id(BucketId, State0#user.tenant_id) of
	true ->
	    UserBelongsToGroup = lists:any(fun(Group) ->
		utils:is_bucket_belongs_to_group(BucketId, State0#user.tenant_id, Group#group.id) end,
		State0#user.groups),
	    case UserBelongsToGroup of
		false -> {true, Req0, []};
		true ->
		    case validate_content_range(Req0) of
			{error, Reason} ->
			    Req1 = cowboy_req:set_resp_body(Reason, Req0),
			    {true, Req1, []};
			State1 -> {false, Req0, State1++[{user, State0}, {bucket_id, BucketId}]}
		    end
	    end;
	false -> {true, Req0, []}
    end.

%%
%% Check if file size do not exceed the limit
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    PartNumber = proplists:get_value(part_number, State),
    TotalBytes = proplists:get_value(total_bytes, State),
    IsBig =
	case TotalBytes =:= undefined of
	    true -> false;
	    false -> (TotalBytes > ?FILE_UPLOAD_CHUNK_SIZE)
	end,
    MaximumPartNumber = (?FILE_MAXIMUM_SIZE div ?FILE_UPLOAD_CHUNK_SIZE),
    case PartNumber < MaximumPartNumber andalso PartNumber >= 1 of
	false -> {false, Req0, []};
	true -> {true, Req0, State ++ [{is_big, IsBig}]}
    end.

previously_existed(Req0, State) ->
    {false, Req0, State}.

allow_missing_post(Req0, State) ->
    {false, Req0, State}.
