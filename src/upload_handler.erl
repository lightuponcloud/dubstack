%%
%% Allows to upload objects to Riak CS.
%%
-module(upload_handler).
-behavior(cowboy_handler).

-export([init/2, resource_exists/2, content_types_accepted/2, handle_post/2,
	 allowed_methods/2, previously_existed/2, allow_missing_post/2,
	 content_types_provided/2, is_authorized/2, forbidden/2, to_json/2, extract_rfc2231_filename/1]).

-include("riak.hrl").
-include("entities.hrl").
-include("action_log.hrl").
-include("log.hrl").

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
%% Checks if modification time is a valid positive integer timestamp
%%
%% modified_utc is required
%%
validate_modified_time(undefined, required) ->  {error, 22};
validate_modified_time(undefined, not_required) ->  undefined;
validate_modified_time(ModifiedTime0, _IsRequired) ->
    try
	ModifiedTime1 = utils:to_integer(ModifiedTime0),
	ModifiedTime2 = calendar:gregorian_seconds_to_datetime(ModifiedTime1),
	{{Year, Month, Day}, {Hour, Minute, Second}} = ModifiedTime2,
	calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}})
    catch error:badarg ->
        {error, 22}
    end.

%%
%% Checks timestamp and GUID, if provided.
%%
validate_modified_time(BucketId, GUID, ModifiedTime0, IsRequired)
	when erlang:is_list(GUID) orelse GUID =:= undefined ->
    case validate_modified_time(ModifiedTime0, IsRequired) of
	{error, Number} -> {error, Number};
	undefined -> undefined;
	ModifiedTime1 ->
	    case GUID of
		undefined -> ModifiedTime1;
		_ ->
		    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
		    case riak_api:list_objects(BucketId, [{prefix, RealPrefix}, {marker, undefined}]) of
			not_found -> {error, 42};
			_ -> ModifiedTime1
		    end
	    end
    end.

%%
%% Returns error, if filename contains prohibited characters.
%% Otherwise returns the same provided filename.
%%
validate_filename(undefined) -> {error, 47};
validate_filename(<<>>) -> {error, 47};
validate_filename(<<Bit:2/binary, _Rest/binary>>) when Bit =:= <<"~$">> -> {error, 47};
validate_filename(<<Bit:2/binary, _Rest/binary>>) when Bit =:= <<".~">> -> {error, 47};
validate_filename(FileName)
        when FileName =:= <<"desktop.ini">> orelse
             FileName =:= <<"thumbs.db">> orelse
             FileName =:= <<".ds_store">> orelse
             FileName =:= <<".dropbox">> orelse
             FileName =:= <<".dropbox.attr">> ->
    {error, 47};
validate_filename(FileName) ->
    ProhibitedChrs = [<<"<">>, <<">">>, <<":">>, <<"\"">>, <<"|">>, <<"?">>, <<"*">>],
    NoProhibitedChrs = lists:all(
        fun(C) ->
            binary:matches(FileName, C) =:= []
        end, ProhibitedChrs),
    case NoProhibitedChrs of
        false -> {error, 47};
        true ->
            case length(unicode:characters_to_list(FileName)) > 260 of
                true -> {error, 48};
                false ->
                    ProhibitedTrailingChrs = [<<".">>, <<" ">>,
                        erlang:list_to_binary(?RIAK_LOCK_SUFFIX)],
                    NoProhibitedTrailingChrs = lists:all(
                        fun(C) ->
                            utils:ends_with(FileName, C) =:= false
                    end, ProhibitedTrailingChrs),
                    case NoProhibitedTrailingChrs of
                        false -> {error, 47};
                        true -> FileName
                    end
            end
    end.

%%
%% Returns undefined, in case not an integer provided.
%% Otherwise returns integer value.
%%
validate_integer_field(undefined) -> undefined;
validate_integer_field(Value) when erlang:is_binary(Value) ->
    try utils:to_integer(Value) of
	V -> V
    catch error:badarg ->
	undefined
    end.

add_action_log_record(State) ->
    User = proplists:get_value(user, State),
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),
    OrigName = proplists:get_value(orig_name, State),  %% generated name
    TotalBytes = proplists:get_value(total_bytes, State),
    UploadTime = proplists:get_value(upload_time, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="upload",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=UploadTime
    },
    UnicodeObjectKey = unicode:characters_to_list(OrigName),
    Summary = lists:flatten([["Uploaded \""], [UnicodeObjectKey],
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
			    cow_qs:urldecode(FileName4);
			_ -> undefined
		    end
	    end
    end.

%%
%% Parse POST fields.
%%
%% modified_utc -- timestamp (UTC)
%% etags[] -- list of MD5
%% prefix -- hex-encoded directory name
%% files[] --
%% guid -- the key in object storage. It also used to track history of file
%% last_seen_modified_utc -- modified time client have seen previously.
%%                           It is used for finding conflicts.
%%
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
			<<"modified_utc">> -> last_modified_utc;
			<<"etags[]">> -> etags;
			<<"prefix">> -> prefix;
			<<"files[]">> -> blob;
			<<"guid">> -> guid;
			<<"last_seen_modified_utc">> -> last_seen_modified_utc;
			<<"width">> -> width;
			<<"height">> -> height;
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

%%
%% Reads HTTP request body.
%%
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
	    FileName0 = validate_filename(proplists:get_value(filename, FieldValues)),
	    Etags = proplists:get_value(etags, FieldValues),
	    BucketId = proplists:get_value(bucket_id, State),
	    Prefix0 = list_handler:validate_prefix(BucketId, proplists:get_value(prefix, FieldValues)),
	    %% Current server UTC time
	    %% It is used by desktop client. TODO: use DVV instead
	    GUID0 =
		case proplists:get_value(guid, FieldValues) of
		    undefined -> undefined;
		    <<>> -> undefined;
		    G -> unicode:characters_to_list(G)
		end,
	    ModifiedTime = validate_modified_time(BucketId, GUID0,
						  proplists:get_value(last_modified_utc, FieldValues), required),
	    LastSeenModifiedTime = validate_modified_time(BucketId, GUID0,
							  proplists:get_value(last_seen_modified_utc, FieldValues),
							  not_required),
	    Blob = proplists:get_value(blob, FieldValues),
	    StartByte = proplists:get_value(start_byte, State),
	    EndByte = proplists:get_value(end_byte, State),
	    DataSizeOK =
		case Blob of
		    undefined -> true;
		    _ -> validate_data_size(size(Blob), StartByte, EndByte)
		end,
	    Width0 = validate_integer_field(proplists:get_value(width, FieldValues)),
	    Height0 = validate_integer_field(proplists:get_value(height, FieldValues)),
	    %% Both width and height of image must be specified
	    {Width1, Height1} =
		case lists:all(fun(I) -> I =/= undefined end, [Width0, Height0]) of
		    true -> {Width0, Height0};
		    false -> {undefined, undefined}
		end,
	    case lists:keyfind(error, 1, [FileName0, Prefix0, ModifiedTime, LastSeenModifiedTime, DataSizeOK]) of
		{error, Number} -> js_handler:bad_request(Req1, Number);
		false ->
		    UploadTime = erlang:round(utils:timestamp()/1000),
		    NewState = [
			{etags, Etags},
			{prefix, Prefix0},
			{file_name, FileName0},
			{last_modified_utc, ModifiedTime},
			{last_seen_modified_utc, LastSeenModifiedTime},
			{guid, GUID0},
			{upload_time, UploadTime},
			{width, Width1},
			{height, Height1}
		    ] ++ State,
		    upload_to_riak(Req1, NewState, Blob)
	    end;
	_ -> js_handler:bad_request(Req0, 2)
    end.

%%
%% Compares binary's Md5 and Etag.
%%
%% Returns false in case Etag:
%% - Not valid
%% - Not specified
%% - Not equal to Md5(BinaryData)
%%
%%
validate_md5(undefined, BinaryData) when erlang:is_binary(BinaryData) -> false;
validate_md5(Etag0, BinaryData) when erlang:is_binary(BinaryData) ->
    try utils:unhex(Etag0) of
	Etag1 ->
	    Md5 = riak_crypto:md5(BinaryData),
	    Etag1 =:= Md5
    catch
	error:_ -> false
    end.

%%
%% Creates link to actual object, updates index.
%%
update_index(Req0, State0) ->
    User = proplists:get_value(user, State0),
    BucketId = proplists:get_value(bucket_id, State0),
    Prefix = proplists:get_value(prefix, State0),
    ModifiedTime0 = proplists:get_value(last_modified_utc, State0),
    UploadTime = proplists:get_value(upload_time, State0),
    TotalBytes = proplists:get_value(total_bytes, State0),
    GUID = proplists:get_value(guid, State0),
    ObjectKey0 = proplists:get_value(object_key, State0),
    OrigName0 = proplists:get_value(orig_name, State0),
    IsConflict = proplists:get_value(is_conflict, State0),
    {IsLocked0, LockModifiedTime0, LockedUserId0, LockedUserName0, LockedUserTel0} =
	case proplists:get_value(object, State0) of
	    undefined -> {undefined, undefined, undefined, undefined, undefined};
	    ExistingObject ->
		case IsConflict of
		    true -> {undefined, undefined, undefined, undefined, undefined};
		    false -> {ExistingObject#object.is_locked,
			      ExistingObject#object.lock_modified_utc,
			      ExistingObject#object.lock_user_id,
			      ExistingObject#object.lock_user_name,
			      ExistingObject#object.lock_user_tel}
		end
	end,
    LockedUserName1 =
	case LockedUserName0 of
	    undefined -> undefined;
	    _ -> utils:hex(LockedUserName0)
	end,
    LockedUserTel1 =
	case LockedUserTel0 of
	    undefined -> undefined;
	    _ -> utils:hex(LockedUserTel0)
	end,
    %% Put link to the real object at the specified prefix
    Meta = list_handler:parse_object_record([], [
	    {orig_name, utils:hex(OrigName0)},
	    {last_modified_utc, ModifiedTime0},
	    {upload_time, UploadTime},
	    {guid, GUID},
	    {author_id, User#user.id},
	    {author_name, User#user.name},
	    {author_tel, User#user.tel},
	    {is_locked, IsLocked0},
	    {lock_modified_utc, LockModifiedTime0},
	    {lock_user_id, LockedUserId0},
	    {lock_user_name, LockedUserName1},
	    {lock_user_tel, LockedUserTel1},
	    {is_deleted, "false"},
	    {bytes, utils:to_list(TotalBytes)}
	]),
    MimeType = utils:mime_type(ObjectKey0),
    %% get image width and heignt if it is less than 50 MB
    WidthHeight =
	case TotalBytes > ?MAXIMUM_IMAGE_SIZE_BYTES orelse utils:starts_with(MimeType, <<"image/">>) =:= false of
	    true ->
		[{"width", proplists:get_value(width, State0)},
		{"height", proplists:get_value(height, State0)}];
	    false ->
		{OldBucketId, RealPath} = download_handler:real_path(BucketId,
		    [{"x-amz-meta-guid", GUID}, {"x-amz-meta-modified-utc", utils:to_list(ModifiedTime0)}]),
		case riak_api:get_object(OldBucketId, RealPath) of
		    not_found ->
			[{"width", proplists:get_value(width, State0)},
			 {"height", proplists:get_value(height, State0)}];
		    RiakResponse ->
			Content = proplists:get_value(content, RiakResponse),
			Reply0 = img:scale([{from, Content}, {just_get_size, true}]),
			case Reply0 of
			    {error, _Reason} ->
				[{"width", proplists:get_value(width, State0)},
				 {"height", proplists:get_value(height, State0)}];
			    {Width, Height} -> [{"width", Width}, {"height", Height}]
			end
		end
	end,
    Options = [{acl, public_read}, {meta, Meta++WidthHeight}],
    case riak_api:put_object(BucketId, Prefix, ObjectKey0, <<>>, Options) of
	ok ->
	    %% Update pseudo-directory index for faster listing.
	    case indexing:update(BucketId, Prefix, [{modified_keys, [ObjectKey0]}]) of
		lock -> js_handler:too_many(Req0);
		_ ->
		    %% Update Solr index if file type is supported
		    %% TODO: uncomment the following
		    %%gen_server:abcast(solr_api, [{bucket_id, BucketId},
		    %% {prefix, Prefix},
		    %% {total_bytes, TotalBytes}]),
		    State1 = [
			{bucket_id, BucketId},
			{prefix, Prefix},
			{upload_time, erlang:integer_to_list(UploadTime)},
			{orig_name, OrigName0},
			{total_bytes, TotalBytes},
			{user, proplists:get_value(user, State0)}
		    ],
		    add_action_log_record(State1),
		    LockedUserTel2 =
			case LockedUserTel1 of
			    undefined -> null;
			    Tel -> erlang:list_to_binary(Tel)
			end,
		    LockedUserName2 =
			case LockedUserName1 of
			    undefined -> null;
			    _ -> erlang:list_to_binary(LockedUserName1)
			end,
		    LockedUserId1 =
			case LockedUserId0 of
			    undefined -> null;
			    _ -> erlang:list_to_binary(LockedUserId0)
			end,
		    LockModifiedTime1 =
			case LockModifiedTime0 of
			    undefined -> null;
			    _ -> LockModifiedTime0
			end,
		    IsLocked1 =
			case IsLocked0 of
			    undefined -> false;
			    _ -> IsLocked0
			end,
		    AuthorTel =
			case User#user.tel of
			    undefined -> null;
			    _ -> unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(User#user.tel)))
			end,
		    UploadId =
			case proplists:get_value(upload_id, State0) of
			    undefined -> null;
			    UID -> UID
			end,
		    Req1 = cowboy_req:set_resp_body(jsx:encode([
			{guid, unicode:characters_to_binary(GUID)},
			{orig_name, OrigName0},
			{last_modified_utc, ModifiedTime0},
			{object_key, erlang:list_to_binary(ObjectKey0)},
			{upload_id, UploadId},
			{end_byte, proplists:get_value(end_byte, State0, null)},
			{md5, proplists:get_value(md5, State0, null)},
			{upload_time, UploadTime},
			{author_id, erlang:list_to_binary(User#user.id)},
			{author_name, unicode:characters_to_binary(utils:unhex(erlang:list_to_binary(User#user.name)))},
			{author_tel, AuthorTel},
			{is_locked, IsLocked1},
			{lock_modified_utc, LockModifiedTime1},
			{lock_user_id, LockedUserId1},
			{lock_user_name, LockedUserName2},
			{lock_user_tel, LockedUserTel2},
			{is_deleted, false},
			{bytes, TotalBytes},
			{width, proplists:get_value(width, State0, null)},
			{height, proplists:get_value(height, State0, null)}
		    ]), Req0),

		    {true, Req1, []}
	    end;
	{error, Reason} ->
	    ?WARN("[upload_handler] Error: ~p~n", [Reason]),
	    js_handler:incorrect_configuration(Req0, 5)
    end.

%%
%% Deletes previous version of object for the same date, 
%% if ther'a no links on previous version. This is the case when .stop file exists.
%%
delete_previous_one(_BucketId, undefined, _NewModifiedTime, _OldGUID, _NewGUID) -> ok;
delete_previous_one(BucketId, OldModifiedTime, NewModifiedTime, OldGUID, NewGUID)
	when erlang:is_list(BucketId), erlang:is_integer(OldModifiedTime), erlang:is_integer(NewModifiedTime),
	     erlang:is_list(OldGUID), erlang:is_list(NewGUID) ->
    OldDate = utils:format_timestamp(OldModifiedTime),
    NewDate = utils:format_timestamp(NewModifiedTime),
    case OldDate =:= NewDate of
	false -> ok;
	true ->
	    %% New object could be conflicted. In that case a new GUID should be created
	    case OldGUID =:= NewGUID of
		true ->
		    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, OldGUID),
		    ExistingLastModified = erlang:integer_to_list(OldModifiedTime),
		    PrefixedStopSignName = utils:prefixed_object_key(RealPrefix, ExistingLastModified++".stop"),
		    case riak_api:head_object(BucketId, PrefixedStopSignName) of
			not_found ->
			    PrefixedRealPath = utils:prefixed_object_key(RealPrefix, ExistingLastModified),
			    ?WARN("Removing ~p~n", [PrefixedRealPath]),
			    riak_api:delete_object(BucketId, PrefixedRealPath);
			_ -> ok
		    end;
		false -> ok %% GUID has changed, do nothing
	    end
    end.

get_locked_flags(ExistingObject, OrigName0) ->
    case ExistingObject =:= undefined of
	true -> [];
	false ->
	    case ExistingObject#object.orig_name =:= OrigName0 of
		false -> [];
		true -> %% name has not changed, copy lock attributes
		    LockUserTel =
			case ExistingObject#object.lock_user_tel of
			    undefined -> undefined;
			    _ -> utils:hex(ExistingObject#object.lock_user_tel)
			end,
		    [{is_locked, ExistingObject#object.is_locked},
		     {lock_modified_utc, ExistingObject#object.lock_modified_utc},
		     {lock_user_id, ExistingObject#object.lock_user_id},
		     {lock_user_name, utils:hex(ExistingObject#object.lock_user_name)},
		     {lock_user_tel, LockUserTel}]
	    end
    end.

get_guid(_OldGUID, undefined, undefined, _IsConflict) -> erlang:binary_to_list(riak_crypto:uuid4());
get_guid(_OldGUID, NewGUID, undefined, _IsConflict) -> NewGUID;
get_guid(_OldGUID, _NewGUID, ExistingObject, false) -> ExistingObject#object.guid;
%% User could have edited conflicted copy. In that case
%% GUID remains the same as previous conflicted copy.
get_guid(undefined, _NewGUID, _ExistingObject, true) -> erlang:binary_to_list(riak_crypto:uuid4());
get_guid(OldGUID, _NewGUID, _ExistingObject, true) -> OldGUID.

upload_to_riak(Req0, State0, BinaryData) ->
    BucketId = proplists:get_value(bucket_id, State0),
    Prefix = proplists:get_value(prefix, State0),
    FileName = proplists:get_value(file_name, State0),
    ModifiedTime0 = proplists:get_value(last_modified_utc, State0),
    LastSeenModifiedTime = proplists:get_value(last_seen_modified_utc, State0),
    User = proplists:get_value(user, State0),
    %% Create a bucket if it do not exist
    case riak_api:head_bucket(BucketId) of
	not_found -> riak_api:create_bucket(BucketId);
	_ -> ok
    end,
    IndexContent = indexing:get_index(BucketId, Prefix),
    UserName = utils:unhex(erlang:list_to_binary(User#user.name)),
    {ObjectKey0, OrigName0, IsNewVersion0, ExistingObject0, IsConflict} = riak_api:pick_object_key(BucketId, Prefix,
	FileName, ModifiedTime0, LastSeenModifiedTime, UserName, IndexContent),
    {IsLocked, LockUserId, LockUserName, LockUserTel0, LockModifiedTime} =
	case ExistingObject0 of
	    undefined -> {false, undefined, undefined, undefined, undefined};
	    _ -> {ExistingObject0#object.is_locked,
		  ExistingObject0#object.lock_user_id,
		  ExistingObject0#object.lock_user_name,
		  ExistingObject0#object.lock_user_tel,
		  ExistingObject0#object.lock_modified_utc
		}
	end,
    case IsNewVersion0 of
	false -> js_handler:not_modified(Req0);
	true ->
	    State1 = [{is_conflict, IsConflict}],
	    case IsLocked of
		true ->
		    case LockUserId =/= undefined andalso User#user.id =/= LockUserId of
			true ->
                            LockUserTel1 =
                                case LockUserTel0 of
                                    undefined -> null;
                                    V -> V
                                end,
			    LockData = [{is_locked, <<"true">>},
					{lock_user_id, erlang:list_to_binary(LockUserId)},
					{lock_user_name, LockUserName},
					{lock_user_tel, LockUserTel1},
					{lock_modified_utc, LockModifiedTime}],
			    Req1 = cowboy_req:reply(423, #{
				<<"content-type">> => <<"application/json">>
			    }, jsx:encode(LockData), Req0),
			    {stop, Req1, []};
			false ->
			    ExistingObject1 = ExistingObject0#object{is_locked="true"},
			    upload_to_riak(Req0, State0 ++ State1, BinaryData, ExistingObject1, ObjectKey0, OrigName0)
		    end;
		_ -> upload_to_riak(Req0, State0 ++ State1, BinaryData, ExistingObject0, ObjectKey0, OrigName0)
	    end
    end.

upload_to_riak(Req0, State0, BinaryData, ExistingObject, ObjectKey0, OrigName0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    ModifiedTime0 = proplists:get_value(last_modified_utc, State0),
    User = proplists:get_value(user, State0),
    AuthorId =
	case ExistingObject of
	    undefined -> User#user.id;
	    _ -> ExistingObject#object.author_id
	end,
    AuthorName =
	case ExistingObject of
	    undefined -> User#user.name;
	    _ -> utils:hex(ExistingObject#object.author_name)
	end,
    AuthorTel =
	case ExistingObject of
	    undefined -> User#user.tel;
	    _ -> utils:hex(ExistingObject#object.author_tel)
	end,
    IsConflict = proplists:get_value(is_conflict, State0),
    Prefix = proplists:get_value(prefix, State0),
    %% The destination object GUID can be different, for example, after user has edited conflict copy
    {ObjectToOverwriteGUID, ObjectToOverwriteTime} =
	case riak_api:head_object(BucketId, utils:prefixed_object_key(Prefix, ObjectKey0)) of
	    not_found -> {undefined, undefined};
	    ConflictedMeta ->
		OrigName1 = utils:unhex(erlang:list_to_binary(
		    proplists:get_value("x-amz-meta-orig-filename", ConflictedMeta))),
		case OrigName1 =:= OrigName0 of
		    true -> {proplists:get_value("x-amz-meta-guid", ConflictedMeta),
			     erlang:list_to_integer(proplists:get_value("x-amz-meta-modified-utc", ConflictedMeta))};
		    false -> {undefined, undefined}  %% object with a new name has been uploaded
		end

	end,
    GUID = get_guid(ObjectToOverwriteGUID, proplists:get_value(guid, State0), ExistingObject, IsConflict),
    State1 = [{orig_name, OrigName0}, {object, ExistingObject}],
    PartNumber = proplists:get_value(part_number, State0),
    IsBig = proplists:get_value(is_big, State0),
    case IsBig of
	true ->
	    case (PartNumber > 1) of
		true -> check_part(Req0, State0 ++ State1, BinaryData);
		false ->
		    State2 = lists:keyreplace(guid, 1, State0, {guid, GUID}),
		    start_upload(Req0, State1 ++ State2, BinaryData)
	    end;
	false ->
	    Etags = proplists:get_value(etags, State0),
	    case validate_md5(Etags, BinaryData) of
		false -> js_handler:bad_request(Req0, 40);
		true ->
		    %% check if conflict
		    TotalBytes = byte_size(BinaryData),
		    UploadTime = proplists:get_value(upload_time, State0),
		    %% Put object under service prefix
		    Meta0 = get_locked_flags(ExistingObject, OrigName0),
		    Meta1 = list_handler:parse_object_record([], Meta0 ++ [
				{orig_name, utils:hex(OrigName0)},
				{last_modified_utc, ModifiedTime0},
				{upload_time, UploadTime},
				{guid, GUID},
				{author_id, AuthorId},
				{author_name, AuthorName},
				{author_tel, AuthorTel},
				{is_deleted, "false"},
				{bytes, utils:to_list(TotalBytes)},
				{width, proplists:get_value(width, State0)},
				{height, proplists:get_value(height, State0)}]),
		    Options = [{acl, public_read}, {meta, Meta1}],
		    %% Put object to real path ( i.e. ~object/ff1b69e5-7c23-4611-b0ca-65cab048073f/1575541599992 )
		    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
		    case riak_api:put_object(BucketId, RealPrefix, erlang:integer_to_list(ModifiedTime0),
					     BinaryData, Options) of
			ok ->
			    case ExistingObject of
				undefined -> ok;
				_ ->
				    case IsConflict of
				        true -> delete_previous_one(BucketId, ObjectToOverwriteTime,
								    ModifiedTime0, GUID, GUID); %% delete older conflict
					false -> delete_previous_one(BucketId, ExistingObject#object.last_modified_utc,
								     ModifiedTime0, ExistingObject#object.guid, GUID)
				    end
			    end,
			    State3 = lists:keyreplace(total_bytes, 1, State0, {total_bytes, TotalBytes}),
			    State4 = lists:keyreplace(guid, 1, State3, {guid, GUID}),
			    update_index(Req0, State1 ++ State4 ++ [{object_key, ObjectKey0}]);
			_ -> js_handler:incorrect_configuration(Req0, "Something's went horribly wrong.")
		    end
	    end
    end.

%%
%% Checks if upload with provided ID exists and uploads `BinaryData` to Riak CS
%%
check_part(Req0, State, BinaryData) ->
    BucketId = proplists:get_value(bucket_id, State),
    UploadId = proplists:get_value(upload_id, State),
    GUID = proplists:get_value(guid, State),
    ModifiedTime = proplists:get_value(last_modified_utc, State),
    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
    RealPath = utils:prefixed_object_key(RealPrefix, erlang:integer_to_list(ModifiedTime)),
    case riak_api:validate_upload_id(BucketId, RealPath, UploadId) of
	not_found -> js_handler:bad_request(Req0, 4);
	{error, _Reason} -> js_handler:bad_request(Req0, 5);
	_ -> upload_part(Req0, State, BinaryData)
    end.

%% @todo: get rid of utils:to_list() by using proper xml binary serialization
parse_etags([K,V | T]) -> [{
	utils:to_integer(K),
	utils:to_list(<<  <<$">>/binary, V/binary, <<$">>/binary >>)
    } | parse_etags(T)];
parse_etags([]) -> [].

upload_part(Req0, State0, BinaryData) ->
    BucketId = proplists:get_value(bucket_id, State0),
    Prefix = proplists:get_value(prefix, State0),
    UploadId = proplists:get_value(upload_id, State0),
    PartNumber = proplists:get_value(part_number, State0),
    Etags0 = proplists:get_value(etags, State0),
    EndByte = proplists:get_value(end_byte, State0),
    TotalBytes = proplists:get_value(total_bytes, State0),
    GUID = proplists:get_value(guid, State0),

    ModifiedTime0 = proplists:get_value(last_modified_utc, State0),
    LastSeenModifiedTime = proplists:get_value(last_seen_modified_utc, State0),
    User = proplists:get_value(user, State0),

    %% Get the real prefix file should be uploaded to
    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
    RealPath = utils:prefixed_object_key(RealPrefix, erlang:integer_to_list(ModifiedTime0)),
    case riak_api:upload_part(BucketId, RealPath, UploadId, PartNumber, BinaryData) of
	{ok, [{_, NewEtag0}]} ->
	    case (EndByte+1 =:= TotalBytes) of
		true ->
		    case Etags0 =:= undefined of
			true -> js_handler:bad_request(Req0, 5);
			false ->
			    %% parse etags from request to complete upload
			    Etags1 = parse_etags(binary:split(Etags0, <<$,>>, [global])),
			    case riak_api:complete_multipart(BucketId, RealPath, UploadId, Etags1) of
				ok ->
				    %% Pick object key again, as it could have been taken by now
				    Metadata = riak_api:head_object(BucketId, RealPath),
				    FinalEtag = proplists:get_value(etag, Metadata),
				    OrigName0 = proplists:get_value("x-amz-meta-orig-filename", Metadata),
				    OrigName1 = utils:unhex(erlang:list_to_binary(OrigName0)),
				    UploadTime = erlang:round(utils:timestamp()/1000),
				    IsLocked = proplists:get_value("x-amz-meta-is-locked", Metadata, "false"),

				    IndexContent = indexing:get_index(BucketId, Prefix),
				    UserName = utils:unhex(erlang:list_to_binary(User#user.name)),
				    {ObjectKey0, OrigName2, _IsNewVersion, ExistingObject, IsConflict} =
					riak_api:pick_object_key(BucketId, Prefix, OrigName1, ModifiedTime0,
								 LastSeenModifiedTime, UserName, IndexContent),
				    ObjectToOverwriteTime =
					case riak_api:head_object(BucketId,
								  utils:prefixed_object_key(Prefix, ObjectKey0)) of
					    not_found -> undefined;
					    ConflictedMeta ->
						T = proplists:get_value("x-amz-meta-modified-utc", ConflictedMeta),
						erlang:list_to_integer(T)
					end,
				    case ExistingObject of
					undefined -> ok;
					_ ->
					    case IsConflict of
				    		true ->  %% delete older conflict
						    delete_previous_one(BucketId, ObjectToOverwriteTime,
									ModifiedTime0, GUID, GUID);
						false ->
						    delete_previous_one(BucketId, ExistingObject#object.last_modified_utc,
									ModifiedTime0, ExistingObject#object.guid, GUID)
					    end
				    end,
				    %% Create object key in destination prefix and update index
				    State1 = [{user, proplists:get_value(user, State0)}, {bucket_id, BucketId},
					      {prefix, Prefix}, {object_key, ObjectKey0}, {orig_name, OrigName2},
					      {last_modified_utc, ModifiedTime0}, {object, ExistingObject},
					      {is_locked, IsLocked}, {upload_time, UploadTime}, {guid, GUID},
					      {total_bytes, TotalBytes}, {is_conflict, IsConflict},
					      {upload_id, unicode:characters_to_binary(UploadId)},
					      {end_byte, EndByte}, {width, proplists:get_value(width, State0)},
					      {height, proplists:get_value(height, State0)},
					      {md5, unicode:characters_to_binary(string:strip(FinalEtag, both, $"))}],
				    update_index(Req0, State1)
			    end
		    end;
		false ->
		    <<_:1/binary, NewEtag1:32/binary, _:1/binary>> = unicode:characters_to_binary(NewEtag0),
		    Response = [
			{upload_id, unicode:characters_to_binary(UploadId)},
			{end_byte, EndByte},
			{md5, NewEtag1},
			{guid, unicode:characters_to_binary(GUID)}],
		    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
		    {true, Req1, []}
	    end;
	{error, _} -> js_handler:bad_request(Req0, 6)
    end.

%%
%% Creates identifier and uploads first part of data
%%
start_upload(Req0, State, BinaryData) ->
    User = proplists:get_value(user, State),
    OrigName0 = proplists:get_value(orig_name, State),
    ExistingObject = proplists:get_value(object, State),
    BucketId = proplists:get_value(bucket_id, State),
    EndByte = proplists:get_value(end_byte, State),
    %% The filename is used to pick object name when upload finishes
    FileName = proplists:get_value(file_name, State),
    ModifiedTime = proplists:get_value(last_modified_utc, State),
    UploadTime = proplists:get_value(upload_time, State),
    GUID = proplists:get_value(guid, State),
    TotalBytes = proplists:get_value(total_bytes, State),

    %% Get the real prefix file should be uploaded to
    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
    RealPath = utils:prefixed_object_key(RealPrefix, erlang:integer_to_list(ModifiedTime)),
    Meta0 = get_locked_flags(ExistingObject, OrigName0),
    Meta1 = list_handler:parse_object_record([], Meta0 ++ [
		{orig_name, utils:hex(FileName)},
		{last_modified_utc, ModifiedTime},
		{upload_time, UploadTime},
		{guid, GUID},
		{author_id, User#user.id},
		{author_name, User#user.name},
		{author_tel, User#user.tel},
		{is_deleted, "false"},
		{bytes, utils:to_list(TotalBytes)},
		{width, proplists:get_value(width, State)},
		{height, proplists:get_value(height, State)}
	]),
    Options = [{acl, public_read}, {meta, Meta1}],
    MimeType = utils:mime_type(unicode:characters_to_list(FileName)),
    Headers = [{"content-type", MimeType}],
    {ok, [{_, UploadId}]} = riak_api:start_multipart(BucketId, RealPath, Options, Headers),
    {ok, [{_, Etag0}]} = riak_api:upload_part(BucketId, RealPath, UploadId, 1, BinaryData),

    %% Remove quotes from md5
    <<_:1/binary, Etag1:32/binary, _:1/binary>> = unicode:characters_to_binary(Etag0),
    Response = [
	{upload_id, unicode:characters_to_binary(UploadId)},
	{last_modified_utc, ModifiedTime},
	{end_byte, EndByte},
	{md5, Etag1},
	{guid, unicode:characters_to_binary(GUID)}],
    Req1 = cowboy_req:set_resp_body(jsx:encode(Response), Req0),
    {true, Req1, []}.

%%
%% Checks if provided token is correct.
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, State) ->
    list_handler:is_authorized(Req0, State).

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
    case cowboy_req:header(<<"content-range">>, Req) of
	undefined ->
	    [{part_number, PartNumber},
	     {upload_id, UploadId0},
	     {start_byte, undefined},
	     {end_byte, undefined},
	     {total_bytes, undefined}];
	Value ->
	    try cow_http_hd:parse_content_range(Value) of
		{bytes, Start, End, Total} ->
		    case Total > ?FILE_MAXIMUM_SIZE of
			true -> {error, 24};
			false ->
			    case PartNumber > 1 andalso UploadId0 =:= undefined of
				true -> {error, 25};
				false -> [{part_number, PartNumber}, {upload_id, UploadId0},
					  {start_byte, Start}, {end_byte, End},
					  {total_bytes, Total}]
			    end
		    end
	    catch error:function_clause ->
		{error, 25}
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
forbidden(Req0, User) ->
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
	true ->
	    UserBelongsToGroup = lists:any(fun(Group) ->
		utils:is_bucket_belongs_to_group(BucketId, User#user.tenant_id, Group#group.id) end,
		User#user.groups),
	    case UserBelongsToGroup of
		false ->
		    PUser = admin_users_handler:user_to_proplist(User),
		    js_handler:forbidden(Req0, 37, proplists:get_value(groups, PUser));
		true ->
		    case validate_content_range(Req0) of
			{error, Reason} -> js_handler:forbidden(Req0, Reason);
			State1 -> {false, Req0, State1++[{user, User}, {bucket_id, BucketId}]}
		    end
	    end;
	false -> js_handler:forbidden(Req0, 7)
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
