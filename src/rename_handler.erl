%%%
%%% Changes name of an object in .riak_index.etf
%%%
%%% See js_handler.erl to learn what error code stands for.
%%%
-module(rename_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
	 to_json/2, allowed_methods/2, forbidden/2, is_authorized/2,
	 validate_post/3, handle_post/2, rename_pseudo_directory/5]).

-include("riak.hrl").
-include("user.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Returns callback 'to_json()'
%% ( called after 'forbidden()' )
%%
content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, []}, to_json}
    ], Req, State}.

%%
%% Returns callback 'handle_post()'
%% ( called after 'resource_exists()' )
%%
content_types_accepted(Req, State) ->
    case cowboy_req:method(Req) of
	<<"POST">> -> {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State};
	_ -> {[], Req, State}
    end.

validate_src_object_key(BucketId, Prefix0, SrcObjectKey0) ->
    case list_handler:validate_prefix(BucketId, Prefix0) of
	{error, Number0} -> {error, Number0};
	Prefix1 ->
	    case utils:ends_with(SrcObjectKey0, <<"/">>) of
		true ->
		    case list_handler:validate_prefix(BucketId,
			    utils:prefixed_object_key(Prefix0, SrcObjectKey0)) of
			{error, Number1} -> {error, Number1};
			SrcObjectKey1 -> {Prefix1, SrcObjectKey1}
		    end;
		false -> {Prefix1, string:to_lower(erlang:binary_to_list(SrcObjectKey0))}
	    end
    end.


validate_post(Req, BucketId, FieldValues) ->
    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
    SrcObjectKey0 = proplists:get_value(<<"src_object_key">>, FieldValues),
    DstObjectName0 = proplists:get_value(<<"dst_object_name">>, FieldValues),
    case (SrcObjectKey0 =:= undefined orelse DstObjectName0 =:= undefined) of
	true -> js_handler:bad_request(Req, 9);
	false ->
	    case validate_src_object_key(BucketId, Prefix0, SrcObjectKey0) of
		{error, Number} -> js_handler:bad_request(Req, Number);
		{Prefix1, SrcObjectKey1} ->
		    case utils:is_valid_object_key(DstObjectName0) of
			false -> js_handler:bad_request(Req, 9);
			true ->
			    [{prefix, Prefix1},
			     {src_object_key, SrcObjectKey1},
			     {dst_object_name, DstObjectName0}]
		    end
	    end
    end.

%%
%% Validates provided fields and updates object index, in case of object rename
%% or moves objects to the new prefix, in case prefix rename requested.
%%
handle_post(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case jsx:is_json(Body) of
	{error, badarg} -> js_handler:bad_request(Req1, 21);
	false -> js_handler:bad_request(Req1, 21);
	true ->
	    FieldValues = jsx:decode(Body),
	    case validate_post(Req1, BucketId, FieldValues) of
		{true, Req3, []} -> {true, Req3, []};  % error
		State1 -> rename(Req0, BucketId, State0 ++ State1)
	    end
    end.

%%
%% Copies file and checks the status of copy.
%% Returns ok in case of success. Otherwise returns filename.
%%
copy_delete(BucketId, PrefixedSrcDirectoryName, PrefixedDstDirectoryName, PrefixedObjectKey0) ->
    PrefixedObjectKey1 = re:replace(PrefixedObjectKey0, "^" ++ PrefixedSrcDirectoryName, "", [{return, list}]),
    %% We have cheked previously that destination directory do not exist
    PrefixedObjectKey2 = utils:prefixed_object_key(PrefixedDstDirectoryName, PrefixedObjectKey1),

    CopyResult = riak_api:copy_object(BucketId, PrefixedObjectKey2, BucketId, PrefixedObjectKey0, [{acl, public_read}]),
    case proplists:get_value(content_length, CopyResult, 0) == 0 of
	true -> PrefixedObjectKey2;
	false ->
	    %% Delete regular object
	    riak_api:delete_object(BucketId, PrefixedObjectKey0),
	    PrefixedObjectKey2,
	    ok
    end.

check_src_dir(BucketId, Prefix) ->
    %% Check if correct source directory provided
    PrefixedIndexFilename = utils:prefixed_object_key(Prefix, ?RIAK_INDEX_FILENAME),
    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
	not_found -> {error, 9};
	IndexContent0 -> erlang:binary_to_term(proplists:get_value(content, IndexContent0))
    end.

%%
%% Checks if pseudo-directory exists, by requesting its index object and
%% then checking if requested name is present.
%%
validate_dst_name(BucketId, Prefix, DstName0, IndexContent)
	when erlang:is_binary(DstName0) ->
    %% Remove "/" at the end, if present
    DstName1 =
	case utils:ends_with(DstName0, <<"/">>) of
	    true ->
		Size0 = byte_size(DstName0)-1,
		<<DstName3:Size0/binary, _/binary>> = DstName0,
		DstName3;
	    false -> DstName0
	end,
    case IndexContent of
	{error, Number} -> {error, Number};
	IndexContent1 ->
	    %% Check if object with that name exists already
	    case indexing:object_exists(IndexContent1, DstName1) of
		true -> {error, 29};
		false ->
		    %% Check if pseudo-directory with that name exists already
		    PrefixedName =
			case Prefix of
			    undefined -> utils:hex(DstName1);
			    _ ->
				DstName2 = utils:hex(DstName1),
				utils:prefixed_object_key(Prefix, DstName2)
			end,
		    case indexing:pseudo_directory_exists(IndexContent1, PrefixedName++"/") of
			true -> {error, 10};
			false ->
			    case indexing:update(BucketId, Prefix, [{uncommitted, true}]) of
				lock -> lock;
				_ -> PrefixedName
			    end
		    end
	    end
    end.

%%
%% Rename can be performed within one bucket only for now.
%%
%% Prefix0 -- current pseudo-directory
%%
%% SrcDirectoryName0 -- hex encoded pseudo-directory, that should be renamed
%%
-spec rename_pseudo_directory(BucketId, Prefix, PrefixedSrcDirectoryName, DstDirectoryName, ActionLogRecord) ->
    not_found|exists|true when
    BucketId :: string(),
    Prefix :: string()|undefined,
    PrefixedSrcDirectoryName :: string(),
    DstDirectoryName :: binary(),
    ActionLogRecord :: riak_action_log_record().

rename_pseudo_directory(BucketId, Prefix0, PrefixedSrcDirectoryName, DstDirectoryName0, ActionLogRecord0)
	when erlang:is_list(BucketId), erlang:is_list(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_list(PrefixedSrcDirectoryName), erlang:is_binary(DstDirectoryName0) ->
    IndexContent = check_src_dir(BucketId, Prefix0),
    case validate_dst_name(BucketId, Prefix0, DstDirectoryName0, IndexContent) of
	{error, Number} -> {error, Number};
	lock -> lock;
	PrefixedDstDirectoryName0 ->
	    PrefixedDstDirectoryName1 = PrefixedDstDirectoryName0++"/",
	    List0 = riak_api:recursively_list_pseudo_dir(BucketId, PrefixedSrcDirectoryName),
	    RenameResult0 = [copy_delete(BucketId, PrefixedSrcDirectoryName, PrefixedDstDirectoryName1,
		PrefixedObjectKey) || PrefixedObjectKey <- List0,
		lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectKey) =/= true],
	    %% Update indices for nested pseudo-directories
	    PseudoDirectoryMoveResult = lists:map(
		fun(PrefixedObjectKey) ->
		    case lists:suffix(?RIAK_INDEX_FILENAME, PrefixedObjectKey) of
			false -> ok;
			true ->
			    SrcPrefix =
				case filename:dirname(PrefixedObjectKey) of
				    "." -> undefined;
				    P0 -> P0++"/"
				end,
			    DstKey0 = re:replace(PrefixedObjectKey, "^"++PrefixedSrcDirectoryName,
						 "", [{return, list}]),
			    DstPrefix =
				case utils:prefixed_object_key(PrefixedDstDirectoryName1, DstKey0) of
				    "." -> undefined;
				    P1 ->
					case filename:dirname(P1) of
					    "." -> undefined;
					    P2 -> P2++"/"
					end
				end,
			    case indexing:update(BucketId, DstPrefix, [{copy_from, [
						   {bucket_id, BucketId}, {prefix, SrcPrefix}]}]) of
				lock -> filename:dirname(PrefixedObjectKey);
				_ ->
				    riak_api:delete_object(BucketId, PrefixedObjectKey),
				    ok
			    end
		    end
		end, List0),
		RenameErrors1 = [I || I <- PseudoDirectoryMoveResult, I =/= ok],
		RenameErrors2 = [I || I <- RenameResult0, I =/= ok],
		case length(RenameErrors1) =:= 0 andalso length(RenameErrors2) =:= 0 of
		    true ->
			DstDirectoryName1 = utils:hex(DstDirectoryName0),
			case ActionLogRecord0#riak_action_log_record.action of
			    "delete" ->
				Timestamp = ActionLogRecord0#riak_action_log_record.timestamp,
				case indexing:update(BucketId, Prefix0, [{to_delete,
				    [{erlang:list_to_binary(DstDirectoryName1++"/"), Timestamp}]}]) of
				lock -> lock;
				_ ->
				    DstDirectoryName2 = unicode:characters_to_list(DstDirectoryName0),
				    Summary0 = lists:flatten([["Deleted directory \""], DstDirectoryName2, ["/\"."]]),
				    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
				    action_log:add_record(BucketId, Prefix0, ActionLogRecord1),
				    {dir_name, DstDirectoryName0}
				end;
			    _ ->
				%% Update pseudo-directory index
				case indexing:update(BucketId, Prefix0) of
				    lock -> lock;
				    _ ->
					SrcObjectKey0 = filename:basename(PrefixedSrcDirectoryName),
					SrcObjectKey1 = unicode:characters_to_list(utils:unhex(erlang:list_to_binary(SrcObjectKey0))),
					DstDirectoryName2 = unicode:characters_to_list(DstDirectoryName0),
					Summary0 = lists:flatten([["Renamed \""], [SrcObjectKey1, "\" to \"", DstDirectoryName2, "\""]]),
					ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
					action_log:add_record(BucketId, Prefix0, ActionLogRecord1),
					{dir_name, DstDirectoryName0}
				end
			end;
		    false -> {accepted, {RenameErrors1, RenameErrors2}}
		end
    end.

rename_object(BucketId, Prefix0, SrcObjectKey0, DstObjectName0, ActionLogRecord0) ->
    PrefixedSrcObjectKey = utils:prefixed_object_key(Prefix0, SrcObjectKey0),
    IndexContent = check_src_dir(BucketId, Prefix0),
    case validate_dst_name(BucketId, Prefix0, DstObjectName0, IndexContent) of
	{error, Number} -> {error, Number};
	lock -> lock;
	_ ->
	    %% We can't just update index with new name, as next file uploads/renames
	    %% with the same object names would complain on existing objects.
	    {DstObjectKey, OrigName0, _} = riak_api:pick_object_key(BucketId, Prefix0, DstObjectName0,
                                                                    0, IndexContent),
	    PrefixedDstObjectKey = utils:prefixed_object_key(Prefix0, DstObjectKey),
	    CopyResult = riak_api:copy_object(BucketId, PrefixedDstObjectKey,
                                              BucketId, PrefixedSrcObjectKey, [{acl, public_read}]),
	    case proplists:get_value(content_length, CopyResult, 0) =:= "0" of
		true -> {error, 35};
		false ->
		    %% Delete regular object
		    riak_api:delete_object(BucketId, PrefixedSrcObjectKey),
		    %% Find original object name for action log record
		    ObjectRecord = lists:nth(1, [I || I <- proplists:get_value(list, IndexContent),
			proplists:get_value(object_key, I) =:= erlang:list_to_binary(SrcObjectKey0)]),
		    PreviousOrigName = unicode:characters_to_list(proplists:get_value(orig_name, ObjectRecord)),
		    ObjectBytes = proplists:get_value(bytes, ObjectRecord),
		    %% Update objects index
		    case indexing:update(BucketId, Prefix0,
					 [{renamed, [{DstObjectKey, [{name, unicode:characters_to_list(OrigName0)},
								     {bytes, ObjectBytes}]}]}]) of
			lock -> lock;
			_ ->
			    %% Create action log record
			    OrigName1 = unicode:characters_to_list(OrigName0),
			    Summary1 = lists:flatten([["Renamed \""], [PreviousOrigName], ["\" to \""],
						     [OrigName1], ["\""]]),
			    ActionLogRecord2 = ActionLogRecord0#riak_action_log_record{details=Summary1},
			    action_log:add_record(BucketId, Prefix0, ActionLogRecord2),
			    [{orig_name, unicode:characters_to_binary(OrigName1)}]
		    end
	    end
    end.

rename(Req0, BucketId, State) ->
    Prefix0 = proplists:get_value(prefix, State),
    SrcObjectKey0 = proplists:get_value(src_object_key, State),
    DstObjectName0 = proplists:get_value(dst_object_name, State),
    User = proplists:get_value(user, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="rename",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()])
    },
    case string:sub_string(SrcObjectKey0, length(SrcObjectKey0), length(SrcObjectKey0)) =:= "/" of
	true ->
	    case rename_pseudo_directory(BucketId, Prefix0, SrcObjectKey0, DstObjectName0, ActionLogRecord0) of
		lock -> js_handler:too_many(Req0);
		{accepted, {RenameErrors1, RenameErrors2}} ->
		    %% Rename is not complete, as Riak CS was busy.
		    %% Return names of objects that were not copied.
		    Req1 = cowboy_req:reply(202, #{
			<<"content-type">> => <<"application/json">>
		    }, jsx:encode([{dir_errors, RenameErrors1}, {object_errors, RenameErrors2}]), Req0),
		    {true, Req1, []};
		{error, Number} -> js_handler:bad_request(Req0, Number);
		{dir_name, DstDirectoryName0} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{dir_name, DstDirectoryName0}]), Req0),
		    {true, Req1, []}
	    end;
	false ->
	    case rename_object(BucketId, Prefix0, SrcObjectKey0, DstObjectName0, ActionLogRecord0) of
		{error, Number} -> js_handler:bad_request(Req0, Number);
		lock -> js_handler:too_many(Req0);
		Body ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode(Body), Req0),
		    {true, Req1, []}
	    end
    end.

%%
%% Serializes response to json
%%
to_json(Req0, State) ->
    {<<"{\"status\": \"ok\"}">>, Req0, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

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

%%
%% Checks if user has access
%% ( called after 'is_authorized()' )
%%
forbidden(Req0, State) ->
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    case utils:is_valid_bucket_id(BucketId, State#user.tenant_id) of
	true ->
	    UserBelongsToGroup = lists:any(fun(Group) ->
		utils:is_bucket_belongs_to_group(BucketId, State#user.tenant_id, Group#group.id) end,
		State#user.groups),
	    case UserBelongsToGroup of
		false -> {true, Req0, []};
		true -> {false, Req0, [{user, State}, {bucket_id, BucketId}]}
	    end;
	false -> {true, Req0, []}
    end.
