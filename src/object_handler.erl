%%
%% Allows to delete object or to get its attributes.
%%
-module(object_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_json/2,
    allowed_methods/2, is_authorized/2, forbidden/2,
    resource_exists/2, previously_existed/2]).

-include("riak.hrl").
-include("user.hrl").
-include("action_log.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

to_json(Req0, State) ->
    ObjectKey0 = proplists:get_value(object_key, State),
    Prefix0 = proplists:get_value(prefix, State),
    case proplists:get_value(object_meta, State) of
	undefined -> {<<"[]">>, Req0, []};  %% Pseudo-directory
	ObjectMeta ->
	    UploadTimestamp = proplists:get_value(upload_time, ObjectMeta),
	    ModifiedTime = proplists:get_value(last_modified_utc, ObjectMeta),
	    OrigName = proplists:get_value(orig_name, ObjectMeta),
	    ContentType = erlang:list_to_binary(proplists:get_value(content_type, ObjectMeta)),
	    ContentLength = proplists:get_value(bytes, ObjectMeta),
	    AccessToken =
		case utils:starts_with(ContentType, <<"image/">>) of
		    false -> <<>>;
		    true -> proplists:get_value(access_token, ObjectMeta, <<"">>)
		end,
	    Prefix1 =
		case Prefix0 of
		    undefined -> <<>>;
		    _ -> Prefix0
		end,
	    {jsx:encode([
		{prefix, unicode:characters_to_binary(Prefix1)},
		{key, ObjectKey0},
		{orig_name, OrigName},
		{last_modified_utc, ModifiedTime},
		{uploaded, UploadTimestamp},
		{content_length, ContentLength},
		{content_type, ContentType},
		{access_token, AccessToken}
	    ]), Req0, []}
    end.

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

-spec get_object_meta(string(), string(), binary()) -> proplist()|not_found|pseudo_directory.

get_object_meta(BucketId, Prefix0, ObjectKey0)
	when erlang:is_list(BucketId),
	     erlang:is_list(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_binary(ObjectKey0) orelse ObjectKey0 =:= undefined ->
    case ObjectKey0 of
	undefined -> not_found;
	_ ->
	    List0 = indexing:get_index(BucketId, Prefix0),
	    case indexing:get_object_record(List0, ObjectKey0) of
		[] ->
		    %% Check if pseudo-directory exists
		    ObjectKey1 =
			case utils:ends_with(ObjectKey0, <<"/">>) of
			    true -> ObjectKey0;
			    false -> << ObjectKey0/binary, <<"/">>/binary >>
			end,
		    PrefixedObjectKey = utils:prefixed_object_key(Prefix0, erlang:binary_to_list(ObjectKey1)),
		    case indexing:pseudo_directory_exists(List0, PrefixedObjectKey) of
			false -> not_found;
			true -> pseudo_directory
		    end;
		ObjectRecord -> ObjectRecord
	    end
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    case cowboy_req:method(Req0) of
	<<"GET">> ->
	    ParsedQs = cowboy_req:parse_qs(Req0),
	    Prefix0 = proplists:get_value(<<"prefix">>, ParsedQs),
	    case list_handler:validate_prefix(BucketId, Prefix0) of
		{error, Number} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req0),
		    {false, Req1, []};
		Prefix1 ->
		    ObjectKey0 = proplists:get_value(<<"object_key">>, ParsedQs),
		    State1 = [{prefix, Prefix1}, {object_key, ObjectKey0}],
		    case get_object_meta(BucketId, Prefix1, ObjectKey0) of
			not_found -> {false, Req0, []};
			pseudo_directory -> {true, Req0, State0 ++ State1};
			ObjectMeta0 -> {true, Req0, State0 ++ State1 ++ [{object_meta, ObjectMeta0}]}
		    end
	    end;
	<<"DELETE">> -> object_exists(Req0, State0)
    end.

object_exists(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case jsx:is_json(Body) of
	{error, badarg} -> js_handler:bad_request(Req1, 21);
	false -> js_handler:bad_request(Req1, 21);
	true ->
	    FieldValues = jsx:decode(Body),
	    case list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, FieldValues)) of
		{error, Number} ->
		    Req2 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req1),
		    {false, Req2, []};
		Prefix ->
		    ObjectKey = proplists:get_value(<<"object_key">>, FieldValues),
		    State1 = [{prefix, Prefix}, {object_key, ObjectKey}],
		    case get_object_meta(BucketId, Prefix, ObjectKey) of
			not_found -> {false, Req1, []};
			pseudo_directory -> {true, Req1, State0 ++ State1 ++ [{pseudo_directory, true}]};
			ObjectMeta -> {true, Req1, State0 ++ State1 ++ [{object_meta, ObjectMeta},
									{pseudo_directory, false}]}
		    end
	    end
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
