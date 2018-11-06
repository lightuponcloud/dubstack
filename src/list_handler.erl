%%
%% List handler allows the following requests
%%
%% GET
%%	Returns contetnts of index.etf
%%
%% PATCH
%%	Allows to mark objects as undeleted
%%
%% POST
%%	Create directory
%%
-module(list_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, content_types_accepted/2,
    to_json/2, allowed_methods/2, forbidden/2, is_authorized/2,
    resource_exists/2, previously_existed/2, patch_resource/2,
    validate_post/3, create_pseudo_directory/2, handle_post/2,
    validate_prefix/2]).

-include("riak.hrl").
-include("user.hrl").
-include("general.hrl").
-include("action_log.hrl").


init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PATCH">>, <<"POST">>], Req, State}.


content_types_provided(Req, State) ->
    {[
	{{<<"application">>, <<"json">>, '*'}, to_json}
    ], Req, State}.

content_types_accepted(Req, State) ->
    case cowboy_req:method(Req) of
	<<"PATCH">> ->
	    {[{{<<"application">>, <<"json">>, '*'}, patch_resource}], Req, State};
	<<"POST">> ->
	    {[{{<<"application">>, <<"json">>, '*'}, handle_post}], Req, State}
    end.

to_json(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    Prefix0 = proplists:get_value(prefix, State),
    %% TODO: replace UTC time with DVV
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    UTCTimestamp = calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}}) - 62167219200,
    case riak_api:head_bucket(BucketId) of
	not_found ->
	    %% Bucket is valid, but it do not exist yet
	    riak_api:create_bucket(BucketId),
	    {jsx:encode([{list, []}, {dirs, []}, {server_utc, UTCTimestamp}]), Req0, []};
	_ ->
	    Prefix1 = utils:to_lower(Prefix0),
	    PrefixedIndexFilename = utils:prefixed_object_name(Prefix1, ?RIAK_INDEX_FILENAME),
	    case riak_api:get_object(BucketId, PrefixedIndexFilename) of
		not_found ->
		    case Prefix1 of
			undefined -> {jsx:encode([{list, []}, {dirs, []}, {server_utc, UTCTimestamp}]), Req0, []};
			_ -> js_handler:not_found(Req0)
		    end;
		RiakResponse ->
		    List0 = erlang:binary_to_term(proplists:get_value(content, RiakResponse)),
			{jsx:encode([{list, proplists:get_value(list, List0)},
			{dirs, proplists:get_value(dirs, List0)},
			{server_utc, UTCTimestamp}
		    ]), Req0, []}
	    end
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
    BucketId = erlang:binary_to_list(cowboy_req:binding(bucket_id, Req0)),
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

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix0 = proplists:get_value(<<"prefix">>, ParsedQs),
    case utils:is_valid_hex_prefix(Prefix0) of
	false -> {false, Req0, []};
	true -> {true, Req0, State++[{prefix, Prefix0}]}
    end.

previously_existed(Req0, _State) ->
    {false, Req0, []}.

patch_resource(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    {ok, Attrs0, Req1} = cowboy_req:read_body(Req0),
    Attrs1 = jsx:decode(Attrs0),
    Prefix0 = proplists:get_value(<<"prefix">>, Attrs1),
    case proplists:get_value(<<"undelete">>, Attrs1) of
	undefined -> {true, Req1, []};
	ObjectsList0 ->
	    ObjectsList1 = [utils:to_list(N) || N <- ObjectsList0, byte_size(N) < 255],
	    riak_index:update(BucketId, Prefix0, [{undelete, ObjectsList1}]),
	    {true, Req1, []}
    end.

%%
%% Validates prefix from POST request.
%%
-spec validate_prefix(undefined|list(), undefined|list()) -> list()|{error, integer}.

validate_prefix(undefined, undefined) -> undefined;
validate_prefix(BucketId, Prefix0) when erlang:is_list(BucketId),
	erlang:is_binary(Prefix0) orelse Prefix0 =:= undefined ->
    case utils:is_valid_hex_prefix(Prefix0) of
	true ->
	    %% Check if prefix exists
	    Prefix1 = utils:to_lower(Prefix0),
	    case Prefix1 of
		undefined -> undefined;
		_ ->
		    PrefixedIndexFilename = utils:prefixed_object_name(Prefix1, ?RIAK_INDEX_FILENAME),
		    case riak_api:head_object(BucketId, PrefixedIndexFilename) of
			not_found -> {error, 11};
			_ -> Prefix1
		    end
	    end;
	false -> {error, 11}
    end.

validate_directory_name(_BucketId, _Prefix, null) -> {error, 12};
validate_directory_name(_BucketId, _Prefix, undefined) -> {error, 12};
validate_directory_name(BucketId, Prefix0, DirectoryName0)
	when erlang:is_list(BucketId),
	     erlang:is_binary(Prefix0) orelse Prefix0 =:= undefined,
	     erlang:is_binary(DirectoryName0) ->
    case utils:is_valid_object_name(DirectoryName0) of
	true ->
	    case validate_prefix(BucketId, Prefix0) of
		{error, Number} -> {error, Number};
		Prefix1 ->
		    IndexContent = riak_index:get_index(BucketId, Prefix1),
		    ExistingPrefixes = lists:map(
			fun(P0) ->
			    P1 = proplists:get_value(prefix, P0),
			    P2 = unicode:characters_to_list(utils:unhex_path(erlang:binary_to_list(P1))),
			    ux_string:to_lower(P2)
			end, proplists:get_value(dirs, IndexContent, [])),
		    DirectoryName1 = ux_string:to_lower(unicode:characters_to_list(DirectoryName0)),
		    case lists:member(DirectoryName1, ExistingPrefixes) of
			true -> {error, 10};
			false ->
			    HexDirectoryName = utils:hex(DirectoryName0),
			    PrefixedDirectoryName = utils:prefixed_object_name(Prefix1, HexDirectoryName),
			    {PrefixedDirectoryName, Prefix1, DirectoryName0}
		    end
	    end;
	false -> {error, 12}
    end.

%%
%% Checks if directory name and prefix are correct
%%
validate_post(Req, Body, BucketId) ->
    case jsx:is_json(Body) of
	{error, badarg} -> js_handler:bad_request(Req, 21);
	false -> js_handler:bad_request(Req, 21);
	true ->
	    FieldValues = jsx:decode(Body),
	    Prefix0 = proplists:get_value(<<"prefix">>, FieldValues),
	    DirectoryName0 = proplists:get_value(<<"directory_name">>, FieldValues),
	    case validate_directory_name(BucketId, Prefix0, DirectoryName0) of
		{PrefixedDirectoryName, Prefix1, DirectoryName1} ->
		    {PrefixedDirectoryName, Prefix1, DirectoryName1};
		{error, Number} -> js_handler:bad_request(Req, Number)
	    end
    end.

%%
%% Slugifies object name and performs indexation
%%
%% Example: "something" is uploaded as 736f6d657468696e67/.rriak_index.etf
%%
-spec create_pseudo_directory(any(), proplist()) -> any().

create_pseudo_directory(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    PrefixedDirectoryName = proplists:get_value(prefixed_directory_name, State),
    DirectoryName = unicode:characters_to_list(proplists:get_value(directory_name, State)),
    Prefix = proplists:get_value(prefix, State),
    riak_index:update(BucketId, PrefixedDirectoryName++"/"),
    riak_index:update(BucketId, Prefix),

    User = proplists:get_value(user, State),
    ActionLogRecord0 = #riak_action_log_record{
	action="mkdir",
	user_name=User#user.name,
	tenant_name=User#user.tenant_name,
	timestamp=io_lib:format("~p", [utils:timestamp()])
    },
    Summary0 = lists:flatten([["Created directory \""], DirectoryName ++ ["/\"."]]),
    ActionLogRecord1 = ActionLogRecord0#riak_action_log_record{details=Summary0},
    action_log:add_record(BucketId, Prefix, ActionLogRecord1),
    {true, Req0, []}.

%%
%% Creates pseudo directory
%%
handle_post(Req0, State0) ->
    BucketId = proplists:get_value(bucket_id, State0),
    case riak_api:head_bucket(BucketId) of
    	not_found -> riak_api:create_bucket(BucketId);
	_ -> ok
    end,
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    case validate_post(Req1, Body, BucketId) of
	{true, Req2, []} -> {true, Req2, []};  % error
	{PrefixedDirectoryName, Prefix, DirectoryName} ->
	    create_pseudo_directory(Req1, [
		{prefixed_directory_name, PrefixedDirectoryName},
		{prefix, Prefix},
		{directory_name, DirectoryName},
		{user, proplists:get_value(user, State0)},
		{bucket_id, BucketId}])
    end.
