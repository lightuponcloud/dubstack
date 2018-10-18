%%
%% Accepts HTTP requests and scales requested images.
%%
-module(img_scale_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_scale/2, allowed_methods/2,
    is_authorized/2, forbidden/2, resource_exists/2, previously_existed/2]).

-include("riak.hrl").
-include("user.hrl").

init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

content_types_provided(Req, State) ->
    {[
	{{<<"image">>, <<"jpeg">>, '*'}, to_scale}
    ], Req, State}.

%%
%% Called first
%%
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

to_scale(Req0, State) ->
    BucketId = proplists:get_value(bucket_id, State),
    Prefix = proplists:get_value(prefix, State),
    ObjectName = proplists:get_value(object_name, State),
    PrefixedObjectName = utils:prefixed_object_name(Prefix, ObjectName),
    Width =
	case proplists:get_value(width, State) of
	    undefined -> ?DEFAULT_IMAGE_WIDTH;
	    W ->
		case W =< 0 of
		    true -> ?DEFAULT_IMAGE_WIDTH;
		    false -> W
		end
	end,
    Height =
	case proplists:get_value(height, State) of
	    undefined -> Width;
	    H ->
		case H =< 0 of
		    true -> ?DEFAULT_IMAGE_WIDTH;
		    false -> H
		end
	end,
    case riak_api:get_object(BucketId, PrefixedObjectName) of
	not_found -> {<<>>, Req0, []};
	RiakResponse ->
	    Content = proplists:get_value(content, RiakResponse),
	    Reply0 = img:lookup([
		{from, Content},
		{to, jpeg},
		{scale_width, Width},
		{scale_height, Height}
	    ]),
	    case Reply0 of
		{ok, Output0} -> {Output0, Req0, []};
		_ ->
		    case proplists:get_value(is_dummy_required, State) of
			true ->
			    %% return dummy image instead
			    EbinDir = filename:dirname(code:which(img_scale_handler)),
			    AppDir = filename:dirname(EbinDir),
			    Path = filename:join([AppDir, "priv", "img_unavailable.png"]),
			    {ok, Output1} = file:read_file(Path),
			    {ok, Output2} = img:lookup([
				{from, Output1},
				{to, jpeg},
				{scale_width, Width},
				{scale_height, Height}
			    ]),
			    {Output2, Req0, []};
			false -> {<<>>, Req0, []}
		    end
	    end
    end.

%%
%% Looks up Access Token in riak index object for object with specified key.
%% Returns true if token is found. Otherwise returns false.
%%
-spec check_access_token(BucketId, Prefix, ObjectKey, AccessToken) -> boolean() when
    BucketId :: string(),
    Prefix :: string()|atom(),
    ObjectKey :: string(),
    AccessToken :: string()|atom().

check_access_token(_, _, _, undefined) -> false;
check_access_token(BucketId, Prefix, ObjectKey, AccessToken)
	when erlang:is_list(BucketId), erlang:is_list(Prefix) orelse Prefix =:= undefined,
	     erlang:is_list(ObjectKey), erlang:is_list(AccessToken) ->
    PrefixedIndexFilename = utils:prefixed_object_name(Prefix, ?RIAK_INDEX_FILENAME),
    List0 =
	case riak_api:get_object(BucketId, PrefixedIndexFilename) of
	    not_found -> [{access_tokens, []}];
	    C -> erlang:binary_to_term(proplists:get_value(content, C))
	end,
    AccessTokens = proplists:get_value(access_tokens, List0),
    case proplists:get_value(ObjectKey, AccessTokens) of
	undefined -> false;
	V -> V =:= AccessToken
    end.

%%
%% Checks if provided token is correct ( Token is optional here ).
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, _State) ->
    case utils:check_token(Req0) of
	undefined -> {true, Req0, []};
	not_found -> {true, Req0, []};
	expired -> {true, Req0, []};
	User -> {true, Req0, [{user, User}]}
    end.

%%
%% Checks if provided token ( or access token ) is valid.
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, State) ->
    BucketId = erlang:binary_to_list(cowboy_req:binding(bucket_id, Req0)),
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix = list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, ParsedQs)),
    ObjectName0 =
	case proplists:get_value(<<"object_name">>, ParsedQs) of
	    undefined -> {error, 8};
	    ObjectName1 -> unicode:characters_to_list(ObjectName1)
	end,
    Width =
	case proplists:get_value(<<"w">>, ParsedQs) of
	    undefined -> undefined;
	    W -> try utils:to_number(W) catch error:badarg -> undefined end
	end,
    Height =
	case proplists:get_value(<<"h">>, ParsedQs) of
	    undefined -> undefined;
	    H -> try utils:to_number(H) catch error:badarg -> undefined end
	end,
    IsDummyReq =
	case proplists:get_value(<<"dummy">>, ParsedQs) of
	    undefined -> false;
	    <<"1">> -> true
	end,
    case proplists:get_value(user, State) of
	undefined ->
	    case lists:keyfind(error, 1, [Prefix, ObjectName0]) of
		{error, Number} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req0),
		    {true, Req1, []};
		false ->
		    %% Check access token if bearer token is not specified
		    AccessToken =
			case proplists:get_value(<<"access_token">>, ParsedQs) of
			    undefined -> undefined;
			    N2 -> utils:to_list(N2)
			end,
		    case check_access_token(BucketId, Prefix, ObjectName0, AccessToken) of
			false ->
			    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, 28}]), Req0),
			    {true, Req1, []};
			true ->
			    {false, Req0, [
				{bucket_id, BucketId},
				{prefix, Prefix},
				{width, Width},
				{height, Height},
				{is_dummy, IsDummyReq},
				{object_name, ObjectName0}
			    ]}
		    end
	    end;
	User ->
	    BucketIdOK =
		case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
		    false -> {error, 27};
		    true -> ok
		end,
	    case lists:keyfind(error, 1, [Prefix, ObjectName0, BucketIdOK]) of
		{error, Number} ->
		    Req1 = cowboy_req:set_resp_body(jsx:encode([{error, Number}]), Req0),
		    {true, Req1, []};
		false ->
		    UserBelongsToGroup = lists:any(fun(Group) ->
			utils:is_bucket_belongs_to_group(BucketId, User#user.tenant_id, Group#group.id) end,
			User#user.groups),
		    case UserBelongsToGroup of
			false -> {true, Req0, []};
			true ->
			    {false, Req0, [
				{bucket_id, BucketId},
				{prefix, Prefix},
				{width, Width},
				{height, Height},
				{is_dummy, IsDummyReq},
				{object_name, ObjectName0}
			    ]}
		    end
	    end
    end.

%%
%% Validates request parameters
%% ( called after 'content_types_provided()' )
%%
resource_exists(Req0, State) ->
    {true, Req0, State}.

previously_existed(Req0, _State) ->
    {false, Req0, []}.
