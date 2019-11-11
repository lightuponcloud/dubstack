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
    ObjectKey = proplists:get_value(object_key, State),
    PrefixedObjectKey = utils:prefixed_object_key(Prefix, ObjectKey),
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
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	not_found -> {<<>>, Req0, []};
	RiakResponse0 ->
	    ModifiedTime = proplists:get_value("x-amz-meta-modified-utc", RiakResponse0),
	    GUID = proplists:get_value("x-amz-meta-guid", RiakResponse0),
	    RealPrefix = utils:prefixed_object_key(?RIAK_REAL_OBJECT_PREFIX, GUID),
	    RealKey = utils:format_timestamp(utils:to_integer(ModifiedTime)),
	    RealPath = utils:prefixed_object_key(RealPrefix, RealKey),

	    case riak_api:get_object(BucketId, RealPath) of
		not_found -> {<<>>, Req0, []};
		RiakResponse1 ->
		    Content = proplists:get_value(content, RiakResponse1),
		    Reply0 = img:scale([
			{from, Content},
			{to, jpeg},
			{scale_width, Width},
			{scale_height, Height}
		    ]),
		    case Reply0 of
			{error, Reason} -> js_handler:bad_request(Req0, Reason);
			Output0 -> {Output0, Req0, []}
		    end
	    end
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
    BucketId =
	case cowboy_req:binding(bucket_id, Req0) of
	    undefined -> undefined;
	    BV -> erlang:binary_to_list(BV)
	end,
    ParsedQs = cowboy_req:parse_qs(Req0),
    Prefix = list_handler:validate_prefix(BucketId, proplists:get_value(<<"prefix">>, ParsedQs)),
    ObjectKey0 =
	case proplists:get_value(<<"object_key">>, ParsedQs) of
	    undefined -> {error, 8};
	    ObjectKey1 -> unicode:characters_to_list(ObjectKey1)
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
	    case lists:keyfind(error, 1, [Prefix, ObjectKey0]) of
		{error, Number} -> js_handler:forbidden(Req0, Number);
		false ->
		    {false, Req0, [
			{bucket_id, BucketId},
			{prefix, Prefix},
			{width, Width},
			{height, Height},
			{is_dummy, IsDummyReq},
			{object_key, ObjectKey0}
		    ]}
	    end;
	User ->
	    BucketIdOK =
		case utils:is_valid_bucket_id(BucketId, User#user.tenant_id) of
		    false -> {error, 27};
		    true -> ok
		end,
	    case lists:keyfind(error, 1, [Prefix, ObjectKey0, BucketIdOK]) of
		{error, Number} -> js_handler:forbidden(Req0, Number);
		false ->
		    UserBelongsToGroup = lists:any(fun(Group) ->
			utils:is_bucket_belongs_to_group(BucketId, User#user.tenant_id, Group#group.id) end,
			User#user.groups),
		    case UserBelongsToGroup of
			false -> js_handler:forbidden(Req0, 37);
			true ->
			    {false, Req0, [
				{bucket_id, BucketId},
				{prefix, Prefix},
				{width, Width},
				{height, Height},
				{is_dummy, IsDummyReq},
				{object_key, ObjectKey0}
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
