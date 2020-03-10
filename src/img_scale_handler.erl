%%
%% Accepts HTTP requests and scales requested images.
%%
-module(img_scale_handler).
-behavior(cowboy_handler).

-export([init/2, content_types_provided/2, to_scale/2, allowed_methods/2,
    is_authorized/2, forbidden/2, resource_exists/2, previously_existed/2]).

-include("general.hrl").
-include("riak.hrl").
-include("entities.hrl").

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
    CropFlag = proplists:get_value(crop, State),
    case riak_api:head_object(BucketId, PrefixedObjectKey) of
	not_found -> {<<>>, Req0, []};
	RiakResponse0 ->
	    case proplists:get_value("x-amz-meta-bytes", RiakResponse0) of
		undefined -> {<<>>, Req0, []};
		TotalBytes ->
		    case utils:to_integer(TotalBytes) > ?MAXIMUM_IMAGE_SIZE_BYTES of
			true -> {<<>>, Req0, []};
			false ->
			    {OldBucketId, RealPath} = download_handler:real_path(BucketId, RiakResponse0),
			    scale_attempt(Req0, OldBucketId, RealPath, CropFlag, Width, Height)
		    end
	    end
    end.

scale_attempt(Req0, BucketId, RealPath, CropFlag, Width, Height) ->
    case riak_api:get_object(BucketId, RealPath) of
	not_found -> {<<>>, Req0, []};
	RiakResponse1 ->
	    Content = proplists:get_value(content, RiakResponse1),
	    Reply0 = img:scale([
		{from, Content},
		{to, jpeg},
		{crop, CropFlag},
		{scale_width, Width},
		{scale_height, Height}
	    ]),
	    case Reply0 of
		{error, Reason} -> js_handler:bad_request(Req0, Reason);
		Output0 -> {Output0, Req0, []}
	    end
    end.

%%
%% Checks if provided token is correct ( Token is optional here ).
%% ( called after 'allowed_methods()' )
%%
is_authorized(Req0, _State) ->
    %% Extracts token from request headers and looks it up in "security" bucket
    case utils:get_token(Req0) of
	undefined -> 
	    %% Check session id
	    Settings = #general_settings{},
	    SessionCookieName = Settings#general_settings.session_cookie_name,
	    #{SessionCookieName := SessionID0} = cowboy_req:match_cookies([{SessionCookieName, [], undefined}], Req0),
	    case login_handler:check_session_id(SessionID0) of
		false -> js_handler:unauthorized(Req0, 28);
		{error, Number} -> js_handler:unauthorized(Req0, Number);
		User -> {true, Req0, User}
	    end;
	Token ->
	    case login_handler:check_token(Token) of
		not_found -> js_handler:unauthorized(Req0, 28);
		expired -> js_handler:unauthorized(Req0, 28);
		User -> {true, Req0, User}
	    end
    end.


%%
%% Checks if provided token ( or access token ) is valid.
%% ( called after 'allowed_methods()' )
%%
forbidden(Req0, User) ->
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
    %% The following flag can be used to turn off image cropping
    CropFlag =
	case proplists:get_value(<<"crop">>, ParsedQs) of
	    <<"0">> -> false;
	    _ -> true
	end,
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
		false ->
		    PUser = admin_users_handler:user_to_proplist(User),
		    js_handler:forbidden(Req0, 37, proplists:get_value(groups, PUser));
		true ->
		    {false, Req0, [
			{bucket_id, BucketId},
			{prefix, Prefix},
			{width, Width},
			{height, Height},
			{is_dummy, IsDummyReq},
			{crop, CropFlag},
			{object_key, ObjectKey0}
		    ]}
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
