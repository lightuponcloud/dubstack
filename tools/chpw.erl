%%
%% Changes user's password.
%%
-module(chpw).

-export([main/0]).

-include_lib("xmerl/include/xmerl.hrl").
-include("../include/entities.hrl").
-include("../include/riak.hrl").


save_user(User) ->
    UserId = User#user.id,
    Timestamp = io_lib:format("~p", [utils:timestamp()]),
    Groups = [{group, [{id, [G#group.id]},
		       {name, [G#group.name]}]} || G <- User#user.groups],
    EditedUser =
	{record, [
	    {id, [User#user.id]},
	    {name, [User#user.name]},
	    {tenant_id, [User#user.tenant_id]},
	    {tenant_name, [User#user.tenant_name]},
	    {hash_type, [utils:to_list(User#user.hash_type)]},
	    {password, [utils:to_list(User#user.password)]},
	    {salt, [utils:to_list(User#user.salt)]},
	    {login, [User#user.login]},
	    {tel, [User#user.tel]},
	    {enabled, [utils:to_list(User#user.enabled)]},
	    {staff, [utils:to_list(User#user.staff)]},
	    {date_updated, Timestamp},
	    {groups, Groups}
	]},
    RootElement0 = #xmlElement{name=user, content=[EditedUser]},
    XMLDocument0 = xmerl:export_simple([RootElement0], xmerl_xml),
    Response = riak_api:put_object(?SECURITY_BUCKET_NAME, ?USER_PREFIX, UserId,
	unicode:characters_to_binary(XMLDocument0), [{acl, private}]),
    case Response of
	{error, Reason} ->
	    io:fwrite("Can't put object ~p/~p/~p: ~p", [?SECURITY_BUCKET_NAME, ?USER_PREFIX, UserId, Reason]),
	    error;
	_ -> ok
    end.

main() ->
    inets:start(),
    {ok, LoginArg} = init:get_argument(login),
    {ok, PasswordArg} = init:get_argument(password),

    Login = lists:flatten(LoginArg),
    case Login of
	undefined -> throw("Login is expected");
	[] -> throw("Login is expected");
	_ -> ok
    end,

    Password0 = lists:flatten(PasswordArg),
    case Password0 of
	undefined -> throw("Password is expected");
	[] -> throw("Password is expected");
	_ -> ok
    end,

    Users = admin_users_handler:get_full_users_list(),
    HexLogin = utils:hex(erlang:list_to_binary(Login)),
    Users = [U || U <- Users, U =/= not_found],  %% tenant might be deleted
    case [U || U <- Users, U#user.login =:= HexLogin] of
	[] -> io:fwrite("Login ~p not found.", [Login]);
	[User0] ->
	    {ok, Password1, Salt} = riak_crypto:hash_password(erlang:list_to_binary(Password0)),
	    User1 = User0#user{
		salt = Salt,
		password = Password1,
		hash_type = ?AUTH_NAME
	    },
	    save_user(User1);
	[_|_] -> io:fwrite("More than one user found.")
    end.
