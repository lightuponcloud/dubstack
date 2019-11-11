%%
%% This file contains all the URI routing configuration.
%%
-module(middleware_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

-include("riak.hrl").
-include("general.hrl").

start(_Type, _Args) ->
    Dispatch = cowboy_router:compile([
	{'_', [
	    {"/riak/list/[:bucket_id]/", list_handler, []},
	    {"/riak/object/[:bucket_id]/", object_handler, []},
	    {"/riak/thumbnail/[:bucket_id]/", img_scale_handler, []},

	    {"/riak/upload/[:bucket_id]/", upload_handler, []},
	    {"/riak/upload/[:bucket_id]/[:upload_id]/[:part_num]/", upload_handler, []},

	    {"/riak/download/[...]", download_handler, []},

	    {"/riak/copy/[:src_bucket_id]/", copy_handler, []},
	    {"/riak/move/[:src_bucket_id]/", move_handler, []},
	    {"/riak/rename/[:bucket_id]/", rename_handler, []},

	    {"/riak/action-log/[:bucket_id]/", action_log, []},
	    {"/riak-search/", search_handler, []},

	    {"/riak/admin/users/", admin_users_handler, []},
	    {"/riak/admin/[:tenant_id]/users/", admin_users_handler, []},
	    {"/riak/admin/[:tenant_id]/users/[:user_id]/", admin_users_handler, []},
	    {"/riak/admin/tenants/", admin_tenants_handler, []},
	    {"/riak/admin/tenants/[:tenant_id]/", admin_tenants_handler, []},

	    {"/riak/login/", login_handler, []},
	    {"/riak/logout/", logout_handler, []},

	    {"/riak/js/", js_handler, []},
	    {"/riak/js/[:bucket_id]/", js_handler, []},
	    {"/riak/", first_page_handler, []},
	    {"/riak/[:bucket_id]/", first_page_handler, []}
	    %% The following line should be uncommented if you like Cowboy to serve static files
	    %% {"/riak-media/[...]", cowboy_static, {priv_dir, middleware, ""}}
	]}
    ]),
    Settings = #general_settings{},
    {ok, _} = cowboy:start_clear(middleware_http_listener,
	[{port, Settings#general_settings.http_listen_port}],
	#{env => #{
	    dispatch => Dispatch,
	    idle_timeout => 43200000 %% 12 hours
	}}),
    img:start_link(0),
    middleware_sup:start_link().

stop(_State) -> ok.
