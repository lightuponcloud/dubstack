-module(middleware_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

-include("riak.hrl").
-include("general.hrl").

start(_Type, _Args) ->
    Dispatch = cowboy_router:compile([
	{'_', [
	    {"/riak/list/[:token]/[:bucket_name]/", list_handler, []},
	    {"/riak/object/[:token]/[:bucket_name]/", object_handler, []},

	    {"/riak/upload/[:token]/[:bucket_name]/", upload_handler, []},
	    {"/riak/upload/[:token]/[:bucket_name]/[:upload_id]/[:part_num]/", upload_handler, []},

	    {"/riak/delete/[:token]/[:bucket_name]/", delete_handler, []},
	    {"/riak/create-pseudo-directory/[:token]/[:bucket_name]/", mkdir_handler, []},
	    {"/riak/copy/[:token]/[:src_bucket_name]", copy_handler, []},
	    {"/riak/move/[:token]/[:src_bucket_name]", move_handler, []},

	    {"/riak-search/[:token]/", search_handler, []},
	    {"/token/[:token]/[:bucket_name]/riak.js", js_handler, []},
	    {"/riak/[:token]/[:bucket_name]/", first_page_handler, []}
	]}
    ]),
    Settings = #general_settings{},
    {ok, _} = cowboy:start_clear(middleware_http_listener, 100,
        [{port, Settings#general_settings.listen_port}],
        #{env => #{dispatch => Dispatch}}
    ),
    middleware_sup:start_link().

stop(_State) ->
    ok.
