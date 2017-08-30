-module(js_handler).

-export([init/2]).

-include("general.hrl").

-define(ERROR_CODES, [
    {001, "File size do not match headers."},
    {002, "No data received."},
    {003, "Object name is required parameter."},
    {004, "Invalid upload id."},
    {005, "Something's went wrong."},
    {006, "Backend unavailable."},
    {007, "Bucket not found"},
    {008, "Object name is required"},
    {009, "Incorrect object name."},
    {010, "Directory exists already"},
    {011, "Incorrect prefix name."}
]).

init(Req0, Opts) ->
    Token = binary_to_list(cowboy_req:binding(token, Req0)),
    BucketName = binary_to_list(cowboy_req:binding(bucket_name, Req0)),
    Settings = #general_settings{},
    {ok, Body} = jquery_riak_js_dtl:render([
	{error_codes, ?ERROR_CODES},
	{base_url, Settings#general_settings.base_url},
	{root_uri, Settings#general_settings.root_uri},
	{token, Token},
	{bucket_name, BucketName}
    ]),

    Req1 = cowboy_req:reply(200, #{
	<<"content-type">> => <<"text/html">>
    }, Body, Req0),

    {ok, Req1, Opts}.
