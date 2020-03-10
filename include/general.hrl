%% brand_name
%%	Name that appears in title of index.html
%%
%% domain
%%	FQDN you are going to use. Used to set cookies
%% session_cookie_name
%%      Authentication cookie name, that you can change
%%      in case it conflicts with another web application.
%%
%% csrf_cookie_name
%%	You can change that in case of conflicts with another app
%%
%% root_path
%%	Root path of this application on web server. Used in JavaScript.
%%
%% static_root
%%	URI where static files are available
%%
%% listen_port
%%	Port number for Cowboy to listen on
%%
-type general_settings() :: #{
    brand_name => string(),
    admin_email => string(),
    domain => string(),
    session_cookie_name => atom(),
    csrf_cookie_name => atom(),
    root_path => string(),
    static_root => string(),
    listen_port => integer(),
    locale => string()
}.

-record(general_settings, {
    brand_name="Demo"::string(),
    admin_email="sales@lightupon.cloud"::string(),
    domain="127.0.0.1"::string(),
    session_cookie_name=midsessionid::atom(),
    csrf_cookie_name=midcsrftoken::atom(),
    root_path="/riak/"::string(),
    static_root="/riak-media/"::string(),
    http_listen_port=8081,
    locale="uk"
}).

-define(DEFAULT_LANGUAGE_TAG, "en").
