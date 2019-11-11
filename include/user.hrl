-export_type([tenant/0, group/0, user/0, token/0, csrf_token/0, app_token/0]).

%% Time in miliseconds
-define(CSRF_TOKEN_EXPIRATION_TIME, 31449600000). % 1000 * 60 * 60 * 24 * 7 * 52
-define(SESSION_EXPIRATION_TIME, 31449600000).

-type tenant() :: #{
    id          => string(),
    name        => string(),
    enabled     => boolean(),
    groups      => list()
}.

-type group() :: #{
    id          => string(),
    name        => string(),
    date_added  => string()
}.

-type user() :: #{
    id          => string(),
    name        => string(),
    tenant_id   => string(),
    tenant_name => string(),
    login       => string(),
    tel         => string(),
    enabled     => boolean(),
    staff       => boolean(),
    groups      => list()
}.

%%
%% Authentication token for API requests
%%
-type token() :: #{
    id          => string(),
    expires     => pos_integer() | infinity,
    user_id     => string()
}.

%%
%% CSRF token for login page
%%
-type csrf_token() :: #{
    id          => string(),
    expires     => pos_integer() | infinity
}.

%%
%% Application token allow applications to interact with this middleware
%%
-type app_token() :: #{
    id          => string(),
    name        => string(),
    enabled     => boolean()
}.

-record(tenant, {
    id          = ""::string(),
    name        = ""::string(),
    enabled     = true::boolean(),
    groups      = []::list()  %% Grous within that project
}).

-record(user, {
    id             = ""::string(),
    name           = ""::string(),
    tenant_id      = ""::string(),
    tenant_name    = ""::string(),
    tenant_enabled = true::boolean(),
    login          = ""::string(),
    tel            = ""::string(),
    salt           = ""::string(),
    password	   = ""::string(),
    hash_type      = ""::string(),
    enabled        = true::boolean(),
    staff          = false::boolean(),
    groups         = []::list()  %% Groups user belongs to
}).

-record(group, {
    id          = ""::string(),
    name        = ""::string()
}).

-record(app_token, {
    id          = ""::string(),
    name        = ""::string(),
    enabled     = true::boolean()
}).

-define(AUTH_NAME, pbkdf2_sha256).
