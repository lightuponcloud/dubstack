PROJECT = middleware
PROJECT_DESCRIPTION = Rest interface for Riak CS
PROJECT_VERSION = 0.1.0

DEPS = cowboy erlydtl jsx
LOCAL_DEPS = inets xmerl
dep_cowboy_commit = master
DEP_PLUGINS = cowboy

include erlang.mk
