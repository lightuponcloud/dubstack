PROJECT = middleware
PROJECT_DESCRIPTION = Rest interface for Riak CS
PROJECT_VERSION = 0.1.0

DEPS = cowboy erlydtl jsx ux
LOCAL_DEPS = inets xmerl
DEP_PLUGINS = cowboy
TEST_DEPS = meck

dep_cowboy_commit = 2.0.0-rc.4
dep_meck = git https://github.com/eproxus/meck.git master

dep_ux = git https://github.com/erlang-unicode/ux.git master

APP_VERSION = $(shell cat rel/version)

middleware: all
	echo APP_VERSION = $(APP_VERSION)

include erlang.mk
