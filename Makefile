PROJECT = middleware
PROJECT_DESCRIPTION = Rest interface for Riak CS
PROJECT_VERSION = 0.1.0

DEPS = cowboy erlydtl jsx ux lager sqlite3
LOCAL_DEPS = inets xmerl
DEP_PLUGINS = cowboy
TEST_DEPS = meck

dep_cowboy_commit = 2.7.0
dep_meck = git https://github.com/eproxus/meck.git master
dep_ux = git https://github.com/erlang-unicode/ux.git master
dep_jsx = git https://github.com/talentdeficit/jsx v2.11.0
dep_sqlite3 = git https://github.com/processone/erlang-sqlite3 master

APP_VERSION = $(shell cat rel/version)

middleware: all
	echo APP_VERSION = $(APP_VERSION)

messages:
	erl -pa ./ebin -pa ./deps/gettext/ebin -noshell -run makemessages generate_pos uk

include erlang.mk
