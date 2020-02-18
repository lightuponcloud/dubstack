PROJECT = middleware
PROJECT_DESCRIPTION = Rest interface for Riak CS
PROJECT_VERSION = 0.1.0

DEPS = cowboy erlydtl jsx ux lager
LOCAL_DEPS = inets xmerl pbkdf2
DEP_PLUGINS = cowboy
TEST_DEPS = meck

dep_cowboy_commit = 2.7.0
dep_meck = git https://github.com/eproxus/meck.git master
dep_ux = git https://github.com/erlang-unicode/ux.git master

APP_VERSION = $(shell cat rel/version)

middleware: all
	echo APP_VERSION = $(APP_VERSION)

messages:
	erl -pa ./ebin -pa ./deps/gettext/ebin -noshell -run makemessages generate_pos uk

include erlang.mk
