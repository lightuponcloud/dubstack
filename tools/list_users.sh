#!/bin/bash

erlc -pa ../ebin list_users.erl && \
erl -pa ../ebin -eval 'list_users:main(), init:stop()' -noshell chpw.beam -username $username
