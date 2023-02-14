#!/bin/bash

while [ $# -gt 0 ]; do
  case "$1" in
    --bucket=*)
      bucket="${1#*=}"
      ;;
    --userid=*)
      userid="${1#*=}"
      ;;
    *)
      printf "***************************\n"
      printf "* Usage: $0 --bucket=the-example-bucket.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done

echo "USER ID: "$userid
erlc -pa ../ebin -pa ../deps/sqlite3/ebin -pa ../deps/ux/ebin -pa ../deps/jsx/ebin populate.erl && \
erl -pa ../ebin -pa ../deps/sqlite3/ebin -pa ../deps/ux/ebin -pa ../deps/jsx/ebin -eval 'populate:main(), init:stop()' -noshell populate.beam -bucket $bucket -userid $userid
