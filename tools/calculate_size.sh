#!/bin/bash

while [ $# -gt 0 ]; do
  case "$1" in
    --bucket=*)
      bucket="${1#*=}"
      ;;
    *)
      printf "***************************\n"
      printf "* Usage: $0 --bucket=the-example-bucket.*\n"
      printf "***************************\n"
      exit 1
  esac
  shift
done

erlc -pa ../ebin -pa ../deps/sqlite3/ebin -pa ../deps/ux/ebin -pa ../deps/jsx/ebin calculate_size.erl && \
erl -pa ../ebin -pa ../deps/sqlite3/ebin -pa ../deps/ux/ebin -pa ../deps/jsx/ebin -eval 'calculate_size:main(), init:stop()' -noshell populate.beam -bucket $bucket
