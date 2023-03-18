#!/bin/bash

while [ $# -gt 0 ]; do
  case "$1" in
    --login=*)
      login="${1#*=}"
      ;;
    --password=*)
      password="${1#*=}"
      ;;
    *)
      printf "**************************************************\n"
      printf "* set +o history  # temporarily turn off history *\n"
      printf "* Usage: $0 --login=someone --password=blah\n"
      printf "* set -o history  # turn it back on              *\n"
      printf "**************************************************\n"
      exit 1
  esac
  shift
done

[ -z "$login" ] || [ -z "$password" ] && {
      printf "**************************************************\n"
      printf "* set +o history  # temporarily turn off history *\n"
      printf "* Usage: $0 --login=someone --password=blah\n"
      printf "* set -o history  # turn it back on              *\n"
      printf "**************************************************\n"
      exit 1
}


erlc -pa ../ebin chpw.erl && \
erl -pa ../ebin -eval 'chpw:main(), init:stop()' -noshell chpw.beam -login $login -password $password
