#!/bin/bash

erlc tst.erl && erl -eval 'tst:tst(), init:stop()' -noshell tst.beam
