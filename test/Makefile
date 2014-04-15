## Adapted from ../src/Makefile
##  gnumake
##
## Main usage: compile, run tests
##  (make tests)
## 
## NB: does not use smart_exceptions, etc.
##
## Structure:
## - Binaries written to ./ebin to not pollute
##   the "standard" ../ebin
## - Data (such as schema definitions) found
##   in ./data/
## - currently mixes tests and libraries

## UNFINISHED
## - should read the node name from somewhere, etc
NODE=test@$(shell hostname -s)

SCHEMATOOL_INC_DIR=../include
SCHEMATOOL_SRC_DIR=../src

ERLC=erlc
ERLC_OPTS= \
	+debug_info \
	+nowarn_unused_vars \
	+strict_record_tests \
	+warn_export_vars \
	-pa ../ebin \
	-I../ebin \
	-I$(SCHEMATOOL_INC_DIR) \
	-o ./ebin \
	+export_all

HRL_SRC= \

BEHAVIOUR_SRC= \

SCHEMATOOL_MODS= \
	alter_nodes_1 \
	alter_nodes_2 \
	alter_table_1 \
	simple_tests

SCHEMATOOL_SRC := $(patsubst %, %.erl, $(SCHEMATOOL_MODS))
SCHEMATOOL_TESTS := $(patsubst %, %.TOUCH, $(SCHEMATOOL_MODS))

# OBJS are all .beam files

SCHEMATOOL_OBJS := $(patsubst %.erl,../ebin/%.beam,$(SCHEMATOOL_SRC))

######################################################################

../ebin/%.beam:	%.erl
	$(ERLC) $(ERLC_OPTS) $<

%.TOUCH: ../ebin/%.beam
	(erl -noshell \
		-pa ../ebin \
		-eval "case eunit:test("$(*F)", [verbose]) of error -> init:stop(1); _ -> init:stop(0) end" \
	 && \
	 touch $@ \
	)

######################################################################

all: $(SCHEMATOOL_OBJS)

test: $(SCHEMATOOL_TESTS)

## Start an erlang with the proper paths and node.

test-dev:
	erl -pa ../src/ebin -pa ./ebin  -sname $(NODE)

## Cleanup rules

clean: clean_beam clean_test

clean_test:
	rm -f *.TOUCH

clean_beam:
	rm -f $(SCHEMATOOL_OBJS)

