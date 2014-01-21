## Schematool makefile [for examples]
##
## - this is basically a copy of src/Makefile
##   except with other modules

SCHEMATOOL_INC_DIR=../include
SCHEMATOOL_SRC_DIR=../examples

## UNFINISHED
## - weird paths, should be fixed

BASEDIR=$(HOME)/git
UTIL_DIR=$(BASEDIR)/smart_exceptions/devel/ebin

# NB: We currently export everything, to simplify debugging

ERLC=erlc
ERLC_OPTS= \
	+debug_info \
	+nowarn_unused_vars \
	+strict_record_tests \
	+warn_export_vars \
	-pa ../ebin -pa $(UTIL_DIR) \
	-I../ebin \
	-I$(UTIL_DIR) \
	-I$(SCHEMATOOL_INC_DIR) \
	-o ../ebin \
	+'{parse_transform, smart_exceptions}' \
	+export_all

# rules to make every beam file
# $< is the name of the .erl file

../ebin/%.beam:	%.erl
	$(ERLC) $(ERLC_OPTS) $<

HRL_SRC= \

BEHAVIOUR_SRC= \

SCHEMATOOL_MODS= \

SCHEMATOOL_SRC := $(patsubst %, %.erl, $(SCHEMATOOL_MODS))

# OBJS are all .beam files

SCHEMATOOL_OBJS := $(patsubst %.erl,../ebin/%.beam,$(SCHEMATOOL_SRC))
BEHAV_OBJS := $(patsubst %.erl,../ebin/%.beam,$(BEHAVIOUR_SRC))

all:	behaviours objs

behaviours: $(BEHAV_OBJS)

objs:	$(SCHEMATOOL_OBJS)

clean: clean_bin 

clean_bin: clean_beam clean_app

clean_beam:
	rm -f ../ebin/*.beam

clean_app:
	rm -f ../ebin/*.app

