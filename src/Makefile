## Schematool makefile
##
## - this has been adapted from a generic makefile,
##   so it's a bit more elaborate than needed
##
## We assume gnumake here
## 
## See ERLC_OPTS for some choices:
## - debug_info
## - export_all
## - uses smart_exceptions parse transform
##
## UNFINISHED
## (The smart exception usage is pretty clunky. As
## written, you'll have to install it in the right
## place.)

SCHEMATOOL_INC_DIR=../include
SCHEMATOOL_SRC_DIR=../src

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

# currently not used, import bin/ scripts
LISTIFY=perl ../bin/listify.pl
BIGSUBST=perl ../bin/bigsubst.pl

# rules to make every beam file
# $< is the name of the .erl file

../ebin/%.beam:	%.erl
	$(ERLC) $(ERLC_OPTS) $<

HRL_SRC= \

BEHAVIOUR_SRC= \

SCHEMATOOL_MODS= \
	schematool \


SCHEMATOOL_SRC := $(patsubst %, %.erl, $(SCHEMATOOL_MODS))
OM_SRC=$(SCHEMATOOL_SRC)
ERL_SRC= $(BEHAVIOUR_SRC) $(OM_SRC)

# OBJS are all .beam files

SCHEMATOOL_OBJS := $(patsubst %.erl,../ebin/%.beam,$(SCHEMATOOL_SRC))
BEHAV_OBJS := $(patsubst %.erl,../ebin/%.beam,$(BEHAVIOUR_SRC))

all:	behaviours objs

behaviours: $(BEHAV_OBJS)

objs:	$(SCHEMATOOL_OBJS)

## currently not used
../ebin/schematool.app:
	$(LISTIFY) $(SCHEMATOOL_MODS) > ../ebin/schematool.tmp
	$(BIGSUBST) \
	  -x %SCHEMATOOL_MODS% ../ebin/schematool.tmp \
	  -e %SCHEMATOOL_VSN% $(shell cat ../vsn.mk) \
	  -e %SCHEMATOOL_DEPS% $(shell $(LISTIFY) $(SCHEMATOOL_DEPS)) \
	  -e %SCHEMATOOL_PROCS% $(shell $(LISTIFY) $(SCHEMATOOL_PROCS)) \
	  -i schematool.app.src \
	  > ../ebin/schematool.app
	rm -f ../ebin/schematool-mods.tmp

clean: clean_bin 

clean_bin: clean_beam clean_app

clean_beam:
	rm -f ../ebin/*.beam

clean_app:
	rm -f ../ebin/*.app

