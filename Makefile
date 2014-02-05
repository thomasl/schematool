
## Note: smart_exceptions is a submodule
##
## Note: is there some builtin way to invoke make in each of the listed subdirs?
##  (earlier used (cd subdir ; make all) and so on)

all: 
	make all -C smart_exceptions/devel
	make all -C src/
	make all -C test/

clean:
	make clean -C smart_exceptions/devel
	make clean -C src/
	make clean -C test/

test:
	make -C smart_exceptions/devel test/ MAKEFLAGS="test""
