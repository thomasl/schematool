
## Note: smart_exceptions is a submodule

all: 
	(cd smart_exceptions/devel ; make all)
	(cd src; make all)
	(cd examples; make all)
	(cd test; make all)

clean:
	(cd smart_exceptions/devel ; make clean)
	(cd src; make clean)
	(cd examples; make clean)
	(cd test; make clean)

test:
	(cd smart_exceptions/devel ; make test)
	(cd test ; make test)
