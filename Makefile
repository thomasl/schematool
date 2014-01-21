
all: 
	(cd src; make all)
	(cd examples; make all)
	(cd test; make all)

clean:
	(cd src; make clean)
	(cd examples; make clean)
	(cd test; make clean)

