$(HOME)/bin/lmbk : lmbk
	ln -f lmbk $(HOME)/bin/lmbk

lmbk : lmbk.scm ../sclibs/args.scm
	csc lmbk.scm

