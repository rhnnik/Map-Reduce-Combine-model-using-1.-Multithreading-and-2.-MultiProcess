all: combiner

combiner:
	gcc -pthread -o combiner combiner.c

