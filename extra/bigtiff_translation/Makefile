CC = gcc
CFLAGS = -O -std=c99
OBJ = main.o checks.o

.PHONY: all clean cleaner

all : bt

bt : $(OBJ)
	$(CC) $(LDFLAGS) $(OBJ) -o bt

%.o : %.c *.h
	$(CC) $(CFLAGS) -c $*.c -o $@

clean :
	rm -f $(OBJ)

cleaner : clean
	rm -f bt
