CFLAGS = -Wno-write-strings -DSWAP_BYTES -g -fdiagnostics-show-option

CC = g++

CHOPPER_SOURCES = chopper.cpp PZdabFile.cxx 

# PThread.cxx PThreadInt.cxx PZdabWriter.cxx CUtils.cxx error_ph.c

all: chopper 

chopper: $(CHOPPER_SOURCES)
	$(CC) -o chopper $(CHOPPER_SOURCES) $(CFLAGS) 

clean:
	rm -f chopper
