CFLAGS = -Wno-write-strings -DSWAP_BYTES -g -fdiagnostics-show-option

CC = g++

CHOPPER_SOURCES = chopper.cpp PZdabFile.cxx 

all: chopper 

chopper: $(CHOPPER_SOURCES)
	$(CC) -o chopper $(CHOPPER_SOURCES) $(CFLAGS) 

clean:
	rm -f chopper
