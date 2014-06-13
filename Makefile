CFLAGS = -Wall -Wextra -Wno-write-strings -DSWAP_BYTES \
         -fdiagnostics-show-option -L/cp/home/cp/klabe/hiredis \
         -I/cp/home/cp/klabe/hiredis -lhiredis 

CC = g++

CHOPPER_SOURCES = chopper.cpp PZdabFile.cxx PZdabWriter.cxx \
                  MD5Checksum.cxx 

all: chopper 

chopper: $(CHOPPER_SOURCES)
	$(CC) -o chopper $(CHOPPER_SOURCES) $(CFLAGS) 

clean:
	rm -f chopper
