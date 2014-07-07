CFLAGS = -Wall -Wextra -Wno-write-strings -DSWAP_BYTES \
         -fdiagnostics-show-option $(curl-config --cflags) 

LINKFLAGS = -L/cp/home/cp/klabe/hiredis -lhiredis -lcurl 

all: chopper 

chopper: chopper.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o
	g++ -o chopper chopper.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o $(LINKFLAGS)

chopper.o: chopper.cpp snbuf.h
	g++ -c chopper.cpp $(CFLAGS) -I/cp/home/cp/klabe/hiredis


PZdabFile.o: PZdabFile.cxx
	g++ -c PZdabFile.cxx $(CFLAGS) 


PZdabWriter.o: PZdabWriter.cxx
	g++ -c PZdabWriter.cxx $(CFLAGS) 


MD5Checksum.o: MD5Checksum.cxx
	g++ -c MD5Checksum.cxx $(CFLAGS) 


snbuf.o: snbuf.cpp
	g++ -c snbuf.cpp $(CFLAGS) 


clean:
	rm -f chopper
