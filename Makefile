CFLAGS = -Wall -Wextra -Wno-write-strings -DSWAP_BYTES \
         -fdiagnostics-show-option $(curl-config --cflags) 

LINKFLAGS = -L/cp/home/cp/klabe/hiredis -L/cp/home/cp/klabe/chopper/SFMT-src-1.4.1 -lhiredis -lcurl -lsfmt

all: chopper 

chopper: chopper.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o curl.o redis.o output.o
	g++ $(CFLAGS) -o chopper chopper.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o curl.o redis.o output.o $(LINKFLAGS)

chopper.o: chopper.cpp snbuf.h curl.h redis.h struct.h output.h
	g++ -c chopper.cpp $(CFLAGS) -I/cp/home/cp/klabe/hiredis -I/home/cp/klabe/chopper/SFMT-src-1.4.1 -DSFMT_MEXP=19937


PZdabFile.o: PZdabFile.cxx
	g++ -c PZdabFile.cxx $(CFLAGS) 


PZdabWriter.o: PZdabWriter.cxx
	g++ -c PZdabWriter.cxx $(CFLAGS) 


MD5Checksum.o: MD5Checksum.cxx
	g++ -c MD5Checksum.cxx $(CFLAGS) 


snbuf.o: snbuf.cpp 
	g++ -c snbuf.cpp $(CFLAGS) 

curl.o: curl.cpp
	g++ -c curl.cpp $(CFLAGS)

redis.o: redis.cpp
	g++ -c redis.cpp -I/cp/home/cp/klabe/hiredis $(CFLAGS) $(LINKFLAGS)

output.o: output.cpp
	g++ -c output.cpp $(CFLAGS)


clean:
	rm -f chopper chopper.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o curl.o redis.o
