CFLAGS = -Wall -Wextra -Wno-write-strings -DSWAP_BYTES \
         -fdiagnostics-show-option $(curl-config --cflags) 

LINKFLAGS = -L/usr/include/hiredis -lhiredis -lcurl

all: stonehenge 

stonehenge: stonehenge.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o curl.o redis.o output.o config.o
	g++ $(CFLAGS) -o stonehenge stonehenge.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o curl.o redis.o output.o config.o $(LINKFLAGS)

stonehenge.o: stonehenge.cpp snbuf.h curl.h redis.h struct.h output.h config.h
	g++ -c stonehenge.cpp $(CFLAGS) -I/usr/include/hiredis


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

redis.o: redis.cpp struct.h
	g++ -c redis.cpp $(CFLAGS) -I/usr/include/hiredis

output.o: output.cpp
	g++ -c output.cpp $(CFLAGS)

config.o: config.cpp
	g++ -c config.cpp $(CFLAGS)


clean:
	rm -f stonehenge stonehenge.o PZdabFile.o PZdabWriter.o MD5Checksum.o snbuf.o curl.o redis.o
