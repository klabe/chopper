CFLAGS = -Wno-write-strings -DSWAP_BYTES -g -fdiagnostics-show-option
INCLUDE = -Isrc -Iutil -Iutil/include -I$(ROOTSYS)/include -I$(RATROOT)/include -I$(RATROOT)/include/RAT
LFLAGS = -Llib -L$(ROOTSYS)/lib -L$(RATROOT)/lib

CC = g++
LIBS = $(shell root-config --libs)
RAT_LIBS = -lRATEvent_$(shell uname)

CHOPPER_SOURCES = chopper.cpp
ZCONVERT_SOURCES = src/zdab_convert.cpp util/PZdabFile.cxx
ZFILE_SOURCES = src/zdab_file.cpp util/PZdabFile.cxx
ZDISPATCH_SOURCES = src/zdab_dispatch.cpp
Z2R_SOURCES = src/zdab2root.cpp

RAT_INC = $(RATROOT)/include/RAT
DICT_SOURCES = src/zdab_dispatch.hpp src/zdab_file.hpp src/zdab_convert.hpp $(RAT_INC)/DS/Digitiser.hh $(RAT_INC)/DS/PMTUnCal.hh $(RAT_INC)/Extensible.hh util/include/Record_Info.h src/linkdef.h


all: chopper 

lib:
	test -d lib || mkdir lib

rootlib:
	test -d build || mkdir build
	$(ROOTSYS)/bin/rootcint -f ./build/ratzdab_dict.cxx -c -p -Isrc $(INCLUDE) $(DICT_SOURCES)
	$(CC) -o lib/ratzdab_root.so -shared -fPIC $(CFLAGS) build/ratzdab_dict.cxx src/zdab_convert.cpp src/zdab_file.cpp src/zdab_dispatch.cpp util/PZdabFile.cxx -I. -Icontrib/disp/include $(INCLUDE) -Lcontrib/disp/lib $(LFLAGS) $(RAT_LIBS) $(LIBS) -lconthost

chopper:
	$(CC) -o chopper $(CHOPPER_SOURCES) $(INCLUDE) $(CFLAGS) src/zdab_file.cpp $(CXXFLAGS) $(LFLAGS) $(RAT_LIBS) $(LIBS) -Iconstrib/disp/include -Lcontrib/disp/lib


clean:
	if test -d contrib/disp; then cd contrib/disp/src && make clean; fi
	cd examples && make clean
	-$(RM) *.o lib/*.so lib/*.a bin/* python/*.pyc
	-$(RM) -rf build *.dSYM lib/*.dSYM

