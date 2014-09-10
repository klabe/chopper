Chopper (Stonehenge) Readme file.

Because the chopper links to a few external libraries (specifically, hiredis
in order to communicate to the monitoring database, and sfmt for geenrating
random numbers), it is useful to run it in a wrapper script to avoid needing
to set the LD_LIBRARY_PATH yourself.  This is available at chopper.sh.

Instructions for compiling sfmt library.
In the SFMT directory, run the following commands:

gcc -c -Wall -Werror -fpic -DSFMT_MEXP=19937 SFMT.c 
gcc -shared -o libsfmt.so SFMT.o
