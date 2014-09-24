Chopper (Stonehenge) Readme file.

Installation instructions
-------------------------
After cloning the chopper git repository, you must compile the sfmt library.
In the SFMT directory, run the following commands:

gcc -c -Wall -Werror -fpic -DSFMT_MEXP=19937 SFMT.c
gcc -shared -o libsfmt.so SFMT.o

In addition, the hiredis and libcurl libraries are required.  These must be
installed independently.  Furthermore, the Makefile must be updated to provide
the appropriate library and inclusion paths.  Thereafter, a simple "make"
should suffice to build the software.

Because of the external library dependencies (specifically, hiredis
in order to communicate to the monitoring database, and sfmt for geenrating
random numbers), it is useful to run Stonehenge  in a wrapper script to avoid 
needing to set the LD_LIBRARY_PATH yourself.  This is available at chopper.sh.


A Note on the Format of Configuration Files
-------------------------------------------
The configuration file is to consist of a text file, with one line dedicated
to each of the 11 parameters that must be set.  Each line is to begin with the
name of the parameter, with the value of the parameter following after a
space.

All parameters must be included, and all parameters must be given values.  All
values should be integers, with the exception of the bitmask parameter, which
should be given in hex.  The bitmask parameter must be given last.  The other
parameters may be given in any order.

If no configuration file is specified, the program will exit with a message
asking for one.  If an incomplete configuration file is provided, the program
will likewise exit with an error message.
