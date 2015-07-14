// File Output header file
//
// K Labe, September 24 2014
// K Labe, July 14      2015 - Move hexdump function to here

#include "PZdabWriter.h"
#include "PZdabFile.h"

// This function writes out to the file zwrite the ZDAB record pointed to 
// by data in the file zfile.
void OutZdab(nZDAB* const data, PZdabWriter* const zwrite, PZdabFile* const zfile);

// This function prints ZDAB records to the screen in a human-readable format
// ptr is the place to begin read the record, len is the number of characters
// to read (4 x number of words to read);
void hexdump(char* const ptr, int const len);

// This function writes out a header record hdr of type j to file w.
void OutHeader(nZDAB* nzdab, PZdabWriter* const w);

// This function builds a new output file.  If it cannot open the file, it 
// aborts the program, so the pointer does not need to be checked.
PZdabWriter* Output(const char * const base, bool clobber, bool burst=0);
