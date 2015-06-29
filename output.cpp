#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include "curl.h"

// This function writes out the ZDAB record
void OutZdab(nZDAB * const data, PZdabWriter * const zwrite,
                    PZdabFile * const zfile){
  if(!data) return;
  const int index = PZdabWriter::GetIndex(data->bank_name);
  if(index < 0){
     fprintf(stderr, "Unrecognized bank name\n");
     alarm(40, "Outzdab: unrecognized bank name.", 5);
  }
  else{
    uint32_t * const bank = zfile->GetBank(data);
    zwrite->WriteBank(bank, index);
  }
}


// This function writes out a header record from the buffer to a file
void OutHeader(nZDAB* nzdab, PZdabWriter* const w){
  if (!nzdab) return;
  if( nzdab->bank_name != 0){
    // Must put nzdab in external format for use with GetIndex
    SWAP_INT32(&nzdab->bank_name, 1);
    int index = PZdabWriter::GetIndex(nzdab->bank_name);
    SWAP_INT32(&nzdab->bank_name, 1);
    if(index < 0){
      fprintf(stderr, "Unknown bank name %x\n", nzdab->bank_name);
      alarm(40, "Outheader: You never see this!", 6);
      exit(1);
    }
    if(w->WriteBank(PZdabFile::GetBank(nzdab), index)){
      fprintf(stderr,"Error writing to zdab file\n");
      alarm(40, "Outheader: error writing to zdab file.", 7);
    }
  }
}


// This function builds a new output file.  If it can't open 
// the file, it aborts the program, so the return pointer does not
// need to be checked.
// The optional argument burst states whether to write to the burst
// directory instead of the data directory.  Defaults to false.
PZdabWriter * Output(const char * const base, bool clobber, bool burst){
  const int maxlen = 1024;
  char outfilename[maxlen];

  if(!burst){
    if(snprintf(outfilename, maxlen, "/home/trigger/zdab/%s.zdab", base) >= maxlen){
      outfilename[maxlen-1] = 0; // or does snprintf do this already?
      fprintf(stderr, "WARNING: Output filename truncated to %s\n",
              outfilename);
      alarm(40, "Output: output filename truncated", 8);
    }
  }
  else{
    if(snprintf(outfilename, maxlen, "/raid/data/burst/%s.zdab", base) >= maxlen){
      outfilename[maxlen-1] = 0;
      fprintf(stderr, "WARNING: Output filename truncated to %s\n",
              outfilename);
      alarm(40, "OutputL output filename truncated", 8);
    }
  }

  if(!access(outfilename, W_OK)){
    if(!clobber){
      fprintf(stderr, "%s already exists and you told me not to "
              "overwrite it!\n", outfilename);
      alarm(40, "Output: Should not overwrite that file.", 9);
      exit(1);
    }
    unlink(outfilename);
  }
  else if(!access(outfilename, F_OK)){
    fprintf(stderr, "%s already exists and we can't overwrite it!\n",
            outfilename);
    alarm(40, "Output: Cannot overwrite that file.", 10);
    exit(1);
  }

  PZdabWriter * const ret = new PZdabWriter(outfilename, 1);

  if(!ret || !ret->IsOpen()){
    fprintf(stderr, "Could not open output file %s\n", outfilename);
    alarm(40, "Output: Cannot open file.", 11);
    exit(1);
  }
  return ret;
}

