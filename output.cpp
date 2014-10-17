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
     alarm(40, "Outzdab: unrecognized bank name.");
  }
  else{
    uint32_t * const bank = zfile->GetBank(data);
    zwrite->WriteBank(bank, index);
  }
}


// This function writes out the header buffer to a file
void OutHeader(const GenericRecordHeader * const hdr,
                      PZdabWriter* const w, const int j){
  if (!hdr) return;

  int index = PZdabWriter::GetIndex(hdr->RecordID);
  if(index < 0){
    // PZdab for some reason got zero for the header type, 
    // but I know what it is, so I will set it
    switch(j){
      case 0: index=2; break;
      case 1: index=4; break;
      case 2: index=3; break;
      default: fprintf(stderr, "Not reached\n"); alarm(40, "Outheader: You never see this!"); exit(1);
    }
  }
  if(w->WriteBank((uint32_t *)hdr, index)){
    fprintf(stderr,"Error writing to zdab file\n");
    alarm(40, "Outheader: error writing to zdab file.");
  }
}


// This function builds a new output file.  If it can't open 
// the file, it aborts the program, so the return pointer does not
// need to be checked.
PZdabWriter * Output(const char * const base, bool clobber){
  const int maxlen = 1024;
  char outfilename[maxlen];

  if(snprintf(outfilename, maxlen, "%s.zdab", base) >= maxlen){
    outfilename[maxlen-1] = 0; // or does snprintf do this already?
    fprintf(stderr, "WARNING: Output filename truncated to %s\n",
            outfilename);
    alarm(40, "Output: output filename truncated");
  }

  if(!access(outfilename, W_OK)){
    if(!clobber){
      fprintf(stderr, "%s already exists and you told me not to "
              "overwrite it!\n", outfilename);
      alarm(40, "Output: Should not overwrite that file.");
      exit(1);
    }
    unlink(outfilename);
  }
  else if(!access(outfilename, F_OK)){
    fprintf(stderr, "%s already exists and we can't overwrite it!\n",
            outfilename);
    alarm(40, "Output: Cannot overwrite that file.");
    exit(1);
  }

  PZdabWriter * const ret = new PZdabWriter(outfilename, 0);

  if(!ret || !ret->IsOpen()){
    fprintf(stderr, "Could not open output file %s\n", outfilename);
    alarm(40, "Output: Cannot open file.");
    exit(1);
  }
  return ret;
}

