// File Output header file
//
// K Labe, September 24 2014

void OutZdab(nZDAB* const data, PZdabWriter* const zwrite,
             PZdabFile* const zfile);
void OutHeader(const GenericRecordHeader* const hdr, PZdabWriter* const w,
               const int j);
PZdabWriter* Output(const char * const base, bool clobber);
