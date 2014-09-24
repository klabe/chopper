// Supernova Buffer Header
// 
// K Labe, June 17 2014
// K Labe, September 23 2014 - Move burstbool here from main file
// K Labe, September 24 2014 - Move module variables to source file

void InitializeBuf();
void UpdateBuf(uint64_t longtime, int BurstLength);
void AddEvBFile(PZdabWriter* const b);
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime, const int reclen);
int Burstlength();
void Writeburst(uint64_t longtime, PZdabWriter* b);
void Openburst(PZdabWriter* & b, alltimes alltime, int headertypes,
               char* outfilebase, char* header[], bool clobber);
void Finishburst(PZdabWriter* & b, alltimes alltime);
