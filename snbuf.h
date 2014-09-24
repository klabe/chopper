// Supernova Buffer Header
// 
// K Labe, June 17 2014
// K Labe, September 23 2014 - Move burstbool here from main file
// K Labe, September 24 2014 - Move module variables to source file

void InitializeBuf();
void UpdateBuf(uint64_t longtime);
void AddEvBFile(PZdabWriter* const b);
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime, const int reclen);
int Burstlength();
void Writeburst(uint64_t longtime, PZdabWriter* b);
void Finishburst(PZdabWriter* b);
void setburstcut(unsigned int cut);
void settimecut(double time);
void setratecut(double rate);
