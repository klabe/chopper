// Supernova Buffer Header
// 
// K Labe, June 17 2014
// K Labe, September 23 2014 - Move burstbool here from main file
// K Labe, September 24 2014 - Move module variables to source file

// This function should be called once at the beginning of a subfile to set
// up the burst buffers
void InitializeBuf();

// This function drops old events from the buffer once they expire
// longtime specifies the current time (see comment elsewhere for exact def.)
// Events older than BurstLength (in secs) are expired.
void UpdateBuf(uint64_t longtime, int BurstLength);

// This function writes an event to an open Burst File b
void AddEvBFile(PZdabWriter* const b);

// This function adds an event to the buffer
// If the event is too large, a message is printed and the event does not 
// enter the buffer.  (This should not occur).
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime, const int reclen);

// This function returns the number of events in the buffer
int Burstlength();

// This function writers out the allowable portion of the buffer to a burst 
// file b.  Longtime again specifies the current time (see comment elsewhere 
// for definition).  By allowable, we mean that the portion of the burst not
// occuring within the integration period used to determine whether the burst 
// has ended.
void Writeburst(uint64_t longtime, PZdabWriter* b);

// This function opens a new burst file b.  Longtime is the present time (see 
// definition elsewhere).  Headertypes in the number of distinct kinds of header
// records saved in the header buffer (header[]).  Clobber tells whether to
// write over existing files.
void Openburst(PZdabWriter* & b, uint64_t longtime, int headertypes,
               char* outfilebase, char* header[], bool clobber);

// This function writes out the remainder of the burst buffer when the burst
// ends into the file b, and closes it.  Longtime is the present time (see 
// definition elsewhere), which is used to provide some statistics about the
// burst in the log.
void Finishburst(PZdabWriter* & b, uint64_t longtime);
