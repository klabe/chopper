// Supernova Buffer Header
// 
// K Labe, June 17 2014
// K Labe, September 23 2014 - Move burstbool here from main file
// K Labe, September 24 2014 - Move module variables to source file
// K Labe, September 26 2014 - Add new functions to handle saving at end of file
// K Labe, September 29 2014 - Add ClearBuffer and AdvanceHead functions

// This function should be called once at the beginning of a subfile to set
// up the burst buffers.  It tries to read in the buffer state from file, or
// otherwise initializes empty.
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
// The PZdabWriter object is required so that an event can be written to file
// if a burst is ongoing and the buffer overflows.
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime, const int reclen,
              PZdabWriter* const b);

// This function returns the number of events in the buffer
int Burstlength();

// This function writers out the allowable portion of the buffer to a burst 
// file b.  Longtime again specifies the current time (see comment elsewhere 
// for definition).  By allowable, we mean that portion of the burst not
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

// This function is used to save the state of the burstbuffer to disk so that
// the burst detection algorithm can pick up from where it left off when the 
// next file begins.
void Saveburstbuff();

// This function manages the writing of events into a burst file.  It returns
// a bool stating whether a burst is ongoing.
bool Burstfile(PZdabWriter* & b, configuration config, alltimes alltime, 
               int headertypes, char* outfilebase, char* header[], bool clobber);

// This function wraps up the burst buffer when the end of a subfile is reached.
void BurstEndofFile(PZdabWriter* & b, uint64_t longtime);

// This function just advances the pointer to the head of the burst properly
// when the buffer is updated.
void AdvanceHead();

// This function is used to clear the buffer when Stonehenge detects that 
// the event timestamps have jumped in a non-recoverable way.  b and longtime 
// are used in the event that a burst is ongoing when the buffer needs to be 
// cleared.
void ClearBuffer(PZdabWriter* & b, uint64_t longtime);
