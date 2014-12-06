// Supernova Buffer Code
//
// K Labe June 17 2014
// K Labe September 26 2014 Add code to handle end of file and buffer saving
// K Labe November 2 2014   Use a single contiguous block of memory for buffer

#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "struct.h"
#include "snbuf.h"
#include "curl.h"
#include "output.h"

#define MAXSIZE 30472 // Largest possible event
struct burststate
{
int head;
int tail;
bool burst;
};

static const uint64_t maxtime = (1UL << 43);
static char* burstname;

// Stuff for the burst buffer
static const int EVENTNUM = 1000;        // Maximum Burst buffer depth
static const int ENDWINDOW = 1*50000000; // Integration window for ending bursts
static char* burstev[EVENTNUM];      // Burst Event Buffer
static uint64_t bursttime[EVENTNUM]; // Burst Time Buffer
static burststate burstptr; // Object to hold pointers to head and tail of burst
static uint64_t starttick = 0;   // Start time (in 50 MHz ticks) of burst
static int burstindex = 0;  // Number of bursts seen
static int bcount = 0;      // Number of events in present burst

// Stuff for the header buffer
static const int headertypes = 3;
static const uint32_t Headernames[headertypes] = 
  { RHDR_RECORD, TRIG_RECORD, EPED_RECORD };
static char* header[headertypes];

// These are the filenames for storing the buffer between subfiles
static const char* fnburststate = "burststate.txt";
static const char* fnburstev    = "burstev.bin";
static const char* fnbursttime  = "bursttime.txt";

// This function initializes the two SN Buffers.  It tries to read in the 
// state of the buffer from file, or otherwise initializes it empty.  It also 
// initializes the header buffer.
void InitializeBuf(){
  // Try to read from file
  FILE* fburststate = fopen(fnburststate, "r");
  FILE* fburstev    = fopen(fnburstev,    "rb");
  FILE* fbursttime  = fopen(fnbursttime,  "r");
  if(fburststate && fburstev && fbursttime){
    fscanf(fburststate, "%d %d %d", &burstptr.head, &burstptr.tail,
                                    &burstptr.burst);
    burstev[0] = (char*) malloc(MAXSIZE*sizeof(uint32_t)*EVENTNUM);
    if(burstev[0] == NULL){
      printf("Error: SN Buffer could not be initialized.\n");
      alarm(40, "Stonehenge: SN Buffer could not be initialized.");
      exit(1);
    }
    for(int i=0; i<EVENTNUM; i++){
      if(fscanf(fbursttime, "%llu \n", &bursttime[i]) != 1)
        bursttime[i] = 0;
      burstev[i] = (char*) (burstev[0] + i*MAXSIZE*sizeof(uint32_t));
    }
    double fburstevsize = ftell(fburstev);
    if(fread(burstev[0], sizeof(char), sizeof(burstev), fburstev) != fburstevsize){
      memset(burstev[0], 0, MAXSIZE*sizeof(uint32_t)*EVENTNUM);
    }
  }
  // Otherwise, initialize empty
  else{
    burstev[0] = (char*) malloc(MAXSIZE*sizeof(uint32_t)*EVENTNUM);
    if(burstev[0] == NULL){
      printf("Error: SN Buffer could not be initialized.\n");
      alarm(40, "Stonehenge: SN Buffer could not be initialized.");
      exit(1);
    }
    for(int i=0; i<EVENTNUM; i++){
      burstev[i] = (char*) (burstev[0] + i*MAXSIZE*sizeof(uint32_t));
      bursttime[i]=0;
    }
    burstptr.head = -1;
    burstptr.tail = -1;
    burstptr.burst = false;
  }

  // Set up the header buffer
  for(int i=0; i<headertypes; i++){
    header[i] = (char*) malloc(NWREC);
    memset(header[i], 0, NWREC);
  }

  // Close files if necessary
  if(fburststate) fclose(fburststate);
  if(fburstev)    fclose(fburstev);
  if(fbursttime)  fclose(fbursttime);
}

// This function clears the pre-loaded buffer if the times are in the future
void Checkbuffer(uint64_t firsttime){
  if(!burstptr.head==-1){
    uint64_t oldtime = bursttime[burstptr.head];
    if( firsttime < oldtime ){
      memset(burstev[0], 0, MAXSIZE*sizeof(uint32_t)*EVENTNUM);
      for(int i=0; i<EVENTNUM; i++){
        bursttime[i] = 0;
      }
      burstptr.head = -1;
      burstptr.tail = -1;
    }
  }
}

// This function drops old events from the buffer once they expire
void UpdateBuf(uint64_t longtime, int BurstLength){
  // The case that the buffer is empty
  if(burstptr.head==-1)
    return;
  // Normal Case
  int BurstTicks = BurstLength*50000000; // length in ticks
  while((bursttime[burstptr.head] < longtime - BurstTicks) && (burstptr.head!=-1)){
    bursttime[burstptr.head] = 0;
    memset(burstev[burstptr.head], 0, MAXSIZE*sizeof(uint32_t));
    AdvanceHead();
    // Reset to empty state if we have emptied the queue
    if(burstptr.head==burstptr.tail){
      burstptr.head=-1;
      burstptr.tail=-1;
    }
  }
}

// This fuction adds events to an open Burst File
void AddEvBFile(PZdabWriter* const b){
  // Write out the data
  if(b->WriteBank((uint32_t *)burstev[burstptr.head], kZDABindex)){
    fprintf(stderr, "Error writing zdab to burst file\n");
    alarm(30, "Stonehenge: Error writing zdab to burst file");
  }
  // Drop the data from the buffer
  memset(burstev[burstptr.head], 0, MAXSIZE*sizeof(uint32_t));
  bursttime[burstptr.head] = 0;
  AdvanceHead();
  bcount++;
}

// This function adds a new event to the buffer
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime, const int reclen,
              PZdabWriter* const b){
  // Check whether we will overflow the buffer
  // If so, first drop oldest event, then write
  if(burstptr.head==burstptr.tail && burstptr.head!=-1){
    fprintf(stderr, "ALARM: Burst Buffer has overflowed!\n");
    alarm(30, "Stonehenge: Burst buffer has overflown.");
    if(!burstptr.burst){
      fprintf(stderr, "ALARM: Burst Threshold larger than buffer!\n");
      alarm(30, "Stonehenge: Burst threshold larger than buffer.");
    }
    else
      AddEvBFile(b);
  }
  
  // Write the event to the buffer
  // If buffer empty, set pointers appropriately
  if(burstptr.tail==-1){
    burstptr.tail=0;
    burstptr.head=0;
  }
  if(reclen < MAXSIZE*4)
    memcpy(burstev[burstptr.tail], zrec+1, reclen);
  else{
    char buf[128];
    sprintf(buf, "ALARM: Event too big for buffer!  %d bytes!  Skipping this event.&notify\n", reclen);
    fprintf(stderr, buf);
    alarm(30, buf);
  }
  bursttime[burstptr.tail] = longtime;
  if(burstptr.tail<EVENTNUM - 1)
    burstptr.tail++;
  else
    burstptr.tail=0;
}

// This function computes the number of burst candidate events currently
// in the buffer
int Burstlength(){
  int burstlength = 0;
  if(burstptr.head!=-1){
    if(burstptr.head<burstptr.tail)
      burstlength = burstptr.tail - burstptr.head;
    else
      burstlength = EVENTNUM + burstptr.tail - burstptr.head;
  }
  return burstlength;
}

// This function writes out the allowable portion of the buffer to a burst file
void Writeburst(uint64_t longtime, PZdabWriter* b){
  while((bursttime[burstptr.head] < longtime - ENDWINDOW) && (burstptr.head < burstptr.tail)){
    AddEvBFile(b);
  }
}

// This function opens a new burst file
void Openburst(PZdabWriter* & b, uint64_t longtime, char* outfilebase, 
               bool clobber){
  starttick = bursttime[burstptr.head];
  char buff[128];
  sprintf(buff, "Burst %i has begun!\n", burstindex);
  fprintf(stderr, buff);
  alarm(20, buff);
  char namebuff[128];
  sprintf(namebuff, "/home/cp/klabe/Burstdata/Burst_%s_%i", burstname, burstindex);
  b = Output(namebuff, clobber);
  for(int i=0; i<headertypes; i++){
    OutHeader((GenericRecordHeader*) header[i], b, i);
  }
}

// This function writes out the remainder of the buffer when burst ends
void Finishburst(PZdabWriter* & b, uint64_t longtime){
  while(burstptr.head < burstptr.tail+1){
    AddEvBFile(b);
  }
  burstptr.head = -1;
  burstptr.tail = -1;
  b->Close();
  uint64_t btime = longtime - starttick;
  float btimesec = btime/50000000.;
  char buff[256];
  sprintf(buff, "Burst %i has ended.  It contains %i events and lasted"
                  " %.2f seconds.\n", burstindex, bcount, btimesec);
  fprintf(stderr, buff);
  alarm(20, buff);
  burstindex++;
  // Reset to prepare for next burst
  bcount = 0;
  burstptr.burst = false;
}

// This function saves the buffer state to disk.
// Burstev is saved in binary, bursttime and burststate are saved in ascii
void Saveburstbuff(){
  FILE* fburststate = fopen(fnburststate, "w");
  FILE* fburstev = fopen(fnburstev, "wb");
  FILE* fbursttime = fopen(fnbursttime, "w");
  fwrite(burstev[0], sizeof(char), MAXSIZE*sizeof(uint32_t)*EVENTNUM, fburstev);
  for(int i=0; i<EVENTNUM; i++){
    fprintf(fbursttime, "%llu \n", bursttime[i]);
  }
  fprintf(fburststate, "%d %d %d", burstptr.head, burstptr.tail, burstptr.burst);
  fclose(fburststate);
  fclose(fburstev);
  fclose(fbursttime);
}

// This function manages the writing of events into a burst file.
bool Burstfile(PZdabWriter* & b, configuration config, alltimes alltime, 
               char* outfilebase, bool clobber){
  // Open a new burst file if a burst starts
  if(!burstptr.burst){
    if(Burstlength() > config.burstsize){
      Openburst(b, alltime.longtime, outfilebase, clobber);
      burstptr.burst = true;
    }
  }
  
  // While in a burst
  if(burstptr.burst){
    Writeburst(alltime.longtime, b);
    // Check whether the burst has ended
    if(Burstlength() < config.endrate){
      Finishburst(b, alltime.longtime);
    }
  }
  return burstptr.burst;
}

// This function wraps up the burst buffer when the end of file is reached
void BurstEndofFile(PZdabWriter* & b, uint64_t longtime){
  Saveburstbuff();
  if(burstptr.burst)
    Finishburst(b, longtime);
}

// This function advances the head pointer appropriately
void AdvanceHead(){
  if(burstptr.head < EVENTNUM - 1)
    burstptr.head++;
  else
    burstptr.head = 0;
}

// This function is used to reset the buffer if the events 
// arrive out of order in a non-recoverable way.
void ClearBuffer(PZdabWriter* & b, uint64_t longtime){
  if(burstptr.burst)
    Finishburst(b, longtime);
  else{
    memset(burstev[0], 0, MAXSIZE*sizeof(uint32_t)*EVENTNUM);
    for(int i=0; i<EVENTNUM; i++){
      bursttime[i] = 0;
    }
    burstptr.head = -1;
    burstptr.tail = -1;
    burstptr.burst = false;
  }
}

// This function checks whether the passed record is a header record, and,
// if it is, writes it to the header buffer.
void FillHeaderBuffer(nZDAB* const zrec){
  for(int i=0; i<headertypes; i++){
    if(zrec->bank_name == Headernames[i]){
      memset(header[i], 0, NWREC);
      unsigned long recLen = ((GenericRecordHeader*)zrec)->RecordLength;
      SWAP_INT32(zrec,recLen/sizeof(uint32_t));
      memcpy(header[i], zrec+1, recLen);
      SWAP_INT32(zrec,recLen/sizeof(uint32_t));
    }
  }
}

// This function returns the epoch value used to write timestamp
// bursttime[burstptr.head]
int GetEpoch()
{
  uint64_t time = bursttime[burstptr.head];
  int epoch = time/maxtime;
  return epoch;
}

// This function sets the burst directory
void setburst(char* burstdir){
  burstname = burstdir;
}
