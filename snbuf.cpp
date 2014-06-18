// Supernova Buffer Code
//
// K Labe June 17 2014

#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "snbuf.h"

// This function initializes the two SN Buffers
void InitializeBuf(){
  for(int i=0; i<EVENTNUM; i++){
    burstev[i] = (char*) malloc(NWREC*sizeof(uint32_t));
    memset(burstev[i],0,NWREC*sizeof(uint32_t));
    bursttime[i]=0;
  }
}

// This function drops old events from the buffer once they expire
void UpdateBuf(uint64_t longtime, int & bursthead, int & bursttail){
  // The case that the buffer is empty
  if(bursthead==-1)
    return;
  // Normal Case
  while(bursttime[bursthead] < longtime - BurstTicks && bursthead!=-1){
    bursttime[bursthead] = 0;
    for(int j =0; j < NWREC*sizeof(uint32_t); j++){
      burstev[bursthead][j] = 0;
    }
    // Advance the head
    if(bursthead < EVENTNUM -1)
      bursthead++;
    else
      bursthead=0;
    // Reset to empty state if we have emptied the queue
    if(bursthead==bursttail){
      bursthead=-1;
      bursttail=-1;
    }
    fprintf(stderr, "bursttail: %d\n", bursttail);
  }
}

// This fuction adds events to an open Burst File
void AddEvBFile(int & bursthead, PZdabWriter* const b){
  // Write out the data
  if(b->WriteBank((uint32_t *)burstev[bursthead], kZDABindex))
    fprintf(stderr, "Error writing zdab to burst file\n");
  // The drop the data from the buffer
  for(int j=0; j < NWREC*sizeof(uint32_t); j++){
    burstev[bursthead][j] = 0;
  }
  bursttime[bursthead] = 0;
  if(bursthead < EVENTNUM - 1)
    bursthead++;
  else
    bursthead=0;
}

// This function adds a new event to the buffer
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime,
              int & bursthead, int & bursttail, const int reclen){
  // Check whether we will overflow the buffer
  if(bursthead==bursttail && bursthead!=-1){
    fprintf(stderr, "ALARM: Burst Buffer has overflowed!\n");
  }
  
  // Write the event to the buffer
  else{
    // If buffer empty, set pointers appropriately
    if(bursttail==-1){
      bursttail=0;
      bursthead=0;
    }
    if(reclen < NWREC*4)
      memcpy(burstev[bursttail], zrec+1, reclen);
    else
      fprintf(stderr, "ALARM: Event too big for buffer!\n");
    bursttime[bursttail] = longtime;
    if(bursttail<EVENTNUM - 1)
      bursttail++;
    else
      bursttail=0;
    fprintf(stderr,"%i \t %i \n", bursthead, bursttail);
  }
}

// This function computes the number of burst candidate events currently
// in the buffer
int Burstlength(int bursthead, int bursttail){
  int burstlength = 0;
  if(bursthead!=-1){
    if(bursthead<bursttail)
      burstlength = bursttail - bursthead;
    else
      burstlength = EVENTNUM + bursttail - bursthead;
  }
  return burstlength;
}
