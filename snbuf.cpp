// Supernova Buffer Code
//
// K Labe June 17 2014

#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "snbuf.h"

static const int EVENTNUM = 1000; // Maximum Burst buffer depth
static const int ENDWINDOW = 1*50000000; // Integration window for ending bursts

static char* burstev[EVENTNUM]; // Burst Event Buffer
static uint64_t bursttime[EVENTNUM]; // Burst Time Buffer
static int bursthead;
static int bursttail;

// This function initializes the two SN Buffers
void InitializeBuf(){
  for(int i=0; i<EVENTNUM; i++){
    burstev[i] = (char*) malloc(NWREC*sizeof(uint32_t));
    memset(burstev[i],0,NWREC*sizeof(uint32_t));
    bursttime[i]=0;
  }
  bursthead = -1;
  bursttail = -1;
}

// This function drops old events from the buffer once they expire
void UpdateBuf(uint64_t longtime, int BurstLength){
  // The case that the buffer is empty
  if(bursthead==-1)
    return;
  // Normal Case
  int BurstTicks = BurstLength*50000000; // length in ticks
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
  }
}

// This fuction adds events to an open Burst File
void AddEvBFile(PZdabWriter* const b){
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
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime, const int reclen){
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
  }
}

// This function computes the number of burst candidate events currently
// in the buffer
int Burstlength(){
  int burstlength = 0;
  if(bursthead!=-1){
    if(bursthead<bursttail)
      burstlength = bursttail - bursthead;
    else
      burstlength = EVENTNUM + bursttail - bursthead;
  }
  return burstlength;
}

// This function writes out the allowable portion of the buffer to a burst file
void Writeburst(uint64_t longtime, PZdabWriter* b){
  while(bursttime[bursthead] < longtime - ENDWINDOW && bursthead < bursttail){
    AddEvBFile(b);
  }
}

// This function writes out the remainder of the buffer when burst ends
void Finishburst(PZdabWriter* b){
  while(bursthead < bursttail+1){
    AddEvBFile(b);
  }
  bursthead = -1;
  bursttail = -1;
}
