// Stonehenge
// K Labe and M Strait, U Chicago, 2013-2014.

// Stonehenge is a set of utilties for handling ZDAB files in a 
// low-latency way, designed to meet the needs of the level two 
// trigger and the supernova trigger.  The utilities are these:
// 1. Supernova buffer, an analogue to RAT's burst processor.
// 2. Chopper, available in older version (see tag: FinalChopper),
//     for splitting a ZDAB into smaller pieces
// 3. L2 cut, currently based on nhit, but generalizable
// 4. Some data quality checks, particularly on time.
// 5. Interface to Redis database for recording information about cut
// 6. Interface to alarm & heartbeat system

// Explanation of the various clocks used in this program:
// The 50MHz clock is tracked for accuracy, and the 10MHz clock for 
// uniqueness.  To handle the situation in which the 50MHz clock rolls over,
// I also keep track of an internal longtime variable, which is a 64-bit
// 50MHz clock, which will last 5000 years without rolling over.  Epoch 
// counts the number of times to 50MHz clock has rolled over since
// longtime started counting.  We also track walltime, which is unix time, 
// in order to write to the database with a time stamp. 

#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <string>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <limits.h>
#include <fstream>
#include <signal.h>
#include <time.h>
#include "hiredis.h"
#include "curl/curl.h"
#include "snbuf.h"

#define EXTASY 0x8000 // Bit 15

static int NHITCUT = 30;

// Whether to overwrite existing output
static bool clobber = true;

// Write to redis database?
static bool yesredis = false;

// Tells us when the 50MHz clock rolls over
static const uint64_t maxtime = (1UL << 43);

// Maximum time allowed between events without a complaint
static const uint64_t maxjump = 10*50000000; // 50 MHz time

// Maximum time drift allowed between two clocks without a complaint
static const int maxdrift = 5000; // 50 MHz ticks (1 us)

// Trigger Bitmask
static uint32_t bitmask = 0x007F8000; // Masking out the external triggers

static char* password = NULL;

// Structure to hold all the relevant times
struct alltimes
{
uint64_t time10;
uint64_t time50;
uint64_t longtime;
int epoch;
};

// Function to Print ZDAB records to screen readably
void hexdump(char* const ptr, const int len){
  for(int i=0; i < len/16 +1; i++){
    char* lptr = ptr+i*16;
    for(int j=0; j<16; j++){
      fprintf(stderr,"%.2x", (unsigned char) lptr[j]);
    }
    fprintf(stderr, " ");
    for(int j=0; j<16; j++){
      fprintf(stderr, "%c", isprint(lptr[j])?lptr[j]:'.');
    }
    fprintf(stderr, "\n");
  } 
}

// This function sends alarms to the website
static void alarm(CURL* curl, const int level, const char* msg)
{
  char curlmsg[256];
  sprintf(curlmsg, "name=L2&level=%d&message=%s",level,msg);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(curlmsg));
  CURLcode res = curl_easy_perform(curl);
  if(res != CURLE_OK)
    fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
}

// This function writes out the ZDAB record
static void OutZdab(nZDAB * const data, PZdabWriter * const zwrite,
                    PZdabFile * const zfile)
{
  if(!data) return;
  const int index = PZdabWriter::GetIndex(data->bank_name);
  if(index < 0) fprintf(stderr, "Unrecognized bank name\n");
  else{
    uint32_t * const bank = zfile->GetBank(data);
    zwrite->WriteBank(bank, index);
  }
}

// This function writes out the header buffer to a file
static void OutHeader(const GenericRecordHeader * const hdr,
                      PZdabWriter* const w, const int j)
{
  if (!hdr) return;

  int index = PZdabWriter::GetIndex(hdr->RecordID);
  if(index < 0){
    // PZdab for some reason got zero for the header type, 
    // but I know what it is, so I will set it
    switch(j){
      case 0: index=2; break;
      case 1: index=4; break; 
      case 2: index=3; break;
      default: fprintf(stderr, "Not reached\n"); exit(1);
    }
  }
  if(w->WriteBank((uint32_t *)hdr, index))
    fprintf(stderr,"Error writing to zdab file\n");
}

// This function builds a new output file.  If it can't open 
// the file, it aborts the program, so the return pointer does not
// need to be checked.
static PZdabWriter * Output(const char * const base)
{
  const int maxlen = 1024;
  char outfilename[maxlen];
  
  if(snprintf(outfilename, maxlen, "%s.zdab", base) >= maxlen){
    outfilename[maxlen-1] = 0; // or does snprintf do this already?
    fprintf(stderr, "WARNING: Output filename truncated to %s\n",
            outfilename);
  }

  if(!access(outfilename, W_OK)){
    if(!clobber){
      fprintf(stderr, "%s already exists and you told me not to "
              "overwrite it!\n", outfilename);
      exit(1);
    }
    unlink(outfilename);
  }
  else if(!access(outfilename, F_OK)){
    fprintf(stderr, "%s already exists and we can't overwrite it!\n",
            outfilename);
    exit(1);
  } 

  PZdabWriter * const ret = new PZdabWriter(outfilename, 0);

  if(!ret || !ret->IsOpen()){
    fprintf(stderr, "Could not open output file %s\n", outfilename);
    exit(1);
  }
  return ret;
}

// This function closes the completed primary chunk and  moves the file
// to the appropriate directory.  It should be used here in place of the 
// PZdabWriter Close() call. 
static void Close(const char* const base, PZdabWriter* const w, 
                  CURL* curl, const bool extasy)
{
  char buff1[256];
  snprintf(buff1, 256, "/trigger/home/PCAdata/%s.zdab", base);
  const char* linkname = buff1;
  char buff2[256];
  snprintf(buff2, 256, "%s.zdab", base);
  const char* outname = buff2;
  char* message = "PCA File could not be copied";
  w->Close();
  if(extasy){
    if(link(outname, linkname)){
      alarm(curl, 1, message);
    }
  }

  std::ofstream myfile;
  myfile.open("chopper.run.log", std::fstream::app);
  myfile.close();
}

// Function to assist in parsing the input variables                  
static double getcmdline_d(const char opt)
{
  char * endptr;
  errno = 0;
  const double answer = strtod(optarg, &endptr);
  if((errno==ERANGE && (fabs(answer) == HUGE_VAL)) ||
     (errno != 0 && answer == 0) ||
     endptr == optarg || *endptr != '\0'){
    fprintf(stderr, "%s (given with -%c) isn't a number I can handle\n",
            optarg, opt);
    exit(1);
  }
  return answer;
}

// Another function to assist in parsing the input variables
static int getcmdline_l(const char opt)
{
  char * endptr;
  errno = 0;
  const unsigned int answer = strtol(optarg, &endptr, 10);
  if((errno == ERANGE && (answer == UINT_MAX)) || 
     (errno != 0 && answer == 0) || 
     endptr == optarg || *endptr != '\0'){
    fprintf(stderr, "%s (given with -%c) isn't a number I can handle\n",
            optarg, opt);
    exit(1);
  }
  return answer;
}

// Prints the Command Line help text
static void printhelp()
{
  printf(
  "chopper: Chops a ZDAB file into smaller ones by time.\n"
  "\n"
  "Mandatory options:\n"
  "  -i [string]: Input file\n"
  "  -o [string]: Base of output files\n"
  "\n"
  "Physics options:\n"
  "  -l [n]: L2 nhit cut (default %d)\n"
  "  -b [n]: Burst file nhit cut (default %d)\n"
  "  -t [n]: Burst window width in seconds (default %d) \n"
  "  -u [n]: Burst size threshold event count (default %d) \n"
  "\n"
  "Misc/debugging options\n"
  "  -n: Do not overwrite existing output (default is to do so)\n"
  "  -r: Write statistics to the redis database.\n"
  "  -h: This help text\n"
  , NHITCUT, NHITBCUT, BurstLength, BurstSize
  );
}

// This function opens the redis connection at startup
static void Openredis(redisContext **redis)
{
  *redis = redisConnect("cp4.uchicago.edu", 6379);
  if((*redis)->err)
    printf("Error: %s\n", (*redis)->errstr);
  else
    printf("Connected to Redis.\n");
}

// This function closes the redis connection when finished
static void Closeredis(redisContext **redis)
{
  redisFree(*redis);
}

// This function writes statistics to redis database
static void Writetoredis(redisContext *redis, const int l1, const int l2,
                         const bool burst, const int time)
{
  const int NumInt = 10;
  const int intervals[NumInt] = {1, 3, 9, 29, 90, 280, 867, 2677, 8267, 25531};
  for(int i=0; i < NumInt; i++){
    int ts = time/intervals[i];
    void* reply = redisCommand(redis, "INCRBY ts:%d:%d:L1 %d", intervals[i], ts, l1);
    reply = redisCommand(redis, "EXPIRE ts:%d:%d:L1 %d", intervals[i], ts, 12000*intervals[i]);

    reply = redisCommand(redis, "INCRBY ts:%d:%d:L2 %d", intervals[i], ts, l2);
    reply = redisCommand(redis, "EXPIRE ts:%d:%d:L2 %d", intervals[i], ts, 12000*intervals[i]);

    if(burst){
      reply = redisCommand(redis, "SET ts:%d:id:%d:Burst 1", intervals[i], ts);
      reply = redisCommand(redis, "EXPIRE ts:%d:id:%d:Burst", intervals[i], ts, 12000*intervals[i]);
    }
  }
}

// Open a curl connection
void Opencurl(CURL** curl, char* password){
  *curl = curl_easy_init();
  char address[264];
  sprintf(address, "http://snoplus:%s@snopl.us/monitoring/log", password);
  if(curl){
    curl_easy_setopt(*curl, CURLOPT_URL, address);
  }
  else
    fprintf(stderr,"Could not initialize curl object");
}

// Close a curl connection
void Closecurl(CURL** curl){
  curl_easy_cleanup(*curl);
}

// This function interprets the command line arguments to the program
static void parse_cmdline(int argc, char ** argv, char * & infilename,
                          char * & outfilebase)
{
  const char * const opts = "hi:o:l:b:t:u:nr";

  bool done = false;
  
  infilename = outfilebase = NULL;

  while(!done){ 
    const char ch = getopt(argc, argv, opts);
    switch(ch){
      case -1: done = true; break;

      case 'i': infilename = optarg; break;
      case 'o': outfilebase = optarg; break;

      case 'l': NHITCUT = getcmdline_l(ch); break;
      case 'b': setburstcut(getcmdline_l(ch)); break;
      case 't': settimecut(getcmdline_d(ch)); break;
      case 'u': setratecut(getcmdline_d(ch)); break;

      case 'n': clobber = false; break;
      case 'r': yesredis = true; password = optarg; break;

      case 'h': printhelp(); exit(0);
      default:  printhelp(); exit(1);
    }
  }

  if(!infilename)  fprintf(stderr, "Give an input file with -i\n");
  if(!outfilebase) fprintf(stderr, "Give an output base with -o\n");

  if(!infilename || !outfilebase){
    printhelp();
    exit(1);
  }

}

// This function calculates the time of an event as measured by the
// varlous clocks we are interested in.
static alltimes compute_times(const PmtEventRecord * const hits, CURL* curl,
                              alltimes oldat, uint64_t & eventn, int & orphan)
{
  alltimes newat = oldat;
  if(eventn == 1){
    newat.time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
                             + hits->TriggerCardData.Bc50_1;
    newat.time10 = (uint64_t(hits->TriggerCardData.Bc10_2) <<32)
                             + hits->TriggerCardData.Bc10_1;
    if(newat.time50 == 0) orphan++;
    newat.longtime = newat.time50;
  }
  else{
    // Get the current 50MHz Clock Time
    // Implementing Part of Method Get50MHzTime() 
    // from PZdabFile.cxx
    newat.time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
                             + hits->TriggerCardData.Bc50_1;

    // Get the current 10MHz Clock Time
    // Method taken from zdab_convert.cpp
    newat.time10 = (uint64_t(hits->TriggerCardData.Bc10_2) << 32)
                             + hits->TriggerCardData.Bc10_1;

    // Check for consistency between clocks
    const int dd = ( (oldat.time10 - newat.time10)*5 > oldat.time50 - newat.time50 ? 
                     (oldat.time10 - newat.time10)*5 - (oldat.time50 - newat.time50) :
                     (oldat.time50 - newat.time50) - (oldat.time10 - newat.time10)*5 );
    if (dd > maxdrift){
      char msg[128];
      sprintf(msg, "Stonehenge: The Clocks jumped by %i ticks!\n", dd);
      alarm(curl, 1, msg);
      fprintf(stderr, msg);
    }

    // Check for pathological case
    if (newat.time50 == 0){
      newat.time50 = oldat.time50;
      orphan++;
      return newat;
    }

    // Check for time running backward:
    if (newat.time50 < oldat.time50){
      // Is it reasonable that the clock rolled over?
      if ((oldat.time50 + newat.time50 < maxtime + maxjump) && dd < maxdrift && (oldat.time50 > maxtime - maxjump) ) {
        fprintf(stderr, "New Epoch\n");
        newat.epoch++;
      }
      else{
        const char msg[128] = "Stonehenge: Time running backward!\n";
        alarm(curl, 1, msg);
        fprintf(stderr, msg);
        // Assume for now that the clock is wrong
        newat.time50 = oldat.time50;
      }
    }

    // Check that the clock has not jumped ahead too far:
    if (newat.time50 - oldat.time50 > maxjump){
      char msg[128] = "Stonehenge: Large time gap between events!\n";
      alarm(curl, 1, msg);
      fprintf(stderr, msg);
      // Assume for now that the time is wrong
      newat.time50 = oldat.time50;
    }

    // Set the Internal Clock
    newat.longtime = newat.time50 + maxtime*newat.epoch;
  }
  return newat;
}

// Function to retreive the trigger word
// This method copied from zdab_convert
uint32_t triggertype(PmtEventRecord* hits){
  uint32_t mtcwords[6];
  memcpy(mtcwords, &(hits->TriggerCardData), 6*sizeof(uint32_t));
  uint32_t triggerword = ((mtcwords[3] & 0xff000000) >> 24) |
                         ((mtcwords[4] & 0x3ffff) << 8);
  return triggerword;
}

// MAIN FUCTION 
int main(int argc, char *argv[])
{
  char * infilename = NULL, * outfilebase = NULL;

  parse_cmdline(argc, argv, infilename, outfilebase);

  FILE* infile = fopen(infilename, "rb");

  PZdabFile* zfile = new PZdabFile();
  if (zfile->Init(infile) < 0){
    fprintf(stderr, "Did not open file\n");
    exit(1);
  }

  // Prepare to record statistics in redis database
  redisContext *redis;
  CURL *curl;
  if(yesredis) 
    Openredis(&redis);
    Opencurl(&curl, password);
  int l1=0;
  int l2=0;
  bool burstbool=false;
  bool extasy=false;

  // Initialize the various clocks
  alltimes alltime;
  int walltime = 0;
  int oldwalltime = 0;

  // Setup initial output file
  PZdabWriter* w1  = Output(outfilebase);
  PZdabWriter* b = NULL; // Burst event file

  // Set up the Header Buffer
  const int headertypes = 3;
  const uint32_t Headernames[headertypes] = 
    { RHDR_RECORD, TRIG_RECORD, EPED_RECORD };
  char* header[headertypes];
  for(int i = 0; i<headertypes; i++){
    header[i] = (char*) malloc(NWREC);
    memset(header[i],0,NWREC);
  }

  // Set up the Burst Buffer
  InitializeBuf();
  int bcount = 0;

  // Loop over ZDAB Records
  uint64_t eventn = 0, recordn = 0;
  int orphan = 0;
  int nhit = 0;
  while(nZDAB * const zrec = zfile->NextRecord()){
    // Check to fill Header Buffer
    for(int i=0; i<headertypes; i++){
      if (zrec->bank_name == Headernames[i]){
        memset(header[i],0,NWREC);
        unsigned long recLen=((GenericRecordHeader*)zrec)->RecordLength;
        SWAP_INT32(zrec,recLen/sizeof(uint32_t));
        memcpy(header[i], zrec+1, recLen);
        SWAP_INT32(zrec,recLen/sizeof(uint32_t));
      }
    }

    // If the record has an associated time, compute all the time
    // variables.  Non-hit records don't have times.
    if(PmtEventRecord * hits = zfile->GetPmtRecord(zrec)){
      nhit = hits->NPmtHit;
      eventn++;
      alltime = compute_times(hits, curl, alltime, eventn, orphan);
      // Has wall time changed?
      if(walltime!=0)
        oldwalltime=walltime;
      walltime=(int)time(NULL);
      if (walltime!=oldwalltime){
        if(yesredis) 
          Writetoredis(redis, l1, l2, burstbool,oldwalltime);
        // Reset statistics
        l1 = 0;
        l2 = 0;
        burstbool = false;
      }

      // Burst Detection Here
      // If the current event is over our burst nhit threshold (NHITBCUT):
      //   * First update the buffer by dropping events older than BurstLength
      //   * Then add the new event to the buffer
      //   * If we were not in a burst, check whether one has started
      //   * If we were in a burst: write event to file, and check if the burst has ended
      // We also check for EXTASY triggers here to send PCA data to Freija

      uint32_t word = triggertype(hits); 
      if(!extasy){
        if((word & EXTASY ) == 0x00008000) // Bit 15
          extasy = true;
      }
      if(nhit > NHITBCUT && (word & bitmask == 0) ){
        UpdateBuf(alltime.longtime);
        int reclen = zfile->GetSize(hits);
        AddEvBuf(zrec, alltime.longtime, reclen*sizeof(uint32_t));
        int burstlength = Burstlength();

        int starttick = 0;

        // Open a new burst file if a burst starts
        if(!burst){
          if(burstlength>BurstSize){
            burst=true;
            bcount=burstlength;
            starttick=alltime.longtime;
            fprintf(stderr, "Burst %i has begun!\n", burstindex);
            char buff[32];
            sprintf(buff,"Burst_%s_%i", outfilebase, burstindex);
            b = Output(buff);
            for(int i=0; i<headertypes; i++){
              OutHeader((GenericRecordHeader*) header[i], b, i);
            }
          }
        }
        // While in a burst
        if(burst){
          burstbool=true;
          bcount++;
          Writeburst(alltime.longtime, b);
          // Check if the burst has ended
          if(burstlength<EndRate){
            Finishburst(b);
            b->Close();
            burst=false;
            int btime = alltime.longtime - starttick;
            float btimesec = btime/50000000.;
            fprintf(stderr, "Burst %i has ended.  It contains %i events"
                  " and lasted %.2f seconds.\n", burstindex, bcount, btimesec);
            burstindex++;
            // Reset to prepare for next burst
            bcount=0;
          }
        }

      } // End Burst Loop
      // L2 Filter
      // *Keep even if nhit over threshold
      // *Also keep event if it was externally triggered
      if(nhit>NHITCUT || (word & bitmask != 0) ){
        OutZdab(zrec, w1, zfile);
        l2++;
      }

    } // End Loop for Event Records
    // Write out all non-event records:
    else{
      OutZdab(zrec, w1, zfile);
      l2++;
    }
    recordn++;
    l1++;
  } // End of the Event Loop for this subrun file
  if(w1) Close(outfilebase, w1, &curl, extasy);

  Closeredis(&redis);
  Closecurl(&curl);
  printf("Done. %lu record%s, %lu event%s processed\n",
         recordn, recordn==1?"":"s", eventn, eventn==1?"":"s");
  return 0;
}
