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
// in order to write to the database with a time stamp.  There is also a 
// variable called exptime, which gives the current time at which the lowered 
// trigger threshold expires(d), if any. 

#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <string>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <limits.h>
#include <fstream>
#include <signal.h>
#include <time.h>
#include "redis.h"
#include "curl.h"
#include "SFMT.h"
#include "curl/curl.h"
#include "struct.h"
#include "snbuf.h"
#include "output.h"

#define EXTASY 0x8000 // Bit 15

// This variable holds the configuration of the parameters that determine the
// behavior of the filter
static configuration config;

// This variable holds the current Nhitcut, which can be either the Hi or 
// Lo Nhitcut, depending on what has been going on
static int NHITCUT;

// Whether to overwrite existing output
static bool clobber = true;

// Whether to write to redis database
static bool yesredis = false;

// Tells us when the 50MHz clock rolls over
static const uint64_t maxtime = (1UL << 43);

// Maximum time allowed between events without a complaint
static const uint64_t maxjump = 10*50000000; // 50 MHz time

// Maximum time drift allowed between two clocks without a complaint
static const int maxdrift = 5000; // 50 MHz ticks (1 us)

static char* password = NULL;

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

// This function closes the completed primary chunk and  moves the file
// to the appropriate directory.  It should be used here in place of the 
// PZdabWriter Close() call. 
static void Close(const char* const base, PZdabWriter* const & w, 
                  const bool extasy)
{
  char buff1[256];
  snprintf(buff1, 256, "/trigger/home/PCAdata/%s.zdab", base);
  const char* linkname = buff1;
  char buff2[256];
  snprintf(buff2, 256, "%s.zdab", base);
  const char* outname = buff2;
  w->Close();
  if(extasy){
    if(link(outname, linkname)){
      char* message = "PCA File could not be copied";
      alarm(30, message);
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
  "  -c [string]: Configuration file\n"
  "\n"
  "Misc/debugging options\n"
  "  -n: Do not overwrite existing output (default is to do so)\n"
  "  -r: Write statistics to the redis database.\n"
  "  -h: This help text\n"
  );
}

// This function prints some information at the end of the file
static void PrintClosing(char* outfilebase, counts count, int stats[], 
                         int psstats[]){
  char messg[128];
  sprintf(messg, "Stonehenge: Subfile %s finished."
                 "  %lu events processed.\n", outfilebase, count.eventn);
  alarm(21, messg);
  printf("Done. %lu record%s, %lu event%s processed\n"
         "%lu events selected by prescaler\n"
         "%i events (%i prescaled events) pass no cut\n"
         "%i events (%i prescaled events) pass only nhit cut\n"
         "%i events (%i prescaled events) pass only external trigger cut\n"
         "%i events (%i prescaled events) pass both external trigger and nhit cuts\n"
         "%i events (%i prescaled events) pass only retrigger cut\n"
         "%i events (%i prescaled events) pass both retrigger cut and nhit cut\n"
         "%i events (%i prescaled events) pass both retrigger cut and nhit cut\n"
         "%i events (%i prescaled events) pass all three cuts\n",
         count.recordn, count.recordn==1?"":"s", count.eventn, 
         count.eventn==1?"":"s", count.prescalen,
         stats[0], psstats[0], stats[1], psstats[1], stats[2], psstats[2],
         stats[3], psstats[3], stats[4], psstats[4], stats[5], psstats[5],
         stats[6], psstats[6], stats[7], psstats[7]);
}

// This function reads the configuration file and sets the cut parameters.
void ReadConfig(const char* filename){ 
   FILE* configfile = fopen(filename, "r");
   if(configfile == NULL){
     printf("Could not open configuration file.\n");
     return;
   }

   char param[16];
   int value;

   while(fscanf(configfile, "%s %d\n", param, &value)==2){
     if     (strcmp(param, "nhithi")       == 0) config.nhithi       = value;
     else if(strcmp(param, "nhitlo")       == 0) config.nhitlo       = value;
     else if(strcmp(param, "lothresh")     == 0) config.lothresh     = value;
     else if(strcmp(param, "lowindow")     == 0) config.lowindow     = value;
     else if(strcmp(param, "nhitretrig")   == 0) config.retrigcut    = value;
     else if(strcmp(param, "retrigwindow") == 0) config.retrigwindow = value;
     else if(strcmp(param, "prescale")     == 0) config.prescale     = value;
     else if(strcmp(param, "nhitburst")    == 0) config.nhitbcut     = value;
     else if(strcmp(param, "burstwindow")  == 0) config.burstwindow  = value;
     else if(strcmp(param, "burstsize")    == 0) config.burstsize    = value;
     else
        printf("ReadConfig does not recognize parameter %s.  Ignoring.\n",
               param);
   }
   rewind(configfile);
   while(fscanf(configfile, "%s %x\n", param, &value)==2){
     if(strcmp(param, "bitmask") == 0) config.bitmask = value;
   }
}

// This function interprets the command line arguments to the program
static void parse_cmdline(int argc, char ** argv, char * & infilename,
                          char * & outfilebase)
{
  char* configfile = NULL;
  const char * const opts = "hi:o:l:b:t:u:c:nr";

  bool done = false;
  bool configure = false; // was there a configuration file provided?
  
  infilename = outfilebase = NULL;

  while(!done){ 
    const char ch = getopt(argc, argv, opts);
    switch(ch){
      case -1: done = true; break;

      case 'i': infilename = optarg; break;
      case 'o': outfilebase = optarg; break;
      case 'c': configure = true; configfile = optarg; break;

      case 'n': clobber = false; break;
      case 'r': yesredis = true; password = optarg; break;

      case 'h': printhelp(); exit(0);
      default:  printhelp(); exit(1);
    }
  }

  if(!infilename)  fprintf(stderr, "Give an input file with -i\n");
  if(!outfilebase) fprintf(stderr, "Give an output base with -o\n");
  if(!configfile)  fprintf(stderr, "Give a configuration file with -c\n");

  if(!infilename || !outfilebase || !configfile){
    printhelp();
    exit(1);
  }

  if(configure)
    ReadConfig(configfile);

}

// This function calculates the time of an event as measured by the
// varlous clocks we are interested in.
static alltimes compute_times(const PmtEventRecord * const hits, alltimes oldat,
                              counts & count, bool & passretrig, bool & retrig)
{
  alltimes newat = oldat;
  if(count.eventn == 1){
    newat.time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
                             + hits->TriggerCardData.Bc50_1;
    newat.time10 = (uint64_t(hits->TriggerCardData.Bc10_2) <<32)
                             + hits->TriggerCardData.Bc10_1;
    if(newat.time50 == 0) count.orphan++;
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
      sprintf(msg, "Stonehenge: The 50MHz clock jumped by %i ticks relative"
                   " to the 10MHz clock!\n", dd);
      alarm(30, msg);
      fprintf(stderr, msg);
    }

    // Check for pathological case
    if (newat.time50 == 0){
      newat.time50 = oldat.time50;
      count.orphan++;
      return newat;
    }

    // Check for time running backward:
    if (newat.time50 < oldat.time50){
      // Is it reasonable that the clock rolled over?
      if ((oldat.time50 + newat.time50 < maxtime + maxjump) && dd < maxdrift && (oldat.time50 > maxtime - maxjump) ) {
        fprintf(stderr, "New Epoch\n");
        alarm(20, "Stonehenge: new epoch.");
        newat.epoch++;
      }
      else{
        const char msg[128] = "Stonehenge: Time running backward!\n";
        alarm(30, msg);
        fprintf(stderr, msg);
        // Assume for now that the clock is wrong
        newat.time50 = oldat.time50;
      }
    }

    // Check that the clock has not jumped ahead too far:
    if (newat.time50 - oldat.time50 > maxjump){
      char msg[128] = "Stonehenge: Large time gap between events!\n";
      alarm(30, msg);
      fprintf(stderr, msg);
      // Assume for now that the time is wrong
      newat.time50 = oldat.time50;
    }

    // Set the Internal Clock
    newat.longtime = newat.time50 + maxtime*newat.epoch;

    // Check for retriggers
    if (newat.time50 - oldat.time50 > 0 &&
        newat.time50 - oldat.time50 <= config.retrigwindow){
      retrig = true;
    }
    else{
      retrig = false;
      passretrig = false;
    }
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

// This Function performs the actual L2 cut
// It returns true if we write out the event and false otherwise
// Keep event if it is over nhit threshold
// or, if it was externally triggered
// or, if it is a retrigger to an accepted event
bool l2filter(const int nhit, const uint32_t word, const bool passretrig, 
              const bool retrig, int &key){
  bool pass = false;
  key = 0;
  if(nhit > NHITCUT){
    pass = true;
    key +=1;
  }
  if((word & config.bitmask) != 0){
    pass = true;
    key +=2;
  }
  if(passretrig && retrig && nhit > config.retrigcut){
    pass = true;
    key +=4;
  }
  return pass;
}

// This function writes the configuration parameters to couchdb.
void WriteConfig(char* infilename){
  CURL* couchcurl = curl_easy_init();
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json"); 
  curl_easy_setopt(couchcurl, CURLOPT_URL, "http://127.0.0.1:5984/l2configuration");
  char configs[1024];
  sprintf(configs, "{\"type\":\"L2CONFIG\", \
                     \"version\":0, \
                     \"run\":\"%s\", \
                     \"pass\":%d, \
                     \"hinhitcut\":%d, \
                     \"lonhitcut\":%d, \
                     \"lowthresh\":%d, \
                     \"lowindow\":%d, \
                     \"retrigcut\":%d, \
                     \"retrigwindow\":%d, \
                     \"prescale\":%d, \
                     \"bitmask\":%d, \
                     \"nhitbcut\":%d, \
                     \"burstwindow\":%d, \
                     \"burstsize\":%d, \
                     \"endrate\":%d, \
                     \"timestamp\":%d}",
                     infilename, 3, config.nhithi, config.nhitlo, config.lothresh, 
                     config.lowindow, config.retrigcut, config.retrigwindow, 
                     config.prescale, config.bitmask, config.nhitbcut,
                     config.burstwindow, config.burstsize, config.endrate,
                     (int)time(NULL)); 
  curl_easy_setopt(couchcurl, CURLOPT_POSTFIELDS, configs);
  curl_easy_setopt(couchcurl, CURLOPT_HTTPHEADER, headers);
  curl_easy_perform(couchcurl);
  curl_easy_cleanup(couchcurl);
  curl_slist_free_all(headers);
  printf("Wrote configuration.\n");
}

// This function zeros out the counters
counts CountInit(){
  counts count;
  count.eventn = 0;
  count.recordn = 0;
  count.prescalen = 0;
  count.orphan = 0;
  return count;
}

// This function initializes the random number generator
static sfmt_t InitRand(const uint32_t seed){
  sfmt_t randgen;
  sfmt_init_gen_rand(&randgen, seed);
  return randgen;
}

// This function initialzes the time object
static alltimes InitTime(){
  alltimes alltime;
  alltime.walltime = 0;
  alltime.oldwalltime = 0;
  alltime.exptime = 0;
  return alltime;
}

// This function sets the trigger threshold appropriately
// The "Kalpana" solution
static void setthreshold(int nhit, alltimes & alltime){
  if(nhit > config.lothresh){
    alltime.exptime = alltime.time50 + config.lowindow;
    NHITCUT = config.nhitlo;
  }
  if(alltime.time50 < alltime.exptime){
    NHITCUT = config.nhithi;
  }
}

// This function checks unix time to see whether to update the times
static void updatetime(alltimes & alltime){
  if(alltime.walltime!=0)
    alltime.oldwalltime=alltime.walltime;
  alltime.walltime = (int) time(NULL);
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
  WriteConfig(infilename);

  // Start Random number generator for prescale selection
  sfmt_t randgen = InitRand(42); // FIXME - use run number for seed or something
  int prescalerand =  (int) (4294967296/config.prescale);

  // Prepare to record statistics in redis database
  Opencurl(password);
  l2stats stat;
  if(yesredis) 
    Openredis(stat);

  bool extasy = false;

  // Initialize the various clocks
  alltimes alltime = InitTime();

  // Setup initial output file
  PZdabWriter* w1  = Output(outfilebase, clobber);
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
  bool burst = false;

  // Flags for the retriggering logic:
  // passretrig true means that if the next event is a retrigger, we should 
  // apply the special retrigger threshold.
  // retrig true means that this event is a retrigger (defined in the sense
  // 0 < dt < 460 ns ).
  bool passretrig = false;
  bool retrig = false;

  // Loop over ZDAB Records
  counts count = CountInit();
  int stats[8] = {0, 0, 0, 0, 0, 0, 0, 0}, psstats[8] = {0, 0, 0, 0, 0, 0, 0, 0};
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
      // The key variable is used to encode which cuts the event passes
      int key = 0;
      nhit = hits->NPmtHit;
      count.eventn++;
      alltime = compute_times(hits, alltime, count, passretrig, retrig);

      // Write statistics to Redis if necessary
      updatetime(alltime);
      if (alltime.walltime!=alltime.oldwalltime){
        if(yesredis) 
          Writetoredis(stat, alltime.oldwalltime);
      }

      // Should we adjust the trigger threshold?
      setthreshold(nhit, alltime);

      // Burst Detection Here
      // If the current event is over our burst nhit threshold (nhitbcut):
      //   * First update the buffer by dropping events older than burstwindow
      //   * Then add the new event to the buffer
      //   * If we were not in a burst, check whether one has started
      //   * If we were in a burst: write event to file, and check if the burst has ended
      // We also check for EXTASY triggers here to send PCA data to Freija

      uint32_t word = triggertype(hits); 
      if(!extasy){
        if((word & EXTASY ) != 0) 
          extasy = true;
      }
      if(nhit > config.nhitbcut && ((word & config.bitmask) == 0) ){
        UpdateBuf(alltime.longtime, config.burstwindow);
        int reclen = zfile->GetSize(hits);
        AddEvBuf(zrec, alltime.longtime, reclen*sizeof(uint32_t));
        int burstlength = Burstlength();

        // Open a new burst file if a burst starts
        if(!burst){
          if(burstlength>config.burstsize){
            Openburst(b, alltime.longtime, headertypes, outfilebase, header, 
                      clobber);
            burst = true;
          }
        }
        // While in a burst
        if(burst){
          stat.burstbool=true;
          Writeburst(alltime.longtime, b);
          // Check if the burst has ended
          if(burstlength < config.endrate){
            Finishburst(b, alltime.longtime);
            burst=false;
          }
        }

      } // End Burst Loop
      // L2 Filter
      if(l2filter(nhit, word, passretrig, retrig, key)){
        OutZdab(zrec, w1, zfile);
        passretrig = true;
        stat.l2++;
        for(int i=1; i<8; i++){
          if(key==i)
            stats[i]++;
        }
      }
      else
        stats[0]++;

      // Decide whether to put event in prescale file
      uint32_t rand = sfmt_genrand_uint32(&randgen);
      if(rand < prescalerand){ //Select 1% of triggers
        count.prescalen++;
        for(int i=0; i<8; i++){
          if(key==i)
            psstats[i]++;
        }
      }

    } // End Loop for Event Records
    // Write out all non-event records:
    else{
      OutZdab(zrec, w1, zfile);
      stat.l2++;
    }
    count.recordn++;
    stat.l1++;
  } // End of the Event Loop for this subrun file
  if(w1) Close(outfilebase, w1, extasy);
  if(b) Finishburst(b, alltime.longtime); 
  Saveburstbuff();

  if(yesredis)
    Closeredis();
  PrintClosing(outfilebase, count, stats, psstats);
  Closecurl();
  return 0;
}
