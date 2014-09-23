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
#include "hiredis.h"
#include "curl.h"
#include "snbuf.h"
#include "SFMT.h"

#define EXTASY 0x8000 // Bit 15

// These are the parameters which are set in the configuration file.
// If there a value is not specified in the configuration file, the program
// will use the default value listed here 
//***************************************************************************
// This is the regular nhit cut for physics events
static int HINHITCUT = 30; 

// This is the special lowered nhit cut for after large events.
static int LONHITCUT = 10;

// This defines "large events" as used above.
static int LOWTHRESH = 50;

// This is the time for lowering the cut, in 50 MHz ticks.
static int LOWINDOW = 20000;

// This is the nhit cut for retriggered events.
static int RETRIGCUT = 5;

// This is the max time between retriggered events, in 50 MHz ticks (23 = 460ns).
static int RETRIGWINDOW = 23;

// This is the prescale fraction (eg 100 = "save 1 in 100 events")
static int PRESCALE = 100;

// This is the external trigger bitmask; currently masking in bits 10, 12-22
static uint32_t bitmask = 0x00FFF800;
//***************************************************************************

// This variable holds the current Nhitcut, which can be either the Hi or 
// Lo Nhitcut, depending on what has been going on
static int NHITCUT;

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

static char* password = NULL;

// Structure to hold all the relevant times
struct alltimes
{
uint64_t time10;
uint64_t time50;
uint64_t longtime;
int epoch;
int walltime;
int oldwalltime;
uint64_t exptime;
};

// Structure to hold all the things we count
struct counts
{
uint64_t prescalen;
uint64_t eventn;
uint64_t recordn;
int orphan;
int l1;
int l2;
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

// This function writes out the ZDAB record
static void OutZdab(nZDAB * const data, PZdabWriter * const zwrite,
                    PZdabFile * const zfile)
{
  if(!data) return;
  const int index = PZdabWriter::GetIndex(data->bank_name);
  if(index < 0){
     fprintf(stderr, "Unrecognized bank name\n");
     alarm(10, "Outzdab: unrecognized bank name.");
  }
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
      default: fprintf(stderr, "Not reached\n"); alarm(10, "Outheader: You never see this!"); exit(1);
    }
  }
  if(w->WriteBank((uint32_t *)hdr, index)){
    fprintf(stderr,"Error writing to zdab file\n");
    alarm(10, "Outheader: error writing to zdab file.");
  }
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
    alarm(10, "Output: output filename truncated");
  }

  if(!access(outfilename, W_OK)){
    if(!clobber){
      fprintf(stderr, "%s already exists and you told me not to "
              "overwrite it!\n", outfilename);
      alarm(10, "Output: Should not overwrite that file.");
      exit(1);
    }
    unlink(outfilename);
  }
  else if(!access(outfilename, F_OK)){
    fprintf(stderr, "%s already exists and we can't overwrite it!\n",
            outfilename);
    alarm(10, "Output: Cannot overwrite that file.");
    exit(1);
  } 

  PZdabWriter * const ret = new PZdabWriter(outfilename, 0);

  if(!ret || !ret->IsOpen()){
    fprintf(stderr, "Could not open output file %s\n", outfilename);
    alarm(10, "Output: Cannot open file.");
    exit(1);
  }
  return ret;
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
  "\n"
  "Physics options:\n"
  "  -l [n]: L2 nhit cut (default %d)\n"
  "  -b [n]: Burst file nhit cut (default %d)\n"
  "  -t [n]: Burst window width in seconds (default %d) \n"
  "  -u [n]: Burst size threshold event count (default %d) \n"
  "\n"
  "Misc/debugging options\n"
  "  -c [string]: Configuration file\n"
  "  -n: Do not overwrite existing output (default is to do so)\n"
  "  -r: Write statistics to the redis database.\n"
  "  -h: This help text\n"
  , NHITCUT, NHITBCUT, BurstLength, BurstSize
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

// This function opens the redis connection at startup
static redisContext* Openredis()
{
  redisContext *redis = redisConnect("cp4.uchicago.edu", 6379);
  if((redis)->err){
    printf("Error: %s\n", (redis)->errstr);
    alarm(10, "Openredis: cannot connect to redis server.");
    return NULL;
  }
  else{
    printf("Connected to Redis.\n");
    alarm(21, "Openredis: connected to server!");
  }
  return redis;
}

// This function closes the redis connection when finished
static void Closeredis(redisContext **redis)
{
  redisFree(*redis);
}

// This function writes statistics to redis database
static void Writetoredis(redisContext *redis, const counts & count,
                         const int time)
{
  if(!redis){
    alarm(30, "Cannot connect to redis.");
    return;
  }
  const char* message = "Writetoredis failed.";
  const int NumInt = 17;
  const int intervals[NumInt] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
  for(int i=0; i < NumInt; i++){
    int ts = time/intervals[i];
    void* reply = redisCommand(redis, "INCRBY ts:%d:%d:L1 %d", intervals[i], ts, count.l1);
    if(!reply)
      alarm(30, message);
    reply = redisCommand(redis, "EXPIRE ts:%d:%d:L1 %d", intervals[i], ts, 2400*intervals[i]);
    if(!reply)
      alarm(30, message);

    reply = redisCommand(redis, "INCRBY ts:%d:%d:L2 %d", intervals[i], ts, count.l2);
    if(!reply) 
      alarm(30, message);
    reply = redisCommand(redis, "EXPIRE ts:%d:%d:L2 %d", intervals[i], ts, 2400*intervals[i]);
    if(!reply)
      alarm(30, message);

    if(burstbool){
      reply = redisCommand(redis, "SET ts:%d:id:%d:Burst 1", intervals[i], ts);
      if(!reply)
        alarm(30, message);
      reply = redisCommand(redis, "EXPIRE ts:%d:id:%d:Burst", intervals[i], ts, 2400*intervals[i]);
      if(!reply)
        alarm(30, message);
    }
  }
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
     if     (strcmp(param,"nhithi")        == 0) HINHITCUT = value;
     else if(strcmp(param,"nhitlo")        == 0) LONHITCUT = value;
     else if(strcmp(param, "lothresh")     == 0) LOWTHRESH = value;
     else if(strcmp(param, "lowindow")     == 0) LOWINDOW = value;
     else if(strcmp(param, "nhitretrig")   == 0) RETRIGCUT = value;
     else if(strcmp(param, "retrigwindow") == 0) RETRIGWINDOW = value;
     else if(strcmp(param, "prescale")     == 0) PRESCALE = value;
     else if(strcmp(param, "bitmask")      == 0) bitmask = value;
     else if(strcmp(param, "nhitbcut")     == 0) setburstcut(value);
     else if(strcmp(param, "burstwindow")  == 0) settimecut(value);
     else if(strcmp(param, "burstsize")    == 0) setratecut(value);
     else
        printf("ReadConfig does not recognize parameter %s.  Ignoring.\n",
               param);
   }
}

// This function interprets the command line arguments to the program
static void parse_cmdline(int argc, char ** argv, char * & infilename,
                          char * & outfilebase)
{
  char* configfile = NULL;
  const char * const opts = "hi:o:l:b:t:u:c:nr";

  bool done = false;
  bool config = false; // was there a configuration file provided?
  
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

      case 'c': config = true; configfile = optarg; break;

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

  if(config)
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
        newat.time50 - oldat.time50 <= RETRIGWINDOW){
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
  if((word & bitmask) != 0){
    pass = true;
    key +=2;
  }
  if(passretrig && retrig && nhit > RETRIGCUT){
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
                     infilename, 3, HINHITCUT, LONHITCUT, LOWTHRESH, LOWINDOW,
                     RETRIGCUT, RETRIGWINDOW, PRESCALE, bitmask, NHITBCUT,
                     BurstLength, BurstSize, EndRate, (int)time(NULL)); 
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
  count.l1 = 0;
  count.l2 = 0;
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
  if(nhit > LOWTHRESH){
    alltime.exptime = alltime.time50 + LOWINDOW;
    NHITCUT = LONHITCUT;
  }
  if(alltime.time50 < alltime.exptime){
    NHITCUT = HINHITCUT;
  }
}

// This function checks unix time to see whether to update the times
static void updatetime(alltimes & alltime){
  if(alltime.walltime!=0)
    alltime.oldwalltime=alltime.walltime;
  alltime.walltime = (int) time(NULL);
}

// This function resets the redis statistics
static void ResetStatistics(counts & count){
  count.l1 = 0;
  count.l2 = 0;
  burstbool = false;
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
  int prescalerand =  (int) (4294967296/PRESCALE);

  // Prepare to record statistics in redis database
  redisContext* redis = NULL;
  if(yesredis) 
    redis = Openredis();

  bool extasy = false;

  // Initialize the various clocks
  alltimes alltime = InitTime();

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
          Writetoredis(redis, count, alltime.oldwalltime);
        ResetStatistics(count);
      }

      // Should we adjust the trigger threshold?
      setthreshold(nhit, alltime);

      // Burst Detection Here
      // If the current event is over our burst nhit threshold (NHITBCUT):
      //   * First update the buffer by dropping events older than BurstLength
      //   * Then add the new event to the buffer
      //   * If we were not in a burst, check whether one has started
      //   * If we were in a burst: write event to file, and check if the burst has ended
      // We also check for EXTASY triggers here to send PCA data to Freija

      uint32_t word = triggertype(hits); 
      if(!extasy){
        if((word & EXTASY ) != 0) 
          extasy = true;
      }
      if(nhit > NHITBCUT && ((word & bitmask) == 0) ){
        UpdateBuf(alltime.longtime);
        int reclen = zfile->GetSize(hits);
        AddEvBuf(zrec, alltime.longtime, reclen*sizeof(uint32_t));
        int burstlength = Burstlength();

        int starttick = 0;

        // Open a new burst file if a burst starts
        if(!burst){
          if(burstlength>BurstSize){
            burst = true;
            bcount = burstlength;
            starttick = alltime.longtime;
            fprintf(stderr, "Burst %i has begun!\n", burstindex);
            alarm(20, "Burst started");
            char buff[32];
            sprintf(buff, "Burst_%s_%i", outfilebase, burstindex);
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
            alarm(20, "Burst ended");
            burstindex++;
            // Reset to prepare for next burst
            bcount=0;
          }
        }

      } // End Burst Loop
      // L2 Filter
      if(l2filter(nhit, word, passretrig, retrig, key)){
        OutZdab(zrec, w1, zfile);
        passretrig = true;
        count.l2++;
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
      count.l2++;
    }
    count.recordn++;
    count.l1++;
  } // End of the Event Loop for this subrun file
  if(w1) Close(outfilebase, w1, extasy);

  if(yesredis)
    Closeredis(&redis);
  PrintClosing(outfilebase, count, stats, psstats);
  Closecurl();
  return 0;
}
