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
#include "curl/curl.h"
#include "struct.h"
#include "snbuf.h"
#include "output.h"
#include "config.h"

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
      alarm(40, message);
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
    char buff[128];
    sprintf(buff, "Stonehenge input %s (given with -%c) isn't a number I"
                  " can handle\n", optarg, opt);
    fprintf(stderr, buff);
    alarm(40, buff);
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
    char buff[128];
    sprintf(buff, "Stonehenge input %s (given with -%c) isn't a number I"
                  " can handle.\n", optarg, opt);
    fprintf(stderr, buff);
    alarm(40, buff);
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
static void PrintClosing(char* outfilebase, counts count, int stats[]){
  char messg[2048];
  sprintf(messg, "Stonehenge: Subfile %s finished."
                 "  %lu records,  %lu events processed.\n"
                 "%i events pass no cut\n"
                 "%i events pass only nhit cut\n"
                 "%i events pass only external trigger cut\n"
                 "%i events pass both external trigger and nhit cuts\n"
                 "%i events pass only retrigger cut\n"
                 "%i events pass both retrigger cut and nhit cut\n"
                 "%i events pass both retrigger cut and nhit cut\n"
                 "%i events pass all three cuts\n",
         outfilebase, count.recordn, count.eventn,
         stats[0], stats[1], stats[2],
         stats[3], stats[4], stats[5], stats[6], stats[7]);

  alarm(21, messg);
  fprintf(stderr, messg);
}

// This function interprets the command line arguments to the program
static void parse_cmdline(int argc, char ** argv, char * & infilename,
                          char * & outfilebase)
{
  char* configfile = NULL;
  const char * const opts = "hi:o:l:b:t:u:c:nr";

  bool done = false;
  
  infilename = outfilebase = NULL;

  while(!done){ 
    const char ch = getopt(argc, argv, opts);
    switch(ch){
      case -1: done = true; break;

      case 'i': infilename = optarg; break;
      case 'o': outfilebase = optarg; break;
      case 'c': configfile = optarg; break;

      case 'n': clobber = false; break;
      case 'r': yesredis = true; password = optarg; break;

      case 'h': printhelp(); exit(0);
      default:  printhelp(); exit(1);
    }
  }

  if(!infilename){
    char buff[128];
    sprintf(buff, "Stonehenge: Must give an input file with -i.  Aborting.\n");
    fprintf(stderr, buff);
    alarm(40, buff);
  }
  if(!outfilebase){
    char buff[128];
    sprintf(buff, "Stonehenge: Must give an output base with -o.  Aborting.\n");
    fprintf(stderr, buff);
    alarm(40, buff);
  }
  if(!configfile){
    char buff[128];
    sprintf(buff, "Stonehenge: Must give a configuration file with -c.  Aborting.\n");
    fprintf(stderr, buff);
    alarm(40, buff);
  }

  if(!infilename || !outfilebase || !configfile){
    printhelp();
    exit(1);
  }

  ReadConfig(configfile, config);

}

// This function checks the clocks for various anomalies and raises alarms.
// It returns true if the event passes the tests, false otherwise
bool IsConsistent(alltimes & newat, alltimes standard, const int dd){
  // Check for time running backward:
  if(newat.time50 < standard.time50){
    // Is it reasonable that the clock rolled over?
    if((standard.time50 + newat.time50 < maxtime + maxjump) &&
        dd < maxdrift && (standard.time50 > maxtime - maxjump) ){
      fprintf(stderr, "New Epoch\n");
      alarm(20, "Stonehenge: new epoch.");
      newat.epoch++;
    }
    else{
      const char msg[128] = "Stonehenge: Time running backward!\n";
      alarm(30, msg);
      fprintf(stderr, msg);
      return false;
    }  
  }
  // Check that time has not jumped too far ahead
  if(newat.time50 - standard.time50 > maxjump){
    char msg[128] = "Stonehenge: Large time gap between events!\n";
    alarm(30, msg);
    fprintf(stderr, msg);
    return false;
  }
  else
    return true;
}

// This function calculates the time of an event as measured by the
// varlous clocks we are interested in.
static alltimes compute_times(const PmtEventRecord * const hits, alltimes oldat,
                              counts & count, bool & passretrig, bool & retrig,
                              l2stats & stat, PZdabWriter* & b)
{
  static alltimes standard; // Previous unproblematic timestamp
  static bool problem;      // Was there a problem with previous timestamp?
  alltimes newat = oldat;
  // For first event
  if(count.eventn == 1){
    newat.time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
                             + hits->TriggerCardData.Bc50_1;
    newat.time10 = (uint64_t(hits->TriggerCardData.Bc10_2) <<32)
                             + hits->TriggerCardData.Bc10_1;
    if(newat.time50 == 0) stat.orphan++;
    newat.longtime = newat.time50;
    standard = newat;
    problem = false;
    Checkbuffer(newat.time50);
  }
  // Otherwise
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

    // Check for retriggers
    if (newat.time50 - oldat.time50 > 0 &&
        newat.time50 - oldat.time50 <= config.retrigwindow){
      retrig = true;
    }
    else{
      retrig = false;
      passretrig = false;
    }

    // Check for pathological case
    if (newat.time50 == 0){
      newat.time50 = oldat.time50;
      stat.orphan++;
      return newat;
    }

    // Check for well-orderedness
    if(IsConsistent(newat, standard, dd)){
      newat.longtime = newat.time50 + maxtime*newat.epoch;
      standard = newat;
      problem = false;
    }
    else if(problem){
      // RESET EVERYTHING
      alarm(40, "Stonehenge: Events out of order - Resetting buffers.");
      ClearBuffer(b, standard.longtime);
      NHITCUT = config.nhithi;
      newat.epoch = 0;
      newat.longtime = newat.time50;
      newat.exptime = 0; 
      standard = newat;
      problem = false;
    }
    else{
      problem = true;
      newat = standard;
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
              const bool retrig, int stats[]){
  bool pass = false;
  int key = 0;
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
  for(int i=0; i<8; i++){
    if(key == i)
      stats[i]++;
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
                     \"bitmask\":%d, \
                     \"nhitbcut\":%d, \
                     \"burstwindow\":%d, \
                     \"burstsize\":%d, \
                     \"endrate\":%d, \
                     \"timestamp\":%d}",
                     infilename, 3, config.nhithi, config.nhitlo, config.lothresh, 
                     config.lowindow, config.retrigcut, config.retrigwindow, 
                     config.bitmask, config.nhitbcut, config.burstwindow, 
                     config.burstsize, config.endrate, (int)time(NULL)); 
  curl_easy_setopt(couchcurl, CURLOPT_POSTFIELDS, configs);
  curl_easy_setopt(couchcurl, CURLOPT_HTTPHEADER, headers);
  CURLcode res = curl_easy_perform(couchcurl);
  if(res != CURLE_OK){
    alarm(30, "Could not log parameters to CouchDB!  Logging here instead.\n");
    alarm(30, configs);
  }
  curl_easy_cleanup(couchcurl);
  curl_slist_free_all(headers);
  printf("Wrote configuration.\n");
  fprintf(stdout, configs);
}

// This function zeros out the counters
counts CountInit(){
  counts count;
  count.eventn = 0;
  count.recordn = 0;
  return count;
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
    alltime.exptime = alltime.longtime + config.lowindow;
    NHITCUT = config.nhitlo;
  }
  if(alltime.longtime > alltime.exptime){
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
  // Connect to minard for monitoring
  Opencurl(password);

  // Configure the system
  char * infilename = NULL, * outfilebase = NULL;

  parse_cmdline(argc, argv, infilename, outfilebase);

  FILE* infile = fopen(infilename, "rb");

  PZdabFile* zfile = new PZdabFile();
  if (zfile->Init(infile) < 0){
    fprintf(stderr, "Did not open file\n");
    alarm(40, "Stonehenge could not open input file.  Aborting.");
    exit(1);
  }
  WriteConfig(infilename);

  // Prepare to record statistics in redis database
  l2stats stat;
  if(yesredis) 
    Openredis(stat);

  bool extasy = false;

  // Initialize the various clocks
  alltimes alltime = InitTime();

  // Setup initial output file
  PZdabWriter* w1  = Output(outfilebase, clobber);
  PZdabWriter* b = NULL; // Burst event file

  // Set up the Burst Buffer
  InitializeBuf();

  // Flags for the retriggering logic:
  // passretrig true means that if the next event is a retrigger, we should 
  // apply the special retrigger threshold.
  // retrig true means that this event is a retrigger (defined in the sense
  // 0 < dt < 460 ns ).
  bool passretrig = false;
  bool retrig = false;

  // Loop over ZDAB Records
  counts count = CountInit();
  int stats[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  int nhit = 0;
  while(nZDAB * const zrec = zfile->NextRecord()){
    // Fill Header buffer if necessary
    FillHeaderBuffer(zrec);

    // If the record has an associated time, compute all the time
    // variables.  Non-hit records don't have times.
    if(PmtEventRecord * hits = zfile->GetPmtRecord(zrec)){
      nhit = hits->NPmtHit;
      count.eventn++;
      alltime = compute_times(hits, alltime, count, passretrig, retrig, stat, b);

      // Write statistics to Redis if necessary
      updatetime(alltime);
      if (alltime.walltime!=alltime.oldwalltime){
        if(yesredis) 
          Writetoredis(stat, alltime.oldwalltime);
        Flusherrors();
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
        AddEvBuf(zrec, alltime.longtime, reclen*sizeof(uint32_t), b);

        // Write to burst file if necessary
        // A comment here about the following bit of opaque code:
        // Burstfile returns whether a burst is ongoing, but we want burstbool
        // to remain true after the burst ends, until it is reset.  We therefore
        // logical-OR the return value of Burstfile with the existing value of 
        // stat.burstbool.
        stat.burstbool = (stat.burstbool | Burstfile(b, config, alltime,
                          outfilebase, clobber) );

      } // End Burst Loop
      // L2 Filter
      if(l2filter(nhit, word, passretrig, retrig, stats)){
        OutZdab(zrec, w1, zfile);
        passretrig = true;
        stat.l2++;
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
  BurstEndofFile(b, alltime.longtime);

  Flusherrors();
  if(yesredis)
    Closeredis();
  PrintClosing(outfilebase, count, stats);
  Closecurl();
  return 0;
}
