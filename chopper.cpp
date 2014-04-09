// The Chopper
// K Labe and M Strait, U Chicago, 2013-2014.

// The general logic here is as follows:
// We open a zdab file and read in events, looking at their time. The 
// events are written out into smaller files of fixed time length.  In 
// addition, we allow for a nonzero overlap interval, in which events
// are written into two files.  When header records are encountered,
// they are saved to a buffer, which is written out at the beginning of
// each new output file.

// Explanation of the various clocks used in this program: Since I'm
// reading ZDABs, I need to track the 50 MHz clock for accuracy, and
// the 10 MHz clock for uniqueness. For a given event, the trigger
// time will be stored in the variables time10 and time50. The chopper
// will start a new file every "chunksize" with a trailing period of
// overlap of size "overlap". Time0 represents the beginning of the
// oldest open chunk, according to the longtime clock. When the chunk is
// closed, Time0 is increased by "increment" to ensure that it increases
// uniformly. "maxtime" tells us when the 50 MHz clock rolls over.
// Longtime is an internal 50 MHz clock that uses the full 64 bits
// available so that it will not roll over during the execution of the
// program (it will last 5000 years). Epoch counts the number of times
// that the real 50 MHz clock has rolled over.

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

static double chunksize = 1.0; // Default Chunk Size in Seconds
static double overlap = 0.1; // Default Overlap Size in Seconds
static char* subrun = "."; // Default output directory

// Whether to write out metadata as macro files for each chunk
static bool macro = true;

// Whether to overwrite existing output
static bool clobber = true;

// Most output files permitted, where zero means unlimited.
static int maxfiles = 0;

// Tells us when the 50MHz clock rolls over
static const uint64_t maxtime = (1UL << 43);

// This function writes macro files needed to correctly interpret the
// chopped files with RAT.  It can be suppressed with the -t flag.
// The inputs have the following meaning:
// The 50MHz clock shows the start of the time interval in which events
// were allowed in.  The 10MHz clock is set to the time of the first event
static void WriteMacro(const int index, const uint64_t time10,
                       const uint64_t time50, const char* base)
{
  char* filename = NULL;
  char* macname = NULL;
  snprintf(filename, 1024, "%s/%s_%i.zdab", subrun, base, index);
  snprintf(macname, 1024, "%s/mac/%i.mac", subrun, index);
  std::ofstream file;
  file.open (macname);
  file << "/PhysicsList/OmitMuonicProcesses true\n";
  file << "/PhysicsList/OmitHadronicProcesses true\n";
  file << "/PhysicsList/OmitCerenkov true\n";
  file << "/PhysicsList/Optical/OmitBoundaryEffects true\n";
  file << "/PhysicsList/OmitHadronicPhysicsList true\n";
  file << "/rat/db/set DETECTOR geo_file \"geo/empty.geo\" \n\n";
  file << "/run/initialize\n";
  file << "/rat/proc calibratePMT\n";
  file << "/rat/proc count\n";
  file << "/rat/proc update 10\n";
  file << "/rat/proc burst\n";
  file << "/rat/proc fBurstTrigName \"Burst\"\n";
  file << "/rat/proc fitter\n";
  file << "/rat/procset method \"quad\"\n";
  file << "/rat/proc filter\n";
  file << "/rat/procset chunk" << chunksize << "\n";
  file << "/rat/procset start" << time50 << "\n";
  file << "/rat/proc monitor\n";
  file << "/rat/procset chunk" << chunksize << "\n";
  file << "/rat/procset time50" << time50 << "\n";
  file << "/rat/procset time10" << time10 << "\n";
  file << "/rat/proc outroot\n";
  file << "/rat/procset filter \"true\"\n";
  file << "/rat/procset file \"" << filename << ".root\"\n\n";
  file << "/rat/inzdab/read " << filename;
  file.close();
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
    if(index==0) SWAP_INT32(bank+3,1);
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

// This function builds a new output file for each chunk and should be
// called each time the index in incremented.  If it can't open 
// the file, it aborts the program, so the return pointer does not
// need to be checked.
static PZdabWriter * Output(const char * const base,
                            const unsigned int index)
{
  const int maxlen = 1024;
  char outfilename[maxlen];
  
  if(snprintf(outfilename, maxlen, "%s_%i.zdab", base, index) >= maxlen){
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
static void Close(const char* const base, const unsigned int index, 
                  PZdabWriter* const w)
{
  w->Close();

  if(macro){
    const int maxlen = 1024;
    char closedfilename[maxlen];

    snprintf(closedfilename, maxlen, "%s_%i.zdab", base, index);
    if(access(closedfilename, F_OK)){
      fprintf(stderr, "%s cannot be found!\n", closedfilename);
      exit(1);
    }
  
    char newname[maxlen];
    snprintf(newname, maxlen, "%s/%s", subrun, closedfilename);
    if(rename(closedfilename, newname)){
      fprintf(stderr, "File %s cannot be moved!\n", closedfilename);
      exit(1);
    }

    char* job;
    snprintf(job, 2048, "rat %s/mac/%i.mac -l rat.%s.%i.log; rm %s; rm %s/mac/%i.mac \n", subrun, index, subrun, index, newname, subrun, index);
    std::ofstream jobqueue;
    jobqueue.open("jobqueue.txt", std::fstream::app);
    jobqueue << job;
    jobqueue.close();
  }

  std::ofstream myfile;
  myfile.open("chopper.run.log", std::fstream::app);
  myfile << index << "\n";
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
  "Adjust physics parameters:\n"
  "  -c [n]: Chunk size in seconds\n"
  "  -l [n]: Overlap size in seconds\n"
  "\n"
  "Misc/debugging options\n"
  "  -t: Do not write out processing macros\n"
  "  -s: Specify the subrun directory\n"
  "  -n: Do not overwrite existing output (default is to do so)\n"
  "  -m [n]: Set maximum number of output files, discarding remainder\n"
  "          of input.  Zero means unlimited.\n"
  "  -h: This help text\n"
  );
}

// This function interprets the command line arguments to the program
static void parse_cmdline(int argc, char ** argv, char * & infilename,
                          char * & outfilebase, uint64_t & ticks,
                          uint64_t & increment)
{
  const char * const opts = "hi:o:tm:c:l:s:n";

  bool done = false;
  
  infilename = outfilebase = NULL;

  while(!done){ 
    const char ch = getopt(argc, argv, opts);
    switch(ch){
      case -1: done = true; break;

      case 'i': infilename = optarg; break;
      case 'o': outfilebase = optarg; break;
      case 'm': maxfiles = getcmdline_l(ch); break;
      case 'c': chunksize = getcmdline_d(ch); break;
      case 'l': overlap = getcmdline_d(ch); break;

      case 't': macro = false; break;
      case 'n': clobber = false; break;
      case 's': subrun = optarg; break;

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

  if(overlap > chunksize){
    fprintf(stderr, "Overlap cannot be bigger than chunksize\n");
    exit(1);
  }

  ticks = uint64_t((chunksize+overlap)*50000000); // 50 MHz clock
  increment = uint64_t(chunksize*50000000);
}

// This function calculates the time of an event as measured by the
// various clocks we are interested in.
static void compute_times(const PmtEventRecord * const hits, 
                          uint64_t & time10, uint64_t & time50,
                          uint64_t & longtime, int & epoch)
{
  // Store the old 50MHz Clock Time for comparison
  const uint64_t oldtime = time50;

  // Get the current 50MHz Clock Time
  // Implementing Part of Method Get50MHzTime() 
  // from PZdabFile.cxx
  time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
    + hits->TriggerCardData.Bc50_1;

  // Check for pathological case
  if (time50 == 0) time50 = oldtime;

  // Check whether clock has rolled over
  if (time50 < oldtime) epoch++;

  // Set the Internal Clock
  longtime = time50 + maxtime*epoch;

  // Now get the 10MHz Clock Time
  // Method taken from zdab_convert.cpp
  time10 = (uint64_t(hits->TriggerCardData.Bc10_2) << 32)
                   + hits->TriggerCardData.Bc10_1;
}

// MAIN FUCTION 
int main(int argc, char *argv[])
{
  char * infilename = NULL, * outfilebase = NULL;

  uint64_t ticks, increment;

  parse_cmdline(argc, argv,
      infilename, outfilebase, ticks, increment);

  FILE* infile = fopen(infilename, "rb");

  PZdabFile* zfile = new PZdabFile();
  if (zfile->Init(infile) < 0){
    fprintf(stderr, "Did not open file\n");
    exit(1);
  }

  // Initialize the various clocks
  uint64_t time0 = 0;
  uint64_t time50 = 0;
  uint64_t time10 = 0;
  uint64_t longtime = 0;
  int epoch = 0;
  int index = 0;

  // Setup initial output file
  PZdabWriter* w1  = Output(outfilebase, index);
  PZdabWriter* w2 = NULL;

  // Set up the Header Buffer
  const int headertypes = 3;
  const uint32_t Headernames[headertypes] = 
    { RHDR_RECORD, TRIG_RECORD, EPED_RECORD };
  char* header[headertypes];
  for(int i = 0; i<headertypes; i++){
    header[i] = (char*) malloc(NWREC);
    memset(header[i],0,NWREC);
  }

  // Loop over ZDAB Records
  uint64_t eventn = 0, recordn = 0;
  while(nZDAB * const zrec = zfile->NextRecord()){

    // Check to fill Header Buffer
    for (int i=0; i<headertypes; i++){
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
    if(const PmtEventRecord * const hits = zfile->GetPmtRecord(zrec)){
      eventn++;
      compute_times(hits, time10, time50, longtime, epoch);
 
      // Set time origin on first event
      if(time0 == 0){
        puts("Initializing time origin"); // Should only print once!
        time0 = longtime;
        // Make initial database entry
        if(macro) WriteMacro(index, time10, time0, outfilebase);
      }
    }

    // Chop
    // Within the unique chunk
    if (longtime < time0 + increment){
      OutZdab(zrec, w1, zfile);
    }
    // Within the overlap interval
    else if (longtime < time0 + ticks){
      if(!w2){
        if(maxfiles > 0 && index+2 >= maxfiles) { eventn--; break; }
        w2 = Output(outfilebase, index+1);
        for(int i=0; i<headertypes; i++){
          OutHeader((GenericRecordHeader*) header[i], w2, i);
        }
        if(macro) WriteMacro(index, time10, time0, outfilebase);
      }
      OutZdab(zrec, w1, zfile);
      OutZdab(zrec, w2, zfile);
    }
    // Past the overlap region
    // First, close old chunk and, if there is an open overlap,
    // promote that file to the current chunk.  If there is no
    // overlap file, open a new chunk.
    else{
      Close(outfilebase, index, w1);
      w1 = NULL;
      index++;
      if(w2){
        w1 = w2;
        w2 = NULL;
      }
      else{
        if(maxfiles > 0 && index+1 >= maxfiles) { eventn--; break; }
        w1 = Output(outfilebase, index);
        for(int i=0; i<headertypes; i++)
          OutHeader((GenericRecordHeader*) header[i], w1, i);
        if(macro) WriteMacro(index, time10, time0, outfilebase);
      }
      time0 += increment;
    // Now check for empty chunks
      int deadsec = 0;
      while(longtime > time0 + ticks + deadsec*increment){
        Close(outfilebase, index, w1);
        w1 = NULL;
        index++;
        if(maxfiles > 0 && index+1 >= maxfiles) {eventn--; break; }
        w1 = Output(outfilebase, index);
        for(int i=0; i<headertypes; i++)
          OutHeader((GenericRecordHeader*) header[i], w1, i);
        if(macro) WriteMacro(index, time10, time0+deadsec*increment, outfilebase);
        deadsec++;
      }
      time0 = time0 + deadsec*increment;
    // Lastly, check whether the event is in an overlap or not
      if (longtime < time0 + increment)
        OutZdab(zrec, w1, zfile);
      else{
        if(maxfiles > 0 && index+2 >= maxfiles) { eventn--; break; }
        w2 = Output(outfilebase, index+1);
        for(int i=0; i<headertypes; i++){
          OutHeader((GenericRecordHeader*) header[i], w2, i);
        }
        if(macro) WriteMacro(index, time10, time0, outfilebase);
        OutZdab(zrec, w1, zfile);
        OutZdab(zrec, w2, zfile);
      }
    }
    recordn++;
  }
  if(w1) Close(outfilebase, index, w1);
  if(w2) Close(outfilebase, index+1, w2);

  printf("Done. %lu record%s, %lu event%s processed\n",
         recordn, recordn==1?"":"s", eventn, eventn==1?"":"s");
  return 0;
}
