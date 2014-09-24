// Struct Header
//
// K Labe, September 24 2014

// This structure holds the variables set by the configuration file and recorded
// to couchdb
struct configuration
{
int nhithi;       // The regular nhit cut for physics events
int nhitlo;       // The special lowered nhit cut for after large events
int lothresh;     // This defines "large events" as used above
int lowindow;     // The time for lowering the cut, in 50 MHz ticks
int retrigcut;    // The nhit cut for retriggered events
int retrigwindow; // The max time between retriggered events, in 50 MHz ticks
int prescale;     // The prescale fraction (eg 100 = "save 1 in 100 events")
uint32_t bitmask; // The external trigger bitmask
int nhitbcut;     // The nhit cut for inclusion in bursts
int burstwindow;  // The integration time for spotting bursts (in secs)
int burstsize;    // The count to exceed to be a burst
int endrate;      // Rate below which burst ends
};

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
};
