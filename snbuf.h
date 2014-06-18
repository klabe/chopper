// Supernova Buffer Header
// 
// K Labe, June 17 2014

static const int EVENTNUM = 1000; // Maximum Burst buffer depth
static const int NHITBCUT = 40; // Nhit cut on burst events
static const int BurstLength = 10; // Burst length in seconds
static const int BurstTicks = BurstLength*500000000;// length in ticks
static const int BurstSize = 30; // Number of events constituting a burst
static bool burst = false; // Flags ongoing bursts
static int burstindex = 0; // Number of bursts observed
static const int ENDWINDOW = 1*500000000; // Integration window for determining whether burst has ended
static const int EndRate = 10; // Rate below which burst ends
static char* burstev[EVENTNUM]; // Burst Event Buffer
static uint64_t bursttime[EVENTNUM]; // Burst Time Buffer

void InitializeBuf();
void UpdateBuf(uint64_t longtime, int & bursthead, int & bursttail);
void AddEvBFile(int & bursthead, PZdabWriter* const b);
void AddEvBuf(const nZDAB* const zrec, const uint64_t longtime,
              int & bursthead, int & bursttail, const int reclen);
