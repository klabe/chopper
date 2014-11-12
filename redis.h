// Hiredis connection header
//
// K Labe, September 23 2014
// K Labe, September 24 2014 - Remove variables to source file
// K Labe, November 10 2014  - Add gtid function

#include <stdint.h>
#include "Record_Info.h"

// This structure holds the data which gets written to the redis server
struct l2stats
{
int l1;
int l2;
bool burstbool;
int orphan;
uint32_t gtid;
uint32_t run;
};

// This function resets the redis statistics and is automatically called by 
// the Writetoredis function.
void ResetStatistics(l2stats & stat);

// This function opens the redis connection.
void Openredis(l2stats & stat);

// This function closes the redis connection.
void Closeredis();

// This function writes the statistics contained in stat to the redis database,
// timestamped with time, and then resets them.
void Writetoredis(l2stats & stat, const int time);

// This function retrieves the current gtid and run for writing to redis
void gtid(l2stats & stat, PmtEventRecord * hits);
