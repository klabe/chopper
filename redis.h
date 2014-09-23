// Hiredis connection header
//
// K Labe, September 23 2014

#include "hiredis.h"

struct l2stats
{
int l1;
int l2;
bool burstbool;
};

static bool yesredis = false; // Write to redis database?
static redisContext* redis = NULL; // hiredis connection object
static l2stats stat;

void ResetStatistics();
void Openredis();
void Closeredis();
void Writetoredis(const int time);
