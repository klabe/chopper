// Hiredis connection header
//
// K Labe, September 23 2014
// K Labe, September 24 2014 - Remove variables to source file

struct l2stats
{
int l1;
int l2;
bool burstbool;
};

void ResetStatistics(l2stats stat);
void Openredis(l2stats stat);
void Closeredis();
void Writetoredis(l2stats stat, const int time);
