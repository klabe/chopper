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

void ResetStatistics();
void Openredis();
void Closeredis();
void Writetoredis(const int time);
