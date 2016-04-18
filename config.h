// Configuration Reader Header
//
// K Labe, September 25 2014
// K Labe, April 18 2016 - Added the SetConfig function

void bit(int num);
bool allset();
void SetConfig(int runtype, configuration allconfigs[2], configuration & config);
void ReadConfig(const char* filename, configuration & config[2]);
