// Curl connection Header
//
// K Labe, September 23 2014

#include "curl/curl.h"

static CURL* curl; // curl connection object

void Opencurl(char* password);
void Closecurl();
void alarm(const int level, const char* msg);
