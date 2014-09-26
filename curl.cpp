// Curl connection code
//
// K Labe September 23 2014

#include "curl.h"
#include "curl/curl.h"
#include <cstring>
#include <stdlib.h>

static CURL* curl; // curl connection object
static const int max = 10; // maximum number of curl messages allowed per second
static int n = 0; // number of curl messages in last second
static int overflow = 0; // number of overfow messages
static int oldwalltime = 0;

// This function sends alarms to the monitoring website
void alarm(const int level, const char* msg){
  int walltime = time(NULL);
  if(walltime != oldwalltime){
    if(overflow){
      char mssg[128];
      sprintf(mssg, "ERROR OVERFLOW: %d messages skipped", overflow);
      char curlmsg[256];
      sprintf(curlmsg, "name=L2-client&level=30&message=%s", mssg);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) strlen(curlmsg));
      CURLcode res = curl_easy_perform(curl);
      if(res != CURLE_OK)
        fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
    }
    oldwalltime = walltime;
    n = 0;
    overflow = 0;
  }
  n++;
  if(n > max && level < 40) 
    overflow++;
  else{
    char curlmsg[256];
    sprintf(curlmsg, "name=L2-client&level=%d&message=%s", level, msg);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) strlen(curlmsg));
    CURLcode res = curl_easy_perform(curl);
    if(res != CURLE_OK)
      fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
  }
}

// This function opens a curl connection
void Opencurl(char* password){
  curl = curl_easy_init();
  char address[264];
  sprintf(address, "http://snoplus:%s@snopl.us/monitoring/log", password);
  if(curl){
//  curl_easy_setopt(curl, CURLOPT_URL, address);
    curl_easy_setopt(curl, CURLOPT_URL, "http://cp4.uchicago.edu:50000/monitoring/log");
  }
  else{
    fprintf(stderr, "Could not initialize curl object");
    exit(1);
  }
}

// This function closes a curl connection
void Closecurl(){
  curl_easy_cleanup(curl);
}
