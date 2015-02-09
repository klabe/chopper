// Curl connection code
//
// K Labe September 23 2014

#include "curl.h"
#include "curl/curl.h"
#include <cstring>
#include <stdlib.h>
#include <stdint.h>

static CURL* curl; // curl connection object
static const int max[5] = {5, 3, 2, 5, 1}; // maximum number of curl messages allowed per second
static int alarmn[5]   = {0, 0, 0, 0, 0}; // number of curl messages in last second
static int overflow[5] = {0, 0, 0, 0, 0}; // number of overfow messages
static int oldwalltime = 0;
static bool silent = false; // Whether to set alarms
static const int ALARMTYPES = 16; // This should be the number of error alarm types
static uint64_t alarmtimes[ALARMTYPES]; // Array of timestamps of alarms
static const int ERRORRATE= 10; // Seconds between alarms

// This function return alarm_type from tony's log number
alarm_type type(const int level){
  if(level == 20)
    return INFO;
  if(level == 21)
    return SUCCESS;
  if(level == 30)
    return WARNING;
  if(level == 40)
    return ERROR;
  else
    return DEBUG;
}

// This function sends alarms to the monitoring website
void alarm(const int level, const char* msg, const int id){
  if(!silent){
    int walltime = time(NULL);
    if(walltime != oldwalltime)
      Flusherrors();
    alarmn[type(level)]++;
    if(alarmn[type(level)] > max[type(level)]) 
      overflow[type(level)]++;
    else{
      if( (level < 40) | (alarmtimes[id] > walltime - ERRORRATE)){
        char curlmsg[2048];
        sprintf(curlmsg, "name=L2-client&level=%d&message=%s", level, msg);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) strlen(curlmsg));
        CURLcode res = curl_easy_perform(curl);
        if(res != CURLE_OK)
          fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
        alarmtimes[id] = walltime;
      }
      else{
        char curlmsg[2048];
        sprintf(curlmsg, "name=L2-client&level=30&message=%s&notify", msg);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) strlen(curlmsg));
        CURLcode res = curl_easy_perform(curl);
        if(res != CURLE_OK)
          fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
      }
    }
  }
}

// This function flushes the error buffer when necessary
void Flusherrors(){
  int overflowsum = 0;
  for(int i=0; i<5; i++){
    overflowsum += overflow[i];
    overflow[i] = 0;
    alarmn[i] = 0;
  }
  if(overflowsum){
    char mssg[128];
    sprintf(mssg, "ERROR OVERFLOW: %d messages skipped&notify", overflowsum);
    char curlmsg[256];
    sprintf(curlmsg, "name=L2-client&level=30&message=%s", mssg);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) strlen(curlmsg));
    CURLcode res = curl_easy_perform(curl);
    if(res != CURLE_OK)
      fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
  }
  int walltime = time(NULL);
  oldwalltime = walltime;
}

// This function opens a curl connection
void Opencurl(char* password){
  curl = curl_easy_init();
  if(curl){
    curl_easy_setopt(curl, CURLOPT_URL, "http://192.168.80.128/monitoring/log");
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 2);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 1);
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

// This function set the silent variable
void setsilent(const int silentword){
  if( silentword == 0 )
    silent = false;
  else
    silent = true;
}
