// Curl connection code
//
// K Labe September 23 2014

#include "curl.h"
#include <cstring>
#include <stdlib.h>

void alarm(const int level, const char* msg){
  char curlmsg[256];
  sprintf(curlmsg, "name=L2-client&level=%d&message=%s", level, msg);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, curlmsg);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) strlen(curlmsg));
  CURLcode res = curl_easy_perform(curl);
  if(res != CURLE_OK)
    fprintf(stderr, "Logging failed: %s\n", curl_easy_strerror(res));
}

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

void Closecurl(){
  curl_easy_cleanup(curl);
}
