// Hiredis connector code
//
// K Labe September 23 2014

#include "hiredis.h"
#include "redis.h"
#include "curl.h"

static redisContext* redis = NULL; // hiredis connection object

// This function resets the redis statistics
void ResetStatistics(l2stats stat){
  stat.l1 = 0;
  stat.l2 = 0;
  stat.burstbool = false;
}

// This function opens the redis connections
void Openredis(l2stats stat){
  redis = redisConnect("cp4.uchicago.edu", 6379);
  if((redis)->err){
    printf("Error: %s\n", (redis)->errstr);
    alarm(10, "Openredis: cannot connect to redis server.");
    redis = NULL;
  }
  else{
    printf("Connected to Redis.\n");
    alarm(21, "Openredis: connected to server!");
  }
  ResetStatistics(stat);
}

// This function closes the redis connection
void Closeredis(){
  redisFree(redis);
}

// This function writes statistics to redis database
void Writetoredis(l2stats stat, const int time){
  if(!redis){
    alarm(30, "Cannot connect to redis.");
    return;
  }
  const char* message = "Writetoredis failed.";
  const int NumInt = 17;
  const int intervals[NumInt] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
  for(int i=0; i < NumInt; i++){
    int ts = time/intervals[i];
    void* reply = redisCommand(redis, "INCRBY ts:%d:%d:L1 %d", intervals[i], ts, stat.l1);
    if(!reply)
      alarm(30, message);
    reply = redisCommand(redis, "EXPIRE ts:%d:%d:L1 %d", intervals[i], ts, 2400*intervals[i]);
    if(!reply)
      alarm(30, message);

    reply = redisCommand(redis, "INCRBY ts:%d:%d:L2 %d", intervals[i], ts, stat.l2);
    if(!reply)
      alarm(30, message);
    reply = redisCommand(redis, "EXPIRE ts:%d:%d:L2 %d", intervals[i], ts, 2400*intervals[i]);
    if(!reply)
      alarm(30, message);

    if(stat.burstbool){
      reply = redisCommand(redis, "SET ts:%d:id:%d:Burst 1", intervals[i], ts);
      if(!reply)
        alarm(30, message);
      reply = redisCommand(redis, "EXPIRE ts:%d:id:%d:Burst", intervals[i], ts, 2400*intervals[i]);
      if(!reply)
        alarm(30, message);
    }
  }
  ResetStatistics(stat);
}
