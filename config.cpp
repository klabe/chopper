// Configuration Reader code
//
// K Labe - September 25 2014

#include "struct.h"
#include <stdlib.h>
#include <fstream>
#include <string.h>

// Number of parameters held by the configuration object
static const int paramn = 11;

// This object keeps track of which configuration parameters have been set
static bool state[paramn];

// This function checks for repeated parameters (in which case, abort), or
// otherwise updates the state.
void bit(int num){
  if(num >= paramn){
    printf("bit:  You should never see this.\n");
    exit(1);
  }
  if(state[num] == 1){
    printf("Tried to set a parameter twice!\n");
    exit(1);
  }
  state[num] = 1;
}

// This fuction checks the state object to see whether all the bools are true
bool allset(){
  for(int i=0; i<paramn; i++){
    if(!state[i])
      return false;
  }
  return true;
}

// This function sets the actual cuts used by the program
// Contains the logic to decide on cuts based on run type
void SetConfig(int runtype, configuration allconfigs[2], configuration & config){
  // run type logic
  int configno = 0;
  if(runtype != 1)
    configno = 1;
  // default
  if(runtype == 0)
    configno = 0;

  // setting of parameters
  config.nhithi       = allconfigs[configno].nhithi;
  config.nhitlo       = allconfigs[configno].nhitlo;
  config.lothresh     = allconfigs[configno].lothresh;
  config.lowindow     = allconfigs[configno].lowindow;
  config.retrigcut    = allconfigs[configno].retrigcut;
  config.retrigwindow = allconfigs[configno].retrigwindow; 
  config.nhitbcut     = allconfigs[configno].nhitbcut;
  config.burstwindow  = allconfigs[configno].burstwindow;
  config.burstsize    = allconfigs[configno].burstsize;
  config.endrate      = allconfigs[configno].endrate;
  config.bitmask      = allconfigs[configno].bitmask;
}

// This function reads the configuration file and writes the results in the
// allconfigs object
void ReadConfig(const char* filename, configuration allconfigs[2]){
   FILE* configfile = fopen(filename, "r");
   if(configfile == NULL){
     printf("Could not open configuration file.\n");
     exit(1);
   }

   char param[16];
   int value[2];

   // Read file and check that each parameter set exactly once
   while(fscanf(configfile, "%s %d %d\n", param, &value[0], &value[1])==3){
     for(int i=0; i<2; i++){
       if     (!strcmp(param, "nhithi")      )
         {allconfigs[i].nhithi       = value[i]; bit(0);}
       else if(!strcmp(param, "nhitlo")      )
         {allconfigs[i].nhitlo       = value[i]; bit(1);}
       else if(!strcmp(param, "lothresh")    )
         {allconfigs[i].lothresh     = value[i]; bit(2);}
       else if(!strcmp(param, "lowindow")    )
         {allconfigs[i].lowindow     = value[i]; bit(3);}
       else if(!strcmp(param, "nhitretrig")  )
         {allconfigs[i].retrigcut    = value[i]; bit(4);}
       else if(!strcmp(param, "retrigwindow"))
         {allconfigs[i].retrigwindow = value[i]; bit(5);}
       else if(!strcmp(param, "nhitburst")   )
         {allconfigs[i].nhitbcut     = value[i]; bit(6);}
       else if(!strcmp(param, "burstwindow") )
         {allconfigs[i].burstwindow  = value[i]; bit(7);}
       else if(!strcmp(param, "burstsize")   )
         {allconfigs[i].burstsize    = value[i]; bit(8);}
       else if(!strcmp(param, "endrate")     )
         {allconfigs[i].endrate      = value[i]; bit(9);}
       else if(!strcmp(param, "bitmask")     ){;} // Do nothing
       else{
          printf("ReadConfig does not recognize parameter %s.  Ignoring.\n",
                 param);
          exit(1);
       }
     }
   }
   rewind(configfile);
   while(fscanf(configfile, "%s %x %x\n", param, &value[0], &value[1])==3){
     if(strcmp(param, "bitmask") == 0){
       allconfigs[0].bitmask = value[0]; 
       allconfigs[1].bitmask = value[1];
       bit(10);
     }
   }

   // Check whether all bits were set
   if(!allset()){
     printf("The configuration file did not set all the parameters!\n");
     exit(1);
   }
}
