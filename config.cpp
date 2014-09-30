// Configuration Reader code
//
// K Labe - September 25 2014

#include "struct.h"
#include <stdlib.h>
#include <fstream>

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

// This function reads the configuration file and sets the cut parameters.
void ReadConfig(const char* filename, configuration & config){
   FILE* configfile = fopen(filename, "r");
   if(configfile == NULL){
     printf("Could not open configuration file.\n");
     exit(1);
   }

   char param[16];
   int value;

   // Read file and check that each parameter set exactly once
   while(fscanf(configfile, "%s %d\n", param, &value)==2){
     if     (!strcmp(param, "nhithi")      ){config.nhithi       = value; bit(0);}
     else if(!strcmp(param, "nhitlo")      ){config.nhitlo       = value; bit(1);}
     else if(!strcmp(param, "lothresh")    ){config.lothresh     = value; bit(2);}
     else if(!strcmp(param, "lowindow")    ){config.lowindow     = value; bit(3);}
     else if(!strcmp(param, "nhitretrig")  ){config.retrigcut    = value; bit(4);}
     else if(!strcmp(param, "retrigwindow")){config.retrigwindow = value; bit(5);}
     else if(!strcmp(param, "nhitburst")   ){config.nhitbcut     = value; bit(6);}
     else if(!strcmp(param, "burstwindow") ){config.burstwindow  = value; bit(7);}
     else if(!strcmp(param, "burstsize")   ){config.burstsize    = value; bit(8);}
     else if(!strcmp(param, "endrate")     ){config.endrate      = value; bit(9);}
     else if(!strcmp(param, "bitmask")     ){;} // Do nothing
     else{
        printf("ReadConfig does not recognize parameter %s.  Ignoring.\n",
               param);
        exit(1);
     }
   }
   rewind(configfile);
   while(fscanf(configfile, "%s %x\n", param, &value)==2){
     if(strcmp(param, "bitmask") == 0){ config.bitmask = value; bit(10);}
   }

   // Check whether all bits were set
   if(!allset()){
     printf("The configuration file did not set all the parameters!\n");
     exit(1);
   }
}
