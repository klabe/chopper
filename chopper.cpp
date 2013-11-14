#include "PZdabFile.h"
#include <string>
#include <iostream>

int main(){
    std::cout << "Hi";
    std::string filename = "~/sno.zdab";
    FILE* f = fopen(filename.c_str(), "rb");

    PZdabFile* p = new PZdabFile();
    p->Init(f); 

    int time0 = -1;
    const double chunksize = 1.2; // Chunk Size in Seconds;
    const int ticks = chunksize*50000000;
    const int maxtime = 8796093022208; // IS THIS CORRECT?
    int index = 0;

    while(1){
        nZDAB* data = p->NextRecord();
        PmtEventRecord* hits = reinterpret_cast<PmtEventRecord*>(p->GetBank(data));
        //  PmtEventRecord* hits = PZdabFile::GetPmtRecord(data);
        double time = get50MHzTime(hits);

        if ( time0 == -1 )
            time0 = time;

        // OUTPUT ZDAB EVENT HERE

        if (time0+ticks < maxtime){
            if (time > time0+ticks)
                //START NEW FILE
                //WRITE TO DATABASE
                index++;
        }
        else{
            if (time > time0+ticks - maxtime && time < time0 ){
                // DO YOUR THING
            }
        }

        std::cout << time;
    }
}
