#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <string>
#include <iostream>

int main(){
    std::string infilename = "/home/cp/klabe/sno.zdab";
    FILE* infile = fopen(infilename.c_str(), "rb");

    PZdabFile* p = new PZdabFile();
    if (p->Init(infile) < 0){
        std::cerr << "Did not open file" << std::endl;
        return -1;
    }

    double time0 = -1;
    const double chunksize = 1.2; // Chunk Size in Seconds;
    const double ticks = chunksize*50000000;
    const int maxtime = 8796093022208; // IS THIS CORRECT?
    double time = 0;
    int index = 0;

    //char *outfilename = "/home/cp/klabe/chopped.zdab";
    //PZdabWriter w = PZdabWriter(outfilename,0);

    while(1){
        nZDAB* data = p->NextRecord();
            if (data == NULL)
                break;
        PmtEventRecord* hits = p->GetPmtRecord(data);
        if (hits != NULL){
            time = get50MHzTime(hits);
            if (time0 == -1)
                time0 = time;
        }
        // OUTPUT ZDAB RECORD HERE
        if (time0 + ticks < maxtime){
            if (time > time0 + ticks){
                // START NEW FILE, CLOSE PRESENT FILE
                // WRITE TO DATABASE
                index++;
                time0 = time;
            }
        }
        else{
            if (time > time0 + ticks - maxtime && time < time0 ){
                // START NEW FILE, CLOSE PRESENT FILE
                // WRITE TO DATABASE
                index ++;
                time0 = time;
            }
        }
        
        std::cerr << time << std::endl;
    }
    return 0;
}
