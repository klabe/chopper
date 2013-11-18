#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <string>
#include <iostream>
#include <mysql.h>
#include <sstream>
#include <stdint.h>

void Database(int, uint64_t, uint64_t);
void OutZdab(nZDAB*, PZdabWriter*, PZdabFile*);
int GetLastIndex();
PZdabWriter* Output(int);

// The general logic here is as follows:
// We open a zdab file and read in events, looking at their time. The 
// events are written out into smaller files of fixed time length.  In 
// addition, we allow for a nonzero overlap interval, in which events
// are written into two files.  When header records are encountered,
// they are saved to a buffer, which is written out at the beginning of
// each new output file.
int main(int argc, char *argv[]){
    // Get Input File
    if(argc != 2){
        std::cerr << "Enter filename" << std::endl;
        return -1;
    }
    char* infilename = argv[1];
    FILE* infile = fopen(infilename, "rb");

    PZdabFile* p = new PZdabFile();
    if (p->Init(infile) < 0){
        std::cerr << "Did not open file" << std::endl;
        return -1;
    }

    // Initialize time information.  I should explain what's going on
    // here because there are so many times.  Since I'm reading ZDABs
    // I need to track the 50 MHz clock for accuracy, and the 10 MHz
    // clock for uniqueness.  For a given event, the trigger time will
    // be stored in the variables time10 and time50.  The chopper is
    // designed to output files with overlapping time intervals to aid
    // in the search of correlated events near edges.  The unique length
    // of the output file will be the "chunksize" with an overlap of 
    // "overlap" size with the following chunk.  Time0 represents the
    // beginning of the oldest open chunk.  (It's initialized to -1 here
    // as a flag that it needs to be set when the data is read).  When
    // the chunk is closed, Time0 is increased by "iterator" to ensure
    // that it increases uniformly.  "maxtime" tells us when the 50 MHz
    // clock rolls over, and is subtracted from times when appropriate.
    uint64_t time0 = -1;
    const double chunksize = 1.0; // Chunk Size in Seconds;
    const double overlap = 0.1; // Overlap Size in Seconds;
    const uint64_t ticks = int((chunksize+overlap)*50000000);
    const uint64_t iterator = int(chunksize*50000000);
    const uint64_t maxtime = (1UL << 43); 
    uint64_t time50 = 0;
    uint64_t time10 = 0;
    int index = 0;

    // Setup initial output file
    PZdabWriter* r1  = Output(index);
    PZdabWriter* w1 = r1;
    if(w1->IsOpen() == 0){
        std::cerr << "Could not open output file" << std::endl;
        return -1;
    }
    PZdabWriter* r2;
    PZdabWriter* w2;
    w2 = r2;
    int testw2 = -1;
    int parity = 0;

    // Loop over ZDAB Records
    while(1){
        nZDAB* data = p->NextRecord();
        if (data == NULL){
            w1->Close();
            Database(index, time10, time50);
            index++;
            time0 = time50;
            break;
        }

        // Set up Header Buffer
        nZDAB* mastheader = NULL;
        nZDAB* rhdrheader = NULL;
        nZDAB* trigheader = NULL;
        u_int32 bank_name = data->bank_name;
        if (bank_name == MAST_RECORD){
            printf("Found a MAST record!");
            mastheader = data;
        }
        if (bank_name == RHDR_RECORD){
            printf("Found a run header!");
            rhdrheader = data;
        }
        if (bank_name == TRIG_RECORD){
            printf("Found a TRIG record!");
            trigheader = data;
        }
        if (bank_name == ZDAB_RECORD){
            ;
        }

        PmtEventRecord* hits = p->GetPmtRecord(data);
        if (hits != NULL){
            // Get the 50MHz Clock Time
            // Implementing Part of Method Get50MHzTime() 
            // from PZdabFile.cxx
            time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
                   + hits->TriggerCardData.Bc50_1;
            // Now get the 10MHz Clock Time
            // Method taken from zdab_convert.cpp
            time10 = (uint64_t(hits->TriggerCardData.Bc10_2) << 32)
                     + hits->TriggerCardData.Bc10_1;
            // Set Time Origin
            if (time0 == -1){
                time0 = time50;
                // Make initial database entry
                Database(index, time10, time50);
            }
        }

        // Chop

        // There are twelve possible cases here: The 50 MHz clock can 
        // roll over during the unique interval, during the overlap 
        // period, or not at all.  In each case, we must be able to
        // identify whether our event falls in the unique interval or 
        // the overlap interval, and recognize the first event in the 
        // overlap interval as a special case.

        // We first identify where the rollover occurs:
        int rollflag = 0;
        if (time0 + iterator > maxtime)
            rollflag = 1;
        else if (time0 + ticks > maxtime)
            rollflag = 2;

        // Now we check the interval in each case:
        if ((rollflag==0) && (time50 < time0 + iterator) ||
            (rollflag==1) && (time50 < time0 + iterator - maxtime) ||
            (rollflag==1) && (time50 > time0) ||
            (rollflag==2) && (time50 < time0 + iterator))
            OutZdab(data, w1, p);
        else{
            if ((rollflag==0) && (time50 < time0 + ticks) ||
                (rollflag==1) && (time50 < time0 + ticks - maxtime) ||
                (rollflag==2) && (time50 > time0 + iterator) ||
                (rollflag==2) && (time50 < time0 + ticks - maxtime)){
                if(testw2==-1){
                    index++;
                    if(w2->IsOpen()==0){
                        std::cerr << "Could not open output file\n";
                        return -1;
                    }
                    OutZdab(mastheader, w2, p);
                    OutZdab(rhdrheader, w2, p);
                    OutZdab(trigheader, w2, p);
                    Database(index, time10, time50);
                    testw2 = 0;
                }
                OutZdab(data, w1, p);
                OutZdab(data, w2, p);    
            }
            else{
                if(parity%2 == 0 ){
                    r1->Close();
                    w1 = r2;
                    PZdabWriter* r1 = Output(index+1);
                    w2 = r1;
                }
                else{
                    r2->Close();
                    w1 = r1;
                    PZdabWriter* r2 = Output(index+1);
                    w2 = r2;
                }
                parity++;
                testw2 = -1;
                time0 += iterator;
                if (time0 > maxtime)
                    time0 -= maxtime;
            }
        }
    }
    return 0;
}

// This function writes index-time pairs to the clock database
void Database(int index, uint64_t time10, uint64_t time50){
    MYSQL* conn = mysql_init(NULL);
    if (! mysql_real_connect(conn, "cps4", "snot", "looseCable60",
                             "monitor",0,NULL,0))
        std::cerr << "Cannot write to database" << std::endl;
    std::stringstream query;
    query << "INSERT INTO clock VALUES (";
    query << index << "," << time10 << "," << time50 << ")";
    mysql_query(conn, query.str().c_str());
    mysql_close(conn);
}

// This function writes out the ZDAB record
void OutZdab(nZDAB* data, PZdabWriter* w, PZdabFile* p){
    int index = PZdabWriter::GetIndex(data->bank_name);
    if (index<0)
        std::cerr << "Unrecognized bank name" << std::endl;
    else
        w->WriteBank(p->GetBank(data), index);
}

// This function queries the clock database to determine the most recent
// value of the index.
int GetLastIndex(){
    MYSQL* conn = mysql_init(NULL);
    if (! mysql_real_connect(conn, "cps4","snot","looseCable60",
                             "monitor",0,NULL,0))
        std::cerr << "Cannot read from database" << std::endl;
    char* query = "SELECT MAX(id) AS id FROM clock";
    mysql_query(conn, query);
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    int id = atoi(row[0]) + 1;
    std::cerr << id << std::endl;
    mysql_close(conn);
    return id;
}

// This function builds a new output file for each chunk and should be
// called each time the index in incremented.
PZdabWriter* Output(int index){
    char outfilename[32];
    sprintf(outfilename, "chopped%i.zdab", index);
    PZdabWriter* w = new PZdabWriter(outfilename, 0);
    return w;
}
