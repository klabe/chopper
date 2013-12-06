#include "PZdabFile.h"
#include "PZdabWriter.h"
#include <string>
#include <iostream>
#include <mysql.h>
#include <sstream>
#include <stdint.h>

void Database(int, uint64_t, uint64_t);
void OutZdab(nZDAB*, PZdabWriter*, PZdabFile*);
void OutHeader(GenericRecordHeader*, PZdabWriter*, int);
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
    // beginning of the oldest open chunk, according to the LongTime 
    // clock.  When
    // the chunk is closed, Time0 is increased by "iterator" to ensure
    // that it increases uniformly.  "maxtime" tells us when the 50 MHz
    // clock rolls over, and is subtracted from times when appropriate.
    // LongTime is an internal 50 MHz clock that uses the full 64 bits
    // available so that it does not roll over during the execution of
    // the processor (it will last 5000 years).  Epoch counts the number
    // of times that the real 50 MHz clock has rolled over.
    uint64_t time0 = 0;
    int firstevent = -1;
    const double chunksize = 100.0; // Chunk Size in Seconds;
    const double overlap = 0.1; // Overlap Size in Seconds;
    const uint64_t ticks = int((chunksize+overlap)*50000000);
    const uint64_t iterator = int(chunksize*50000000);
    const uint64_t maxtime = (1UL << 43); 
    uint64_t time50 = 0;
    uint64_t time10 = 0;
    uint64_t longtime = 0;
    int epoch = 0;
    int index = GetLastIndex();

    // Setup initial output file
    PZdabWriter* w1  = Output(index);
    if(w1->IsOpen() == 0){
        std::cerr << "Could not open output file" << std::endl;
        return -1;
    }
    PZdabWriter* w2;
    int testw2 = -1;

    // Set up the Header Buffer
    const int headertypes = 3;
    uint32_t Headernames[headertypes] = 
             { RHDR_RECORD, TRIG_RECORD, EPED_RECORD };
    char* header[headertypes];
    for(int i = 0; i<headertypes; i++){
        header[i] = (char*) malloc(NWREC);
        memset(header[i],0,NWREC*sizeof(char));
    }

    // Loop over ZDAB Records
    while(1){
        nZDAB* data = p->NextRecord();
        if (data == NULL){
            w1->Close();
            if(testw2 == 0)
                w2->Close();
            Database(index, time10, time50);
            index++;
            time0 = time50;
            break;
        }

        // Check to fill Header Buffer
        uint32_t bank_name = data->bank_name;
        for (int i=0; i<headertypes; i++){
            if (bank_name == Headernames[i]){
                memset(header[i],0,NWREC*sizeof(char));
                GenericRecordHeader* grh = (GenericRecordHeader*) data;
                unsigned long recLen = grh->RecordLength; 
                SWAP_INT32(data,recLen/sizeof(uint32_t));
                memcpy(header[i], data+1, recLen*sizeof(char));
                SWAP_INT32(data,recLen/sizeof(uint32_t));
            }
        }

        // If the event has an associated time, compute every
        // conceivable time variable.
        PmtEventRecord* hits = p->GetPmtRecord(data);
        if (hits != NULL){
            // Store the old 50MHz Clock Time for comparison
            uint64_t oldtime = time50;

            // Get the current 50MHz Clock Time
            // Implementing Part of Method Get50MHzTime() 
            // from PZdabFile.cxx
            time50 = (uint64_t(hits->TriggerCardData.Bc50_2) << 11)
                   + hits->TriggerCardData.Bc50_1;
            // Now get the 10MHz Clock Time
            // Method taken from zdab_convert.cpp
            time10 = (uint64_t(hits->TriggerCardData.Bc10_2) << 32)
                     + hits->TriggerCardData.Bc10_1;

            // Check for pathological case
            if (time50 == 0)
                time50 = oldtime;

            // Check whether clock has rolled over
            if (time50 < oldtime)
                epoch++;
            
            // Set the Internal Clock
            longtime = time50 + maxtime*epoch;

            // Set Time Origin
            if (firstevent == -1){
                time0 = longtime;
                // Make initial database entry
                Database(index, time10, time50);
                firstevent = 0;
            }
        }

        // Chop
        if (longtime < time0 + iterator)
            OutZdab(data, w1, p);
        else{
            if (longtime < time0 + ticks){
                if(testw2==-1){
                    w2 = Output(index+1);
                    if(w2->IsOpen()==0){
                        std::cerr << "Could not open output file\n";
                        return -1;
                    }
                    for(int i=0; i<headertypes; i++){
                        OutHeader((GenericRecordHeader*) header[i], w2, i);
                    }
                    Database(index, time10, time50);
                    testw2 = 0;
                }
                OutZdab(data, w1, p);
                OutZdab(data, w2, p);
            }
            else{
                index++;
                w1->Close();
                if(testw2 == -1){
                    w1 = Output(index);
                    for(int i=0; i<headertypes; i++)
                        OutHeader((GenericRecordHeader*) header[i], w1, i);
                    Database(index, time10, time50);
                }
                if(testw2 == 0){
                    w2->Close();
                    testw2 = -1;
                    w1 = Output(index);
                }
                OutZdab(data, w1, p);
                time0 += iterator;
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
    if (data!=NULL){
        int index = PZdabWriter::GetIndex(data->bank_name);
        if (index<0)
            std::cerr << "Unrecognized bank name" << std::endl;
        else{
            uint32_t *bank = p->GetBank(data);
            if(index==0)
                SWAP_INT32(bank+3,1);
            w->WriteBank(bank, index);
        }
    }
}

// This function writes out the header buffer to a file
void OutHeader(GenericRecordHeader* data, PZdabWriter* w, int j){
    if (data!=NULL){
        int index = w->GetIndex(data->RecordID);
        if(index < 0){
            fprintf(stderr,"Did not recognize index %i in record id %lx,"
                    " record lost\n",index,data->RecordID);
            if(j==0){
                index = 2;
            }
            if(j==1){
                index = 4;
            }
            if(j==2){
                index = 3;
            }
        }
        if(w->WriteBank((uint32_t *)(data), index)){
            fprintf(stderr,"Error writing to zdab file\n");
        }
    }
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
    if (row == NULL){
        std::cerr << "No data in clock database" << std::endl;
        return 0;
    }
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
