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

    // Initialize time information
    uint64_t time0 = -1;
    const double chunksize = 1.2; // Chunk Size in Seconds;
    const uint64_t ticks = int(chunksize*50000000);
    const uint64_t maxtime = (1UL << 43); 
    uint64_t time = 0;
    uint64_t time10 = 0;
    int index = GetLastIndex();

    // Setup output file
    char* outfilename = "~/chopped.zdab";
    PZdabWriter* w = new PZdabWriter(outfilename,0);
    if(w->IsOpen() == 0){
        std::cerr << "Could not open output file" << std::endl;
        return -1;
    }

    // Loop over ZDAB Records
    while(1){
        nZDAB* data = p->NextRecord();
            if (data == NULL){
                w->Close();
                Database(index, time10, time);
                index++;
                time0 = time;
                break;
            }
        PmtEventRecord* hits = p->GetPmtRecord(data);
        if (hits != NULL){
            // Get the 50MHz Clock Time
            // Implementing Part of Method Get50MHzTime() 
            // from PZdabFile.cxx
            time = hits->TriggerCardData.Bc50_2 << 11 
                   + hits->TriggerCardData.Bc50_1;
            // Now get the 10MHz Clock Time
            // Method taken from zdab_convert.cpp
            time10 = hits->TriggerCardData.Bc10_2 << 32
                     + hits->TriggerCardData.Bc10_1;
            if (time0 == -1)
                time0 = time;
        }
        // Output Zdab Record Here
        OutZdab(data, w, p);

        // Chop
        if ((time0 + ticks < maxtime && time > time0 + ticks) ||
            (time > time0 + ticks - maxtime && time < time0) ){
            w->Close();
            // START NEW FILE
            Database(index, time10, time);
            index++;
            std::cerr << index << std::endl;
            time0 = time;
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
    query << "INSERT INTO Index VALUES (";
    query << index << "," << time10 << "," << time50 << ")";
    mysql_query(conn, query.str().c_str());
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
    return id;
}
