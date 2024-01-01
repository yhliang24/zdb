#ifndef LEVELDB_HM_STATUS_H
#define LEVELDB_HM_STATUS_H

//////
// Module function: Some variables and structures
//////

#include <vector>
#include "pebblesdb/options.h"

#define ALIGN_BLOCK_SIZE 4096 // the data aligned to this

#define MEMALIGN_SIZE \
    (sysconf(_SC_PAGESIZE)) // The size of the alignment when applying for memory using
                            // posix_memalign

#define Verify_Table \
    1 //To confirm whether the SSTable is useful, every time an SSTable is written to the disk, \
                             //it will read the handle of the file and add it to the leveldb's table cache. This is the leveldb's own mechanism; \
        // 1 means that there is this mechanism; 0 means no such mechanism.

#define Read_Whole_Table 0 // if 0, then always read block size
#define OPEN_ZONE_NUM 1
#define PRINTOUT_DEBUG 1
#define MOVE_OUT_LEVEL_0_ZONE 0
#define RAM_PICK_FILE 1
// #define BASE_SCORE_LIMIT ((config::kL0_SlowdownWritesTrigger * 1.0) / config::kL0_CompactionTrigger)
#define BASE_SCORE_LIMIT 2
#define SEEK_COM 2 // define the seek compaction threahold
#define FX_RATIO 0.5
#define MAX_SST_ALLOWED_IN_GUARD 10
#define RESTRICT_ZONE_NUM 600
#define TOP_BITS 25
#define READ_WRITE_LOCK 1
namespace leveldb
{

    static const char smr_filename[] = "/dev/sda"; // e.g. smr_filename[]="/dev/sdb1";
    static const char zns_name[] = "traddr:10000:01:00.0";
    // static const char drive_name[] = "/dev/nvme0n1";
    static const char drive_name[] = "/dev/sdb";
    struct Ldbfile
    {                    // file = SSTable ,file Metadata struct
        uint64_t table;  // file name = fiel serial number
        uint64_t zone;   // file's zone number
        uint64_t offset; // file's offset in the zone
        uint64_t size;   // file's size
        int level;       // file in the level number
        std::string guard;

        Ldbfile(uint64_t a, uint64_t b, uint64_t c, uint64_t d, int e, const std::string &f)
            : table(a), zone(b), offset(c), size(d), level(e), guard(f){};
        ~Ldbfile(){};
    };

    struct Zonefile
    {                  // zone struct
        uint64_t zone; // zone num

        std::vector<struct Ldbfile *> ldb; // SSTable pointers
        Zonefile(uint64_t a) : zone(a){};
        ~Zonefile(){};

        void add_table(struct Ldbfile *file)
        {
            ldb.push_back(file);
        }
        void delete_table(struct Ldbfile *file)
        {
            std::vector<struct Ldbfile *>::iterator it;
            for (it = ldb.begin(); it != ldb.end();)
            {
                if ((*it) == file)
                {
                    ldb.erase(it);
                    return;
                }
                else
                    it++;
            }
        }
        uint64_t get_all_file_sizev2()
        {
            uint64_t size = 0;
            std::vector<struct Ldbfile *>::iterator it;
            for (it = ldb.begin(); it != ldb.end(); it++)
            {
                if ((*it)->size % ALIGN_BLOCK_SIZE == 0)
                {
                    size += (*it)->size;
                }
                else
                {
                    size += ((*it)->size / ALIGN_BLOCK_SIZE + 1) * (ALIGN_BLOCK_SIZE);
                }
            }
            return size;
        }

        uint64_t get_all_file_size()
        {
            uint64_t size = 0;
            std::vector<struct Ldbfile *>::iterator it;
            for (it = ldb.begin(); it != ldb.end(); it++)
            {
                size += (*it)->size;
            }
            return size;
        }

        void print_all_file()
        {
            std::vector<struct Ldbfile *>::iterator it;
            printf("files ");
            for (it = ldb.begin(); it != ldb.end(); it++)
            {
                printf("%lu ", (*it)->table);
            }
        }
        uint64_t get_min_file_size()
        {
            uint64_t min_size = 1024 * 1024 * 1024;
            std::vector<struct Ldbfile *>::iterator it;
            for (it = ldb.begin(); it != ldb.end(); it++)
            {
                if (min_size < (*it)->size)
                    min_size = (*it)->size;
            }
            return min_size;
        }
    };

} // namespace leveldb

#endif
