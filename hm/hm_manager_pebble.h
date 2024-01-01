#ifndef LEVELDB_HM_MANAGER_H
#define LEVELDB_HM_MANAGER_H

//////
// Module function: Main module
//////

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <map>
#include <vector>
#include <unordered_set>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <algorithm>

#include "../db/dbformat.h"
#include "../hm/my_log.h"
#include "../hm/BitMap.h"
#include "../hm/hm_status.h"
#include <utility>
#include <algorithm>
#include "hm_interface.h"

extern "C"
{
#include <libzbc/zbc.h>
}

namespace leveldb
{

    class HMManager
    {
    public:
        HMManager(const Comparator *icmp);
        ~HMManager();

        ssize_t hm_write(int level, uint64_t filenum, const void *buf, uint64_t count,
                         const std::string &guard_key); // write a SSTable file to a level
        void rebuild_data(std::vector<struct Ldbfile *> &file);
        void reset_zone();
        ssize_t hm_read(uint64_t filenum, void *buf, uint64_t count, uint64_t offset); // read a SSTable file
        ssize_t hm_delete(uint64_t filenum);                                           // delete a SSTable file
        ssize_t move_file(uint64_t filenum, int to_level);                             // move a SSTable file
        struct Ldbfile *get_one_table(uint64_t filenum);                               // get a SSTable file pointer
        int64_t get_table_size(uint64_t filenum);
        static bool cmp(std::pair<struct Zonefile *, uint64_t> &a, std::pair<struct Zonefile *, uint64_t> &b);
        void gc();
        ssize_t move_zone(uint64_t src, uint64_t dst);
        bool find_gc_list(std::vector<std::pair<struct Zonefile *, uint64_t>> &candidate_gc_zone_and_size,
                          uint64_t zone_start, uint64_t zone_end,
                          std::vector<std::pair<struct Ldbfile *, uint64_t>> &gc_list, uint64_t limit_zone_num,
                          uint64_t &new_zone_num, bool include_open_zone);
        bool move_files_in_gc_list(std::vector<std::pair<struct Ldbfile *, uint64_t>> &gc_list, uint64_t needed_zone,
                                   int reclaim_level, bool include_open_zone);
        ssize_t move_file_to_not_write_zone(uint64_t filenum, struct Zonefile *dst_zone);
        ssize_t merge_and_move_two_zones(uint64_t src_zone1, uint64_t src_zone2);
        static bool cmp_files(struct Ldbfile *a, struct Ldbfile *b);
        void new_level0_senfiles_zone();
        void get_table(std::map<uint64_t, struct Ldbfile *> **table_map)
        {
            *table_map = &table_map_;
        }; // get table_map

        //////dump relation
        void get_zone_table(uint64_t filenum, std::vector<struct Ldbfile *> **zone_table);
        bool trivial_zone_size_move(uint64_t filenum);
        void move_zone(uint64_t filenum);
        //////

        //////compaction relation
        int64_t is_guard_free_zone(int level, std::map<uint64_t, uint64_t> &file_in_guards,
                                   const std::string &target_guard);
        std::map<uint64_t, uint64_t>
        get_zone_files_based_on_files(int level, std::map<uint64_t, uint64_t> &file_in_main_compaction_guards,
                                      std::map<uint64_t, uint64_t> &zone_in_compaction);
        std::map<uint64_t, uint64_t>
        get_new_zone_files_based_on_files(int level, std::map<uint64_t, uint64_t> &file_in_main_compaction_guards,
                                          std::map<uint64_t, uint64_t> &ignore_zone,
                                          std::map<uint64_t, uint64_t> &new_erase_zone);
        void update_com_window(int level);
        void get_com_window_table(int level, std::vector<struct Ldbfile *> *window_table);
        ssize_t adjust_com_window_num(int level);
        void set_com_window(int level, int num);
        void set_com_window_seq(int level, int num);
        //////

        // get table offset
        void get_table_offsets(std::vector<int64_t> &file_nums, std::vector<int64_t> &file_offsets);

        //////statistics
        uint64_t get_zone_num();
        void get_zone_info();
        uint64_t get_guard_accross_zone(int level, const std::vector<uint64_t> files);
        void get_guard_info(uint64_t timestamp);
        uint64_t get_file_zonenum(uint64_t filenum);
        void get_one_level(int level, uint64_t *table_num, uint64_t *table_size);
        void get_per_level_info();
        void get_valid_info();
        void get_all_info(uint64_t timestamp);
        void get_valid_data();
        void get_my_info(int num);
        void get_valid_all_data(int num);
        bool check_rewrite_l6();
        void reset_rewrite_l6();
        uint64_t get_total_zone();
        uint64_t open_zone_max_left(int level);
        uint64_t move_out_open_zone(int level, std::map<uint64_t, uint64_t> &move_out_zone);
        void print_space_amplication(uint64_t i);
        ////// for checking
        std::string get_guard_key_by_filenum(uint64_t filenum)
        {
            std::map<uint64_t, struct Ldbfile *>::iterator it = table_map_.find(filenum);
            if (it == table_map_.end())
            {
                printf("error:move file failed! no find file:%ld\n", filenum);
                assert(0);
            }
            printf("findnumincheck %lu %s\n", filenum, it->second->guard.c_str());

            return it->second->guard;
        };
        //////end
        void print_read_log()
        {
            print_read_log_ = 1;
        }
        std::mutex mtx_;
        std::shared_mutex mutex_;

    private:
        BitMap *bitmap_;

        hm_interface *dev_;
        std::vector<struct hm_zone *> zone_;
        unsigned int zonenum_;
        int first_zonenum_;
        uint64_t zonesize_;

        const InternalKeyComparator icmp_;

        std::map<uint64_t, struct Ldbfile *> table_map_; //<file number, metadate pointer>
        std::vector<struct Zonefile *> zone_info_;       // zone info for non-written zone
        uint64_t open_zones_pointer[7];
        uint64_t open_zones[7][OPEN_ZONE_NUM];
        std::multimap<std::string, uint64_t> guard_to_zones_[7];
        std::vector<std::string> guards_bound[7];

        int restrict_zone_num_;
        int valid_zone_;
        int mid_zonenum_;
        uint64_t *sample_zones;
        int midspeed_start_zonenum_;
        int slow_start_zone_num_;
        int sample_range_;
        //////statistics
        uint64_t invalid_space;
        uint64_t invalid_cause_by_level0;
        uint64_t delete_zone_num;
        uint64_t all_table_size;
        uint64_t table_size[7];
        bool rewrite_level_6;
        uint64_t last_insert_table_size;
        uint64_t insert_table_size;
        uint64_t kv_store_block;
        uint64_t last_kv_store_block;
        uint64_t kv_read_block;
        uint64_t max_zone_num;
        uint64_t move_file_size;
        uint64_t read_time;
        uint64_t write_time;
        uint64_t GC_time;
        uint64_t waste_find_time;
        uint64_t find_time;
        bool print_read_log_;
        //////end

        int set_first_zonenum();
        struct Zonefile *hm_alloc(int level, uint64_t size, const std::string &guard);
        ssize_t hm_alloc_zone(int level);
        struct Zonefile *new_and_switch_slow_zone(int write_zone);
        struct Zonefile *new_and_switch_midspeed_zone(int write_zone);
        struct Zonefile *new_and_switch_fast_zone(int write_zone);
        struct Zonefile *hm_alloc_for_senfile(uint64_t size);
        void hm_free_zone(uint64_t zone);
        void soft_guard(const std::string &key);
        int64_t get_table_offset(int64_t file_num);

        //////
        bool is_com_window(int level, uint64_t zone);
        //////
    };

} // namespace leveldb

#endif
