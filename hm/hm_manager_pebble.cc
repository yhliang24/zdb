#include "../hm/hm_manager_pebble.h"
#include "../db/murmurhash3.h"
#include <cstdint>
#include <fcntl.h>
#include <stdint.h>
#include <sys/time.h>

#define GC_BASE_LEVEL 1
#define GC_way 1 // 2: valid base 1: zone base 0: size based
#define ZONE_AWARE 0
#define SAMPLING 0
#define LEVEL_AWARE 1
namespace leveldb
{
    static uint64_t get_now_micros()
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (tv.tv_sec) * 1000000 + tv.tv_usec;
    }

    static size_t random_number(size_t size)
    {
        return rand() % size;
    }

    HMManager::HMManager(const Comparator *icmp) : icmp_(icmp)
    {
        ssize_t ret;
        // open
        // dev_ = new spdk_interface();
        // dev_ = new hmsmr_interface();
        dev_ = new zbd_interface();
        zonenum_ = dev_->get_zone_disk_num();
        first_zonenum_ = dev_->get_first_seq_zone_num();

        dev_->get_all_zone_info_wait_for_complete(zone_);

        for (int i = 0; i < zonenum_; i++)
        {
            zone_info_.push_back(nullptr);
        }
        restrict_zone_num_ = RESTRICT_ZONE_NUM;
        valid_zone_ = 0;
        bitmap_ = new BitMap(zonenum_);
        zonesize_ = zone_[first_zonenum_]->hm_length * dev_->get_library_unit();

        sample_zones = new uint64_t[restrict_zone_num_];
        for (int i = 0; i < restrict_zone_num_; i++)
        {
            sample_zones[i] = first_zonenum_ + i;
        }

        for (int i = 0; i < 7; i++)
        {
            for (int j = 0; j < OPEN_ZONE_NUM; j++)
                open_zones[i][j] = zonenum_ + 1;
            open_zones_pointer[i] = OPEN_ZONE_NUM;
            table_size[i] = 0;
        }

        init_log_file();

        MyLog("\n  ROCKSDB  \n");
        MyLog("Verify_Table:%d Read_Whole_Table:%d "
              "ONLY_COMPACTION_ENABLE:%d\n",
              Verify_Table, Read_Whole_Table, ONLY_COMPACTION_ENABLE);
        MyLog("the first_zonenum_:%d zone_num:%ld mid start %ld slow start %ld\n", first_zonenum_, zonenum_,
              midspeed_start_zonenum_, slow_start_zone_num_);
        MyLog("GC_BASE_LEVEL %d fastzone %d "
              "restricted zone %lu\n",
              GC_BASE_LEVEL, sample_range_, restrict_zone_num_);
        MyLog("MOVE_OUT_LEVEL 0 %ld\n", MOVE_OUT_LEVEL_0_ZONE);
        MyLog("BASE_SCORE_LIMIT %lf\n", BASE_SCORE_LIMIT);
        MyLog("fx ratio %lf\n", FX_RATIO);
        MyLog("zonesize %ld\n", zonesize_);
        //////statistics
        invalid_space = 0;
        invalid_cause_by_level0 = 0;
        all_table_size = 0;
        kv_store_block = 0;
        kv_read_block = 0;
        move_file_size = 0;
        read_time = 0;
        write_time = 0;
        print_read_log_ = 0;
    }

    void HMManager::reset_zone()
    {
        dev_->hm_reset_all_zone_wait_for_complete();
    }

    void HMManager::rebuild_data(std::vector<struct Ldbfile *> &files)
    {
        uint64_t *zone_write_pointer = new uint64_t[zonenum_];
        for (int i = 0; i < zonenum_; i++)
        {
            zone_write_pointer[i] = 0;
        }
        uint64_t sstable_ocuppied_size = 0;
        for (int i = 0; i < files.size(); i++)
        {
            struct Ldbfile *file = files[i];
            uint64_t write_zone = file->zone;
            MyLog("rebuild %ld %ld %ld %ld %ld\n", file->table, write_zone, file->offset, file->size, file->level);
            struct Ldbfile *ldb = new Ldbfile(file->table, write_zone, file->offset, file->size, file->level, "");
            table_map_.insert(std::pair<uint64_t, struct Ldbfile *>(file->table, ldb));
            if (zone_info_[write_zone] == nullptr)
            {
                zone_info_[write_zone] = new Zonefile(write_zone);
                valid_zone_++; // for gc and info
            }
            zone_info_[write_zone]->add_table(ldb);
            bitmap_->set(write_zone);
            all_table_size += file->size;         // for gc and info
            table_size[file->level] += ldb->size; // for gc and info

            // get the write pointer to reset the invalid space
            uint64_t candidate_write_pointer = 0;
            // when delete a file, invalid data will increase
            if (file->size % ALIGN_BLOCK_SIZE == 0)
            {
                sstable_ocuppied_size += file->size / ALIGN_BLOCK_SIZE;
                candidate_write_pointer = file->offset * ALIGN_BLOCK_SIZE / dev_->get_library_unit() +
                                          file->size / ALIGN_BLOCK_SIZE * dev_->get_library_unit();
            }
            else
            {
                candidate_write_pointer = file->offset * ALIGN_BLOCK_SIZE / dev_->get_library_unit() +
                                          (file->size / ALIGN_BLOCK_SIZE + 1) * dev_->get_library_unit();
                sstable_ocuppied_size += (file->size / ALIGN_BLOCK_SIZE + 1);
            }
            if (zone_write_pointer[write_zone] < candidate_write_pointer)
            {
                zone_write_pointer[write_zone] = candidate_write_pointer;
            }
        }
        uint64_t valid_size = 0;
        for (int i = 0; i < zonenum_; i++)
        {
            if (zone_write_pointer[i] == 0)
                continue;
            // valid size
            struct hm_zone *h_zone = new hm_zone();
            dev_->get_one_zone_info_wait_for_complete(i, h_zone);
            if (h_zone->hm_write_pointer != zone_write_pointer[i])
            {
                printf("rebuild wp not equal\n");
            }
            valid_size += (h_zone->hm_write_pointer - zone_[i]->hm_start);
            delete h_zone;
        }

        invalid_space = (valid_size - sstable_ocuppied_size) * ALIGN_BLOCK_SIZE;
        MyLog("valid disk size %ld sstable ocuppied size %ld invalid space %ld\n", valid_size, sstable_ocuppied_size,
              invalid_space);
        delete[] zone_write_pointer;
    }

    HMManager::~HMManager()
    {
        get_all_info(0);

        std::map<uint64_t, struct Ldbfile *>::iterator it = table_map_.begin();
        while (it != table_map_.end())
        {
            delete it->second;
            it = table_map_.erase(it);
        }
        table_map_.clear();
        int i;
        std::vector<struct Zonefile *>::iterator iz = zone_info_.begin();
        while (iz != zone_info_.end())
        {
            if (*iz != nullptr)
            {
                delete (*iz);
            }
            iz = zone_info_.erase(iz);
        }
        zone_info_.clear();

        // list zone
        for (int i = 0; i < zonenum_; i++)
        {
            delete zone_[i];
        }

        delete dev_;

        if (bitmap_)
        {
            free(bitmap_);
        }
        delete[] sample_zones;
    }

    ssize_t HMManager::hm_alloc_zone(int level)
    {
        ssize_t i;
        uint64_t start_zone = 0;

        for (i = 0; i < restrict_zone_num_; i++)
        { // Traverse from the first sequential write zone
            uint64_t candidate_zone;
            candidate_zone = sample_zones[(start_zone + i) % restrict_zone_num_];
            if (bitmap_->get(candidate_zone) == 0)
            {
                struct hm_zone *h_zone = new hm_zone();
                dev_->get_one_zone_info_wait_for_complete(candidate_zone, h_zone);
                if (h_zone->hm_write_pointer != zone_[candidate_zone]->hm_start)
                {
                    MyLog("alloc error: zone:%ld cur wp:%ld start wp:%ld\n", candidate_zone, h_zone->hm_write_pointer,
                          zone_[candidate_zone]->hm_start);
                    dev_->hm_reset_zone_wait_for_complete(zone_[candidate_zone]->hm_start);
                    delete h_zone;
                    continue;
                }
                if (h_zone)
                    delete h_zone;
                bitmap_->set(candidate_zone);
                valid_zone_++;
                zone_[candidate_zone]->hm_write_pointer = zone_[candidate_zone]->hm_start;
                MyLog2("new zone %lu\n", candidate_zone);
                return candidate_zone;
            }
        }
        // printf("hm_alloc_zone failed!\n");
        // assert(0);
        return -1;
    }

    // ugly suitable fit
    void HMManager::hm_free_zone(uint64_t zone)
    {
        ssize_t ret;
        ret = dev_->hm_reset_zone_wait_for_complete(zone_[zone]->hm_start);
        if (ret != 0)
        {
            MyLog("reset zone:%ld faild! error:%ld\n", zone, ret);
        }
        bitmap_->clr(zone);
        zone_[zone]->hm_write_pointer = zone_[zone]->hm_start;
        valid_zone_--;
        MyLog2("finish free zone %lu\n", zone);
    }

    struct Zonefile *HMManager::hm_alloc(int level, uint64_t size, const std::string &guard)
    {
        uint64_t need_size = (size % ALIGN_BLOCK_SIZE) ? (size / ALIGN_BLOCK_SIZE + 1)
                                                       : size / ALIGN_BLOCK_SIZE; // write size (block unit)
        uint64_t write_zone = 0;

        uint64_t write_time_begin = get_now_micros();
        if (open_zones_pointer[level] >= OPEN_ZONE_NUM)
        { // no open zone
            write_zone = hm_alloc_zone(level);
            if (write_zone != -1)
            {
                // add zone into open zone
                open_zones_pointer[level] = 0;
                open_zones[level][0] = write_zone;
                struct Zonefile *zf = new Zonefile(write_zone);
                zone_info_[write_zone] = zf;

                printf("heregap 1 %ld\n", get_now_micros() - write_time_begin);
                return zf;
            }
        }
        else
        {
            // see if open_zones_pointer can hold the need_size
            write_zone = open_zones[level][open_zones_pointer[level]];
            if ((zone_[write_zone]->hm_length - (zone_[write_zone]->hm_write_pointer - zone_[write_zone]->hm_start)) *
                    dev_->get_library_unit() / ALIGN_BLOCK_SIZE >=
                need_size)
            {
                printf("heregap2 %ld\n", get_now_micros() - write_time_begin);
                return zone_info_[write_zone];
            }
            // if can't, see if other open zone pointer can hold the need_size
            uint64_t min_left_size = zonesize_ + 1;
            uint64_t min_left_size_zone_ind = OPEN_ZONE_NUM;
            uint64_t empty_chair = OPEN_ZONE_NUM;
            for (int i = 0; i < OPEN_ZONE_NUM; i++)
            {
                if (open_zones[level][i] >= zonenum_ + 1)
                {
                    empty_chair = i;
                    continue;
                }
                write_zone = open_zones[level][i];
                uint64_t zone_left_size = (zone_[write_zone]->hm_length -
                                           (zone_[write_zone]->hm_write_pointer - zone_[write_zone]->hm_start)) *
                                          dev_->get_library_unit() / ALIGN_BLOCK_SIZE;
                MyLog2("Checking Open Zone %ld ind %d zone left size %ld needed size %ld\n", write_zone, i,
                       zone_left_size, need_size);
                if (zone_left_size >= need_size)
                {
                    open_zones_pointer[level] = i;

                    printf("heregap3 %ld\n", get_now_micros() - write_time_begin);
                    return zone_info_[write_zone];
                }
                // for evict zone if no zone can hold sst
                if (zone_left_size < min_left_size)
                {
                    min_left_size = zone_left_size;
                    min_left_size_zone_ind = i;
                }
            }
            assert(min_left_size_zone_ind != OPEN_ZONE_NUM);
            if (empty_chair == OPEN_ZONE_NUM)
            {
                empty_chair = min_left_size_zone_ind;
            }
            // if non of them can hold need_size, then new a zone to hold and evict the
            // minimum left-space zone out
            write_zone = hm_alloc_zone(level);
            if (write_zone != -1)
            {
                if (empty_chair == min_left_size_zone_ind)
                {
                    dev_->hm_finish_zone_wait_for_complete(zone_[open_zones[level][empty_chair]]->hm_start);
                    MyLog2("level %d empty size %lf need size %lf\n", level,
                           (zone_[open_zones[level][empty_chair]]->hm_length -
                            (zone_[open_zones[level][empty_chair]]->hm_write_pointer -
                             zone_[open_zones[level][empty_chair]]->hm_start)) *
                               dev_->get_library_unit() / 1024.0 / 1024.0, // get hm_start unit
                           need_size * ALIGN_BLOCK_SIZE / 1024.0 / 1024.0);
                }
                else
                {
                    MyLog2("level %d new zone %ld\n", level, write_zone);
                }
                open_zones[level][empty_chair] = write_zone; // evict and put
                open_zones_pointer[level] = empty_chair;
                struct Zonefile *zf = new Zonefile(write_zone);
                zone_info_[write_zone] = zf;
                printf("heregap4%ld\n", get_now_micros() - write_time_begin);
                return zf;
            }
        }
        MyLog2("no free zone\n");
        assert(0);
        return nullptr;
    }

    ssize_t HMManager::hm_write(int level, uint64_t filenum, const void *buf, uint64_t count, const std::string &guard)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::unique_lock<std::shared_mutex> lock(mutex_);
#endif
        uint64_t write_time_begin = get_now_micros();

        bool needed_gc = false;
        // get the zone that contain target guard
        struct Zonefile *write_zone_info = NULL;
        write_zone_info = hm_alloc(level, count, std::string(""));

        assert(write_zone_info != NULL);

        if (PRINTOUT_DEBUG)
        {
            // debug
            uint64_t table_occupied = write_zone_info->get_all_file_sizev2();
            if (level >= 5 && (table_occupied + ((zone_[write_zone_info->zone]->hm_length -
                                                  (zone_[write_zone_info->zone]->hm_write_pointer -
                                                   zone_[write_zone_info->zone]->hm_start)) *
                                                 dev_->get_library_unit())) != zonesize_)
            {
                MyLog2("%ld %ld %ld %ld", write_zone_info->zone, table_occupied,
                       ((zone_[write_zone_info->zone]->hm_length -
                         (zone_[write_zone_info->zone]->hm_write_pointer - zone_[write_zone_info->zone]->hm_start)) *
                        dev_->get_library_unit()),
                       (table_occupied +
                        ((zone_[write_zone_info->zone]->hm_length -
                          (zone_[write_zone_info->zone]->hm_write_pointer - zone_[write_zone_info->zone]->hm_start)) *
                         dev_->get_library_unit())));
                assert(0);
            }
        }

        void *w_buf = NULL;
        uint64_t write_zone = write_zone_info->zone;
        uint64_t block_count;
        uint64_t block_ofst = zone_[write_zone]->hm_write_pointer * dev_->get_library_unit() / ALIGN_BLOCK_SIZE;
        ssize_t ret;
        MyLog("hmwrite %lu %f MB zone %lu ofs %lu\n", filenum, (float)count / 1024.0 / 1024.0, write_zone, block_ofst);
        MyLog2("hmwrite %lu %f MB zone %lu ofs %lu\n", filenum, (float)count / 1024.0 / 1024.0, write_zone, block_ofst);
        MyLog2("hmwrite %lu %f MB zone %lu ofs %lu\n", filenum, (float)count / 1024.0 / 1024.0, write_zone, block_ofst);
        printf("hmwrite %lu %f MB zone %lu ofs %lu\n", filenum, (float)count / 1024.0 / 1024.0, write_zone, block_ofst);
        if (count % ALIGN_BLOCK_SIZE == 0)
        {
            block_count = count / ALIGN_BLOCK_SIZE;
            ret = posix_memalign(&w_buf, MEMALIGN_SIZE, block_count * ALIGN_BLOCK_SIZE);
            memcpy(w_buf, buf, count);
            ret = dev_->hm_interface_write(w_buf, block_count, block_ofst);
            free(w_buf);
        }
        else
        {
            block_count = (count / ALIGN_BLOCK_SIZE + 1); // Align with physical block
            // w_buf=(void *)malloc(block_count*dev_->get_library_unit());
            ret = posix_memalign(&w_buf, MEMALIGN_SIZE, block_count * ALIGN_BLOCK_SIZE);
            if (ret != 0)
            {
                printf("error:%ld posix_memalign falid!\n", ret);
                return -1;
            }
            memset(w_buf, 0, block_count * ALIGN_BLOCK_SIZE);
            memcpy(w_buf, buf, count);
            ret = dev_->hm_interface_write(w_buf, block_count, block_ofst);
            free(w_buf);
        }
        if (ret < 0)
        {
            printf("ret: %ld error:%ld hm_write falid! table:%ld\n", ret, errno, filenum);
            return -1;
        }
        uint64_t write_time_end = get_now_micros();
        write_time += (write_time_end - write_time_begin);
        zone_[write_zone]->hm_write_pointer +=
            block_count * ALIGN_BLOCK_SIZE / dev_->get_library_unit(); // wp is sector unit
        struct Ldbfile *ldb;
        ldb = new Ldbfile(filenum, write_zone, block_ofst, count, level, guard);

        table_map_.insert(std::pair<uint64_t, struct Ldbfile *>(filenum, ldb));
        write_zone_info->add_table(ldb);
        all_table_size += ldb->size;
        table_size[level] += ldb->size;
        kv_store_block += block_count;

        MyLog("write table:%ld to level-%d zone:%ld of size:%ld bytes ofst:%ld "
              "sect:%ld next:%ld zoneleft %ld onegaptime %ld\n",
              filenum, level, write_zone, count, block_ofst, block_count, block_ofst + block_count,
              zone_[write_zone]->hm_length - (zone_[write_zone]->hm_write_pointer - zone_[write_zone]->hm_start),
              write_time_end - write_time_begin);
        MyLog2("write table:%ld to level-%d zone:%ld of size:%ld bytes ofst:%ld "
               "sect:%ld next:%ld zoneleft %ld\n",
               filenum, level, write_zone, count, block_ofst, block_count, block_ofst + block_count,
               zone_[write_zone]->hm_length - (zone_[write_zone]->hm_write_pointer - zone_[write_zone]->hm_start));
        return ret * ALIGN_BLOCK_SIZE;
    }

    ssize_t HMManager::hm_read(uint64_t filenum, void *buf, uint64_t count, uint64_t offset)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        void *r_buf = NULL;
        uint64_t block_count;
        uint64_t block_ofst;
        uint64_t de_ofst;
        ssize_t ret;
        uint64_t read_time_begin = get_now_micros();

        std::map<uint64_t, struct Ldbfile *>::iterator it;
        it = table_map_.find(filenum);
        if (it == table_map_.end())
        {
            printf(" table_map_ can't find table:%ld!\n", filenum);
            return -1;
        }

        block_ofst = it->second->offset + (offset / ALIGN_BLOCK_SIZE);
        de_ofst = offset - (offset / ALIGN_BLOCK_SIZE) * ALIGN_BLOCK_SIZE;

        block_count = ((count + de_ofst) % ALIGN_BLOCK_SIZE)
                          ? ((count + de_ofst) / ALIGN_BLOCK_SIZE + 1)
                          : ((count + de_ofst) / ALIGN_BLOCK_SIZE); // Align with logical block

        // r_buf=(void *)malloc(block_count*dev_->get_library_unit());
        ret = posix_memalign(&r_buf, MEMALIGN_SIZE, block_count * ALIGN_BLOCK_SIZE);
        if (ret != 0)
        {
            printf("error:%ld posix_memalign falid!\n", ret);
            return -1;
        }
        memset(r_buf, 0, block_count * ALIGN_BLOCK_SIZE);
        uint64_t pure_read_time_begin = get_now_micros();
        ret = dev_->hm_interface_read(r_buf, block_count, block_ofst);
        uint64_t pure_read_time_end = get_now_micros();
        memcpy(buf, ((char *)r_buf) + de_ofst, count);
        free(r_buf);
        if (ret < 0)
        {
            printf("error:%ld hm_read falid! %lu %lu %p %lu\n", ret, block_ofst, filenum, dev_, block_count);
            return -1;
        }

        uint64_t read_time_end = get_now_micros();
        read_time += (read_time_end - read_time_begin);

        kv_read_block += block_count;
        // if(print_read_log_)
        MyLog("read table:%ld of size:%ld bytes file:%ld bytes zone:%ld level-%ld onetime %ld pureonetime %ld\n", filenum, count,
              it->second->size, it->second->zone, it->second->level, read_time_end - read_time_begin, pure_read_time_end - pure_read_time_begin);
        return count;
    }

    ssize_t HMManager::hm_delete(uint64_t filenum)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::unique_lock<std::shared_mutex> lock(mutex_);
#endif
        std::map<uint64_t, struct Ldbfile *>::iterator it;
        it = table_map_.find(filenum);
        if (it != table_map_.end())
        {
            struct Ldbfile *ldb = it->second;
            uint64_t zone_id = ldb->zone;
            uint64_t level = ldb->level;
            bool remove_zone_from_guard = false;
            std::string &guard = ldb->guard;
            // assert(guard.size() != 0);

            table_map_.erase(it);
            zone_info_[zone_id]->delete_table(ldb);

            // when delete a file, invalid data will increase
            if (ldb->size % ALIGN_BLOCK_SIZE == 0)
            {
                invalid_space += ldb->size;
                if (ldb->level == 0)
                    invalid_cause_by_level0 += ldb->size;
            }
            else
            {
                invalid_space += (ldb->size / ALIGN_BLOCK_SIZE + 1) * ALIGN_BLOCK_SIZE;
                if (ldb->level == 0)
                    invalid_cause_by_level0 += (ldb->size / ALIGN_BLOCK_SIZE + 1) * ALIGN_BLOCK_SIZE;
            }

            std::vector<struct Zonefile *>::iterator iz = zone_info_.begin();
            if (zone_info_[zone_id]->ldb.empty())
            {
                assert(invalid_space >=
                       (zone_[zone_id]->hm_write_pointer - zone_[zone_id]->hm_start) * dev_->get_library_unit());
                invalid_space -=
                    (zone_[zone_id]->hm_write_pointer - zone_[zone_id]->hm_start) * dev_->get_library_unit();
                if (ldb->level == 0)
                    invalid_cause_by_level0 -=
                        (zone_[zone_id]->hm_write_pointer - zone_[zone_id]->hm_start) * dev_->get_library_unit();

                struct Zonefile *zf = zone_info_[zone_id];
                zone_info_[zone_id] = nullptr;

                for (int i = 0; i < OPEN_ZONE_NUM; i++)
                {
                    if (zone_id == open_zones[level][i])
                    {
                        open_zones[level][i] = zonenum_ + 1;
                        if (i == open_zones_pointer[level])
                        {
                            // choose a new pointer
                            open_zones_pointer[level] = OPEN_ZONE_NUM;
                            for (int j = 0; j < OPEN_ZONE_NUM; j++)
                            {
                                if (open_zones[level][j] != zonenum_ + 1)
                                {
                                    open_zones_pointer[level] = j;
                                    break;
                                }
                            }
                        }
                        break;
                    }
                }

                delete zf;
                hm_free_zone(zone_id);
                MyLog("delete zone:%ld from level-%d\n", zone_id, level);
                MyLog2("delete zone:%ld from level-%d\n", zone_id, level);
                remove_zone_from_guard = true;
            }
            else
            {
                // see if zone is not containing guard anymore.
                std::vector<struct Ldbfile *>::iterator ldb_iter = zone_info_[zone_id]->ldb.begin();
                bool flag = false;
                for (; ldb_iter < zone_info_[zone_id]->ldb.end(); ldb_iter++)
                {
                    if ((*ldb_iter)->level == level)
                    {
                        if (guard.compare((*ldb_iter)->guard) == 0)
                        {
                            flag = true;
                            break;
                        }
                    }
                }
                remove_zone_from_guard = !flag;
            }

            MyLog("delete table:%ld from level-%d zone:%ld of size:%ld MB\n", filenum, ldb->level, zone_id,
                  ldb->size / 1048576);
            all_table_size -= ldb->size;
            table_size[ldb->level] -= ldb->size;
            delete ldb;
        }
        return 1;
    }

    struct Ldbfile *HMManager::get_one_table(uint64_t filenum)
    {
        std::map<uint64_t, struct Ldbfile *>::iterator it;
        it = table_map_.find(filenum);
        if (it == table_map_.end())
        {
            printf("error:no find file:%ld\n", filenum);
            fflush(stdout);
            return NULL;
        }
        return it->second;
    }

    int64_t HMManager::get_table_size(uint64_t filenum)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        return get_one_table(filenum)->size;
    }

    bool HMManager::cmp(std::pair<struct Zonefile *, uint64_t> &a, std::pair<struct Zonefile *, uint64_t> &b)
    {
        return a.second < b.second;
    }

    bool HMManager::cmp_files(struct Ldbfile *a, struct Ldbfile *b)
    {
        return a->size > b->size;
    }

    // get table offsets
    void HMManager::get_table_offsets(std::vector<int64_t> &file_nums, std::vector<int64_t> &file_offsets)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        for (int i = 0; i < file_nums.size(); i++)
        {
            file_offsets.push_back(get_table_offset(file_nums[i]));
        }
    }

    int64_t HMManager::get_table_offset(int64_t file_num)
    {
        return get_one_table(file_num)->offset;
    }
    uint64_t HMManager::move_out_open_zone(int level, std::map<uint64_t, uint64_t> &move_out_zone)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::unique_lock<std::shared_mutex> lock(mutex_);
#endif
        uint64_t ret = 0;
        for (int i = 0; i < OPEN_ZONE_NUM; i++)
        {
            if (move_out_zone[open_zones[level][i]] == 1)
            {
                MyLog2("move out %d\n", open_zones[level][i]);
                open_zones[level][i] = zonenum_ + 1;
                ret += 1;
                if (i == open_zones_pointer[level])
                {
                    // choose a new pointer
                    open_zones_pointer[level] = OPEN_ZONE_NUM;
                    for (int j = 0; j < OPEN_ZONE_NUM; j++)
                    {
                        if (open_zones[level][j] != zonenum_ + 1)
                        {
                            open_zones_pointer[level] = j;
                            break;
                        }
                    }
                }
            }
        }
        return ret;
    }
    std::map<uint64_t, uint64_t>
    HMManager::get_zone_files_based_on_files(int level, std::map<uint64_t, uint64_t> &file_in_main_compaction_guards,
                                             std::map<uint64_t, uint64_t> &zone_in_compaction)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        std::map<uint64_t, uint64_t> ret;
        std::map<uint64_t, struct Ldbfile *>::iterator table_map_it;
        std::map<uint64_t, uint64_t>::iterator file_in_main_compaction_guards_it =
            file_in_main_compaction_guards.begin();
        MyLog2("may free these zones: ");
        for (; file_in_main_compaction_guards_it != file_in_main_compaction_guards.end();
             file_in_main_compaction_guards_it++)
        {
            table_map_it = table_map_.find(file_in_main_compaction_guards_it->first);
            if (table_map_it == table_map_.end())
            {
                assert(0);
                return ret;
            }
            MyLog2("%d ", table_map_it->second->zone);
            zone_in_compaction[table_map_it->second->zone] = 1;
            std::vector<struct Ldbfile *>::iterator ldb_it = zone_info_[table_map_it->second->zone]->ldb.begin();
            int64_t file_size_freed = 0;
            for (; ldb_it != zone_info_[table_map_it->second->zone]->ldb.end(); ldb_it++)
            {
                ret[(*ldb_it)->table] = 1;
            }
        }
        MyLog2("\n");
        return ret;
    }

    std::map<uint64_t, uint64_t> HMManager::get_new_zone_files_based_on_files(
        int level, std::map<uint64_t, uint64_t> &file_in_main_compaction_guards,
        std::map<uint64_t, uint64_t> &ignore_zone, std::map<uint64_t, uint64_t> &new_erase_zone)
    {
// std::map<uint64_t, uint64_t> HMManager::get_zone_files_based_on_files(int
// level, std::map<uint64_t,uint64_t>& file_in_main_compaction_guards) {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        std::map<uint64_t, uint64_t> ret;
        std::map<uint64_t, struct Ldbfile *>::iterator table_map_it;
        std::map<uint64_t, uint64_t>::iterator file_in_main_compaction_guards_it =
            file_in_main_compaction_guards.begin();
        MyLog2("may free these zones: ");
        for (; file_in_main_compaction_guards_it != file_in_main_compaction_guards.end();
             file_in_main_compaction_guards_it++)
        {
            table_map_it = table_map_.find(file_in_main_compaction_guards_it->first);
            if (table_map_it == table_map_.end())
            {
                assert(0);
                return ret;
            }
            if (ignore_zone[table_map_it->second->zone] == 1)
                continue;
            new_erase_zone[table_map_it->second->zone] = 1;
            std::vector<struct Ldbfile *>::iterator ldb_it = zone_info_[table_map_it->second->zone]->ldb.begin();
            int64_t file_size_freed = 0;
            for (; ldb_it != zone_info_[table_map_it->second->zone]->ldb.end(); ldb_it++)
            {
                ret[(*ldb_it)->table] = 1;
            }
        }
        MyLog2("\n");
        return ret;
    }

    //////statistics
    uint64_t HMManager::get_zone_num()
    {
        return valid_zone_; // todo not exactly 7
    }
    uint64_t HMManager::get_total_zone()
    {
        return restrict_zone_num_; // todo not exactly 7
    }
    uint64_t HMManager::open_zone_max_left(int level)
    {
        uint64_t max_left_size = 0;
        for (int i = 0; i < OPEN_ZONE_NUM; i++)
        {
            if (open_zones[level][i] >= zonenum_)
                continue;
            uint64_t zone_left_size =
                zone_[open_zones[level][i]]->hm_length -
                (zone_[open_zones[level][i]]->hm_write_pointer - zone_[open_zones[level][i]]->hm_start);
            if (zone_left_size > max_left_size)
            {
                max_left_size = zone_left_size;
            }
        }
        return max_left_size * dev_->get_library_unit();
    }

    uint64_t HMManager::get_file_zonenum(uint64_t filenum)
    {
        std::map<uint64_t, struct Ldbfile *>::iterator it = table_map_.find(filenum);
        if (it == table_map_.end())
        {
            printf("error:move file failed! no find file:%ld\n", filenum);
            return -1;
        }
        return it->second->zone;
    }

    void HMManager::print_space_amplication(uint64_t i)
    {
        printf("i=%ld sst %ld used_zone %ld invalid %ld level 0 invalidspace %ld\n", i, all_table_size, valid_zone_,
               invalid_space, invalid_cause_by_level0);
    }

    void HMManager::get_zone_info()
    {
        std::vector<struct Zonefile *>::iterator it;
        uint64_t total_zone_size = 0;
        uint64_t total_zone_sizev2 = 0;
        uint64_t total_zone_sizev3 = 0;
        uint64_t total_zone_size_per_level[7] = {0, 0, 0, 0, 0, 0, 0};
        uint64_t level_has_zone[7] = {0, 0, 0, 0, 0, 0, 0};
        std::string r[7];
        for (it = zone_info_.begin(); it != zone_info_.end(); it++)
        {
            struct Zonefile *zone = *it;
            if (zone != nullptr)
            {
                //(*it)->print_all_file();
                int level = zone->ldb[0]->level;
                r[level].append("zone ");
                AppendNumberTo(&r[level], zone->zone);
                r[level].append(": ");
                // zone: [sstnum,size]
                std::vector<struct Ldbfile *>::iterator file_it;
                for (file_it = zone->ldb.begin(); file_it != zone->ldb.end(); file_it++)
                {
                    r[level].append("[");
                    AppendNumberTo(&r[level], (*file_it)->table);
                    r[level].append(",");
                    AppendNumberTo(&r[level], (*file_it)->size);
                    r[level].append("]");
                    if (level != (*file_it)->level)
                    { // check error
                        for (int i = 0; i < 7; i++)
                        {
                            if (r[i].size() != 0)
                                MyLog2("level %d %s\n", i, r[i].c_str());
                            else
                                MyLog2("level %d zone: -1\n", i);
                        }
                        MyLog2("error level %d %d file %d\n", level, (*file_it)->level, (*file_it)->table);
                        assert(0);
                    }
                }

                total_zone_sizev2 += ((zone_[zone->zone]->hm_length -
                                       (zone_[zone->zone]->hm_write_pointer - zone_[zone->zone]->hm_start)) *
                                      dev_->get_library_unit());
                total_zone_size += (*it)->get_all_file_size();
                total_zone_sizev3 += (*it)->get_all_file_sizev2();
                total_zone_size_per_level[(*it)->ldb[0]->level] += (*it)->get_all_file_size();
                level_has_zone[(*it)->ldb[0]->level] += 1;
            }
        }
        /*
        for(int i = 0; i < 7; i++) {
            if(r[i].size() != 0)
                MyLog2("level %d %s\n", i, r[i].c_str());
            else
                MyLog2("level %d zone: -1\n", i);
        }
        */
        MyLog2("\ntotal size of sst %lu sst total size / used zone size %lf\n", total_zone_sizev3,
               (total_zone_sizev3 * 1.0) / (valid_zone_ * zonesize_ * 1.0));
        MyLog2("total size of space left %lu avg space left %lf\n", total_zone_sizev2,
               (total_zone_sizev2 * 1.0) / (valid_zone_ * zonesize_ * 1.0));
        MyLog2("total size of valid zone %lu avg zone space used %lf\n", total_zone_size,
               (total_zone_size * 1.0) / (valid_zone_ * zonesize_ * 1.0));
        for (int i = 0; i < 7; i++)
        {
            if (level_has_zone[i] != 0)
                MyLog2("level %lu has zone %lu avg usg %lf\n", i, level_has_zone[i],
                       total_zone_size_per_level[i] / double(level_has_zone[i] * zonesize_));
        }
    }

    uint64_t HMManager::get_guard_accross_zone(int level, const std::vector<uint64_t> files)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        std::set<uint64_t> zone_set;
        for (int i = 0; i < files.size(); i++)
        {
            zone_set.insert(get_file_zonenum(files[i]));
        }
        return zone_set.size();
    }

    void HMManager::get_all_info(uint64_t timestamp)
    {
#ifndef READ_WRITE_LOCK
        std::lock_guard<std::mutex> lk(mtx_);
#else
        std::shared_lock<std::shared_mutex> lock(mutex_);
#endif
        uint64_t disk_size = (get_zone_num()) * zone_[first_zonenum_]->hm_length;

        MyLog("\n%lu get all data!\n", timestamp);
        MyLog("table_all_size:%ld MB kv_read_block:%ld MB kv_store_block:%ld MB "
              "Move_count:%ld MB disk_size:%ld MB \n",
              all_table_size / (1024 * 1024), kv_read_block * ALIGN_BLOCK_SIZE / 1024 / 1024,
              kv_store_block * ALIGN_BLOCK_SIZE / 1024 / 1024, move_file_size / (1024 * 1024),
              disk_size * ALIGN_BLOCK_SIZE / 1024 / 1024);
        MyLog("read_time:%.1f s write_time(with GC):%.1f s GC_time:%.1f s "
              "find_time:%.1f s waste_find_time:%.1f s read:%.1f MB/s write:%.1f "
              "MB/s\n",
              1.0 * read_time * 1e-6, 1.0 * write_time * 1e-6, 1.0 * GC_time * 1e-6, 1.0 * find_time * 1e-6,
              1.0 * waste_find_time * 1e-6, (kv_read_block * ALIGN_BLOCK_SIZE / 1024.0 / 1024.0) / (read_time * 1e-6),
              (kv_store_block * ALIGN_BLOCK_SIZE / 1024.0 / 1024.0) / (write_time * 1e-6));
        for (int i = 0; i < 7; i++)
        {
            MyLog("level %d table_size: %ld ", i, table_size[i]);
        }
        MyLog("\n");
        // get_guard_info(timestamp);
        get_zone_info();
    }
    //////end

} // namespace leveldb
