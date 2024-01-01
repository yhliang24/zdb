// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_LIMIT_MACROS

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include <cmath>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "pebblesdb/env.h"
#include "pebblesdb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/timer.h"
#include "db/murmurhash3.h"
#include <inttypes.h>
#include <random>
#include "../hm/my_log.h"
#include "../hm/hm_status.h"
#ifdef TIMER_LOG_SEEK
#    define vvstart_timer(s) vset->timer->StartTimer(s)
#    define vvrecord_timer(s) vset->timer->Record(s)
#    define vvrecord_timer2(s, count) vset->timer->Record(s, count)
#else
#    define vvstart_timer(s)
#    define vvrecord_timer(s)
#    define vvrecord_timer2(s, count)
#endif

#ifdef TIMER_LOG
#    define start_timer(s1, s2, mtc) (mtc == 1) ? timer->StartTimer(s1) : timer->StartTimer(s2)
#    define vstart_timer(s1, s2, mtc) (mtc == 1) ? vset_->timer->StartTimer(s1) : vset_->timer->StartTimer(s2)

#    define record_timer(s1, s2, mtc) (mtc == 1) ? timer->Record(s1) : timer->Record(s2)
#    define vrecord_timer(s1, s2, mtc) (mtc == 1) ? vset_->timer->Record(s1) : vset_->timer->Record(s2)
#else
#    define start_timer(s1, s2, mtc)
#    define vstart_timer(s1, s2, mtc)

#    define record_timer(s1, s2, mtc)
#    define vrecord_timer(s1, s2, mtc)
#endif

#define FORCE_COMPACT_SIZE_THRESHOLD_IN_MB 10 * 1024
//#define FIX_SST_SIZE
namespace leveldb {

#ifdef FIX_SST_SIZE
    /*
    static double MaxBytesForLevel(unsigned level) {
      assert(level < leveldb::config::kNumLevels);
      static const double bytes[] = {64 * 1048576.0,
                                     128 * 1048576.0,
                                     512 * 1048576.0,
                                     4096 * 1048576.0,
                                     32768 * 1048576.0,
                                     262144 * 1048576.0,
                                     2097152 * 1048576.0};
      return bytes[level];
    }
    */
    static double MaxBytesForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        double result = 320. * 1048576.0;
        // 1 80, 2 800, 3 8G, 4 80G, 5 800G, 6 8T
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    static uint64_t MinFileSizeForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const uint64_t bytes[] = {61 * 1048576, 61 * 1048576, 61 * 1048576, 61 * 1048576,
                                         61 * 1048576, 61 * 1048576, 61 * 1048576};
        return bytes[level];
    }
    static uint64_t MaxFileSizeForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const uint64_t bytes[] = {61 * 1048576, 61 * 1048576, 61 * 1048576, 61 * 1048576,
                                         61 * 1048576, 61 * 1048576, 61 * 1048576};
        return bytes[level];
    }

    /*
    static uint64_t MinFileSizeForLevel(unsigned level) {
      assert(level < leveldb::config::kNumLevels);
      static const uint64_t bytes[] = {4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576};
      return bytes[level];
    }
    static uint64_t MaxFileSizeForLevel(unsigned level) {
      assert(level < leveldb::config::kNumLevels);
      static const uint64_t bytes[] = {4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576,
                                       4 * 1048576};
      return bytes[level];
    }
    */
    static double MaxBytesPerGuardForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        if (level == config::kNumLevels - 1)
            return MaxFileSizeForLevel(level) * (config::kMaxFilesPerGuardSentinel + 1);
        return MaxFileSizeForLevel(level) * (config::kMaxFilesPerGuardSentinel + 1);
    }

    static int MaxFilesPerGuardForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        if (level == config::kNumLevels - 1)
            return config::kMaxFilesPerGuardSentinel + 1;
        return config::kMaxFilesPerGuardSentinel + 1;
    }

    static double wa_factor(unsigned level)
    {
        if (level == 6)
            return 2.0;
        return 1.0;
    }

#else
    static double MaxBytesForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const double bytes[] = {64 * 1048576.0,    20 * 1048576.0,     200 * 1048576.0,    2000 * 1048576.0,
                                       20000 * 1048576.0, 200000 * 1048576.0, 2000000 * 1048576.0};
        return bytes[level];
    }

    static double MinBytesForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const double bytes[] = {16 * 1048576.0,   128 * 1048576.0,  256 * 1048576.0, 512 * 1048576.0,
                                       1024 * 1048576.0, 2048 * 1048576.0, 0 * 1048576.0};
        return bytes[level];
    }

    static double MaxBytesPerGuardForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const double bytes[] = {64 * 1048576.0,  128 * 1048576.0,  256 * 1048576.0, 512 * 1048576.0,
                                       512 * 1048576.0, 1024 * 1048576.0, 2048 * 1048576.0};
        return bytes[level];
    }

    static uint64_t MinFileSizeForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const uint64_t bytes[] = {1 * 1048576, 1 * 1048576,  1 * 1048576, 1 * 1048576,
                                         8 * 1048576, 16 * 1048576, 32 * 1048576};
        return bytes[level];
    }

    static uint64_t MaxFileSizeForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const uint64_t bytes[] = {64 * 1048576,  64 * 1048576,  64 * 1048576, 64 * 1048576,
                                         128 * 1048576, 256 * 1048576, 512 * 1048576};
        return bytes[level];
    }

    static int MaxFilesPerGuardForLevel(unsigned level)
    {
        assert(level < leveldb::config::kNumLevels);
        static const int num_files[] = {-1, -1, -1, -1, -1, -1, -1};
        return num_files[level];
    }
#endif

    static uint64_t MaxCompactionBytesForLevel(unsigned level)
    {
        return MaxFileSizeForLevel(level) * 16;
    }

    static int64_t TotalFileSize(const std::vector<FileMetaData *> &files)
    {
        int64_t sum = 0;
        for (size_t i = 0; i < files.size(); i++) {
            sum += files[i]->file_size;
        }
        return sum;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunsafe-loop-optimizations"

    Version::~Version()
    {
        assert(refs_ == 0);

        // Remove from linked list
        prev_->next_ = next_;
        next_->prev_ = prev_;

        // Drop references to files
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            for (size_t i = 0; i < files_[level].size(); i++) {
                FileMetaData *f = files_[level][i];
                assert(f->refs > 0);
                f->refs--;
                if (f->refs <= 0) {
                    delete f;
                }
            }
        }
    }

#pragma GCC diagnostic pop

    int FindFile(const InternalKeyComparator &icmp, const std::vector<FileMetaData *> &files, const Slice &key)
    {
        uint32_t left = 0;
        uint32_t right = files.size();
        while (left < right) {
            uint32_t mid = (left + right) / 2;
            const FileMetaData *f = files[mid];
            if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    struct SortBySmallestGuard {
        const InternalKeyComparator *internal_comparator;
        bool operator()(GuardMetaData *g1, GuardMetaData *g2) const
        {
            int r = internal_comparator->user_comparator()->Compare(g1->guard_key.user_key(), g2->guard_key.user_key());
            return (r < 0);
        }
    };

    // Function to sort guards using their metadata.
    bool guards_sorting_func(GuardMetaData *g1, GuardMetaData *g2)
    {
        const InternalKeyComparator *internal_comparator;
        return internal_comparator->Compare(g1->guard_key, g2->guard_key);
    }

    // Need to have all the guards sorted at this point.
    int FindGuard(const InternalKeyComparator &icmp, const std::vector<GuardMetaData *> &guards, const Slice &key)
    {
        if (guards.size() == 0) {
            return 0;
        }
        ParsedInternalKey parsed_key;
        ParseInternalKey(key, &parsed_key);
        uint32_t left = 0;
        uint32_t right = guards.size() - 1;
        while (left < right) {
            uint32_t mid = (left + right) / 2;

            // Handle end case specially to avoid infinite loop
            if (left + 1 == right) {
                if (icmp.user_comparator()->Compare(parsed_key.user_key, guards[right]->guard_key.user_key()) >= 0) {
                    return right;
                }
                return left;
            }

            const GuardMetaData *f = guards[mid];
            if (icmp.user_comparator()->Compare(parsed_key.user_key, f->guard_key.user_key()) < 0) {
                // Key is lesser than the guard key of mid. Therefore all
                // guards after "mid" are uninteresting.
                right = mid - 1;
            } else {
                // Key is greater than the guard key at mid. Therefore key might be present
                // in some guard after mid or in mid.
                left = mid;
            }
        }
        return (right < 0) ? 0 : right;
    }

    static bool AfterFile(const Comparator *ucmp, const Slice *user_key, const FileMetaData *f)
    {
        // NULL user_key occurs before all keys and is therefore never after *f
        return (user_key != NULL && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
    }

    static bool BeforeFile(const Comparator *ucmp, const Slice *user_key, const FileMetaData *f)
    {
        // NULL user_key occurs after all keys and is therefore never before *f
        return (user_key != NULL && ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
    }

    static bool AfterGuard(const Comparator *ucmp, const Slice *user_key, const GuardMetaData *g)
    {
        return (user_key != NULL && ucmp->Compare(*user_key, g->guard_key.user_key()) >= 0);
    }

    static bool BeforeGuard(const Comparator *ucmp, const Slice *user_key, const GuardMetaData *g)
    {
        return (user_key != NULL && ucmp->Compare(*user_key, g->guard_key.user_key()) < 0);
    }

    bool RangeFitsWithinAGuard(const InternalKeyComparator &icmp, const std::vector<GuardMetaData *> &guards,
                               const Slice *smallest_user_key, const Slice *largest_user_key)
    {
        const Comparator *ucmp = icmp.user_comparator();
        // TODO Optimize search by using binary search
        for (unsigned i = 0; i < guards.size(); i++) {
            if (i == 0) {
                if (BeforeGuard(ucmp, largest_user_key, guards[i])) {
                    return true;
                }
            } else if (i == guards.size() - 1) {
                if (AfterGuard(ucmp, smallest_user_key, guards[i])) {
                    return true;
                }
            } else {
                if (AfterGuard(ucmp, smallest_user_key, guards[i]) &&
                    BeforeGuard(ucmp, largest_user_key, guards[i + 1])) {
                    return true;
                }
            }
        }
        return false;
    }
    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp, bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &files, const Slice *smallest_user_key,
                               const Slice *largest_user_key)
    {
        const Comparator *ucmp = icmp.user_comparator();
        if (!disjoint_sorted_files) {
            // Need to check against all files
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                if (AfterFile(ucmp, smallest_user_key, f) || BeforeFile(ucmp, largest_user_key, f)) {
                    // No overlap
                } else {
                    return true;  // Overlap
                }
            }
            return false;
        }

        // Binary search over file list
        uint32_t index = 0;
        if (smallest_user_key != NULL) {
            // Find the earliest possible internal key for smallest_user_key
            InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
            index = FindFile(icmp, files, small.Encode());
        }

        if (index >= files.size()) {
            // beginning of range is after all files, so no overlap.
            return false;
        }

        return !BeforeFile(ucmp, largest_user_key, files[index]);
    }

    // An internal iterator.  For a given version/level pair, yields
    // information about the files in the level.  For a given entry, key()
    // is the largest key that occurs in the file, and value() is an
    // 16-byte value containing the file number and file size, both
    // encoded using EncodeFixed64.
    //
    // If num != 0, then do not call SeekToLast, Prev
    class Version::LevelFileNumIterator : public Iterator
    {
      public:
        LevelFileNumIterator(const InternalKeyComparator &icmp, const std::vector<FileMetaData *> *flist, uint64_t num)
            : icmp_(icmp), flist_(flist), index_(flist->size()),  // Marks as invalid
              number_(num), status_(Status::OK())
        {
        }
        virtual bool Valid() const
        {
            return index_ < flist_->size();
        }
        virtual void Seek(const Slice &target)
        {
            index_ = FindFile(icmp_, *flist_, target);
            Bump();
        }
        virtual void SeekToFirst()
        {
            index_ = 0;
            Bump();
        }
        virtual void SeekToLast()
        {
            index_ = flist_->empty() ? 0 : flist_->size() - 1;
            Bump();
        }
        virtual void Next()
        {
            assert(Valid());
            index_++;
            Bump();
        }
        virtual void Prev()
        {
            assert(Valid());
            assert(number_ == 0);
            if (index_ == 0) {
                index_ = flist_->size();  // Marks as invalid
            } else {
                index_--;
            }
        }
        Slice key() const
        {
            assert(Valid());
            return (*flist_)[index_]->largest.Encode();
        }
        Slice value() const
        {
            assert(Valid());
            EncodeFixed64(value_buf_, (*flist_)[index_]->number);
            EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
            return Slice(value_buf_, sizeof(value_buf_));
        }
        virtual const Status &status() const
        {
            return status_;
        }

      private:
        LevelFileNumIterator(const LevelFileNumIterator &);
        LevelFileNumIterator &operator=(const LevelFileNumIterator &);
        void Bump()
        {
            while (index_ < flist_->size() && (*flist_)[index_]->number < number_) {
                ++index_;
            }
        }
        const InternalKeyComparator icmp_;
        const std::vector<FileMetaData *> *const flist_;
        uint32_t index_;
        uint64_t number_;
        Status status_;

        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[16];
    };

    static Iterator *GetFileIterator(void *arg, const ReadOptions &options, const Slice &file_value)
    {
        TableCache *cache = reinterpret_cast<TableCache *>(arg);
        if (file_value.size() != 16) {
            return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
        } else {
            return cache->NewIterator(options, DecodeFixed64(file_value.data()), DecodeFixed64(file_value.data() + 8));
        }
    }

    class Version::LevelGuardNumIterator : public Iterator
    {
      public:
        LevelGuardNumIterator(const InternalKeyComparator &icmp, const std::vector<GuardMetaData *> *glist,
                              const std::vector<FileMetaData *> *sentinel_list,
                              const std::vector<FileMetaData *> *file_list, uint64_t num, Timer *timer)
            : icmp_(icmp), glist_(glist), sentinel_list_(sentinel_list), file_list_(file_list),
              index_(glist->size()),  // Marks as invalid
              number_(num), status_(Status::OK()), timer(timer)
        {
        }

        ~LevelGuardNumIterator()
        {
        }

        virtual bool Valid() const
        {
            return index_ < (int)glist_->size() && index_ >= -1;
        }

        virtual void Seek(const Slice &target)
        {
            if (glist_->size() == 0) {  // If there are no guards, setting index_ to -1
                index_ = -1;
            } else {
                index_ = FindGuard(icmp_, *glist_, target);
                if (index_ ==
                    0) {  // If the target is less than first guard key, setting index_ to -1 to point to sentinels
                    ParsedInternalKey parsed_key;
                    ParseInternalKey(target, &parsed_key);
                    if (icmp_.user_comparator()->Compare(parsed_key.user_key, glist_->at(0)->guard_key.user_key()) <
                        0) {
                        index_ = -1;
                    }
                }
            }
            Bump();
        }

        virtual void SeekToFirst()
        {
            index_ = -1;  // Since index of -1 will mean it is pointing to sentinel files
            Bump();
        }

        virtual void SeekToLast()
        {
            index_ = glist_->size() - 1;
            BumpReverse();
        }

        virtual void Next()
        {
            assert(Valid());
            index_++;
            Bump();
        }

        virtual void Prev()
        {
            assert(Valid());
            assert(number_ == 0);
            if (index_ == -1) {
                index_ = glist_->size();  // Marks as invalid
            } else {
                index_--;
                BumpReverse();
            }
        }

        Slice key() const
        {
            assert(Valid());
            if (index_ == -1) {
                InternalKey largest;
                // TODO Optimize getting largest value from list of sentinels
                if (sentinel_list_->size() > 0) {
                    largest = sentinel_list_->at(0)->largest;
                    for (int i = 1; i < sentinel_list_->size(); i++) {
                        if (icmp_.Compare(sentinel_list_->at(i)->largest, largest) > 0) {
                            largest = sentinel_list_->at(i)->largest;
                        }
                    }
                }
                return largest.Encode();
            } else {
                return (*glist_)[index_]->largest.Encode();
            }
        }

        Slice value() const
        {
            assert(Valid());
            std::vector<uint64_t> files;
            std::vector<uint64_t> file_sizes;
            uint64_t num_files = 0;
            if (index_ == -1) {
                for (int i = 0; i < sentinel_list_->size(); i++) {
                    if (sentinel_list_->at(i)->number > number_) {
                        files.push_back(sentinel_list_->at(i)->number);
                        file_sizes.push_back(sentinel_list_->at(i)->file_size);
                        num_files++;
                    }
                }
            } else {
                for (int i = 0; i < glist_->at(index_)->number_segments; i++) {
                    if (glist_->at(index_)->files[i] > number_) {
                        files.push_back(glist_->at(index_)->files[i]);
                        file_sizes.push_back(glist_->at(index_)->file_metas[i]->file_size);
                        num_files++;
                    }
                }
            }

            int num_bytes = 16 * num_files + 8;

            // For each file 16 bytes to store file number and file size and 8 more bytes to store the number of files
            // (first 8 bytes)
            EncodeFixed64(value_buf_, num_files);
            for (int i = 0; i < files.size(); i++) {
                EncodeFixed64(value_buf_ + i * 16 + 8, files[i]);
                EncodeFixed64(value_buf_ + i * 16 + 16, file_sizes[i]);
            }
            return Slice(value_buf_, num_bytes);
        }

        virtual const Status &status() const
        {
            return status_;
        }

      private:
        LevelGuardNumIterator(const LevelGuardNumIterator &);
        LevelGuardNumIterator &operator=(const LevelGuardNumIterator &);

        void Bump()
        {
            // Handle sentinel files --> Go to guard 0 if either sentinel has no files or all sentinel files are invalid
            // (> number)
            if (index_ == -1) {
                bool valid = false;
                for (int i = 0; i < sentinel_list_->size(); i++) {
                    if ((*sentinel_list_)[i]->number > number_) {
                        valid = true;
                        break;
                    }
                }
                if (!valid) {
                    index_++;
                } else {
                    return;
                }
            }
            while (index_ < glist_->size()) {
                bool valid = false;
                for (int i = 0; glist_->at(index_) != NULL && i < glist_->at(index_)->number_segments; i++) {
                    if (glist_->at(index_)->files[i] > number_) {
                        valid = true;
                        break;
                    }
                }
                if (valid) {
                    break;
                }
                ++index_;
            }
        }

        void BumpReverse()
        {
            // Handle sentinel files --> Go to guard 0 if either sentinel has no files or all sentinel files are invalid
            // (> number)
            while (index_ >= -1) {
                if (index_ == -1) {
                    for (int i = 0; i < sentinel_list_->size(); i++) {
                        if ((*sentinel_list_)[i]->number > number_) {
                            return;
                        }
                    }
                } else {
                    for (int i = 0; glist_->at(index_) != NULL && i < glist_->at(index_)->number_segments; i++) {
                        if (glist_->at(index_)->files[i] > number_) {
                            return;
                        }
                    }
                }
                index_--;
            }
        }

        const InternalKeyComparator icmp_;
        const std::vector<GuardMetaData *> *const glist_;
        const std::vector<FileMetaData *> *const sentinel_list_;
        const std::vector<FileMetaData *> *const file_list_;
        int index_;  // uint32 is not used because index_ can be -1 if it's pointing to sentinel files
        uint64_t number_;
        Status status_;
        Timer *timer;

        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[163840];
    };

#ifdef SEEK_PARALLEL
    static Iterator *GetGuardIteratorParallel(void *arg1, const void *arg2, void *arg3, unsigned level,
                                              const ReadOptions &options, const Slice &file_values)
    {
        TableCache *table_cache = reinterpret_cast<TableCache *>(arg1);
        const InternalKeyComparator *icmp = reinterpret_cast<const InternalKeyComparator *>(arg2);
        VersionSet *vset = reinterpret_cast<VersionSet *>(arg3);
        int num_files = (file_values.size() - 8) / 16;
        assert(num_files > 0);
        Iterator **list = new Iterator *[num_files];
        FileMetaData **file_meta_list = new FileMetaData *[num_files];
        pthread_t current_thread = vset->getEnv()->GetThreadId();

        assert(num_files == DecodeFixed64(file_values.data()));

        vvstart_timer(SEEK_TITERATOR_PARALLEL_TOTAL);
        vvstart_timer(SEEK_TITERATOR_PARALLEL_ASSIGN_THREADS);

        std::vector<int> titerator_thread_indices;
        int group_index;

        for (int i = 0; i < num_files; i++) {
            int file_num_pos = i * 16 + 8;
            int file_size_pos = file_num_pos + 8;
            uint64_t file_number = DecodeFixed64(file_values.data() + file_num_pos);
            uint64_t file_size = DecodeFixed64(file_values.data() + file_size_pos);
            file_meta_list[i] = table_cache->GetFileMetaDataForFile(file_number);
#    ifdef SEEK_TWO_WAY_SIGNAL
            // Get a group_index which is used to coordinate among the parallel threads being triggered
            // to maintain the count of pending threds, to signal back using cv etc.
            while (true) {
                group_index = vset->GetNextTableIteratorGroupIndex();
                if (group_index >= 0 && group_index < NUM_SEEK_THREADS) {
                    break;
                }
            }
#    endif

            int index;
            // BUSY WAITING !! To get the index of the idle thread.
            // Assuming that the number of threads will be sufficient enough to process any parallel request.
            while (true) {
                index = vset->GetIdleTableIteratorThreadIndex(current_thread);
                if (index >= 0 && index < NUM_SEEK_THREADS) {
                    break;
                }
            }
            titerator_thread_indices.push_back(index);
            vset->seek_titerator_file_numbers_[index] = file_number;
            vset->seek_titerator_file_sizes_[index] = file_size;
            vset->seek_titerator_read_options_[index] = options;
            vset->seek_titerator_result_iterators_[index] = &list[i];
            vset->seek_titerator_thread_status_[index] = ASSIGNED;

            vset->seek_titerator_assigned_group_id_[index] = group_index;

            vset->seek_titerator_threads_cv_[index].Signal();
        }

        vvrecord_timer(SEEK_TITERATOR_PARALLEL_ASSIGN_THREADS);

        // Signalling the seek threads to start.
        vvstart_timer(SEEK_TITERATOR_PARALLEL_SIGNAL_THREADS);
        while (true) {
            bool all_threads_started = true;
            for (int i = 0; i < titerator_thread_indices.size(); i++) {
                int index = titerator_thread_indices[i];
                if (vset->seek_titerator_thread_status_[index] == ASSIGNED &&
                    vset->seek_titerator_threads_current_workload[index] ==
                        current_thread) {  // This child thread has not yet received the signal from the parent
                    vset->seek_titerator_threads_cv_[index].Signal();
                    all_threads_started =
                        false;  // because we don't know if the child thread actually received the signal.
                }
            }
            if (all_threads_started) {
                break;
            }
        }
        vvrecord_timer(SEEK_TITERATOR_PARALLEL_SIGNAL_THREADS);

#    ifdef SEEK_TWO_WAY_SIGNAL
        while (vset->seek_titerator_group_num_pending_[group_index] > 0) {
            vset->seek_titerator_group_threads_cv_[group_index].Wait();
        }
        vset->seek_titerator_group_return_status_[group_index] =
            ACKNOWLEDGED;  // After this, the last thread will stop signalling.
        vset->seek_titerator_group_occupied_[group_index] =
            0;  // Free up this slot so that this can be used by other thread groups
#    else
        // BUSY WAITING here !
        vvstart_timer(SEEK_TITERATOR_PARALLEL_WAIT_FOR_THREADS);
        while (true) {
            bool completed = true;
            for (int i = 0; i < titerator_thread_indices.size(); i++) {
                int index = titerator_thread_indices[i];
                if (vset->seek_titerator_threads_current_workload[index] == current_thread) {
                    completed = false;
                    break;
                }
            }
            if (completed) {
                break;
            }
        }
        vvrecord_timer(SEEK_TITERATOR_PARALLEL_WAIT_FOR_THREADS);
#    endif
        vvrecord_timer(SEEK_TITERATOR_PARALLEL_TOTAL);
        Iterator *iterator = NewMergingIteratorForFiles(icmp, list, file_meta_list, num_files, icmp, vset, level);
        delete[] list;
        return iterator;
    }
#endif

    static Iterator *GetGuardIteratorSeq(void *arg1, const void *arg2, void *arg3, unsigned level,
                                         const ReadOptions &options, const Slice &file_values)
    {
        TableCache *table_cache = reinterpret_cast<TableCache *>(arg1);
        const InternalKeyComparator *icmp = reinterpret_cast<const InternalKeyComparator *>(arg2);
        VersionSet *vset = reinterpret_cast<VersionSet *>(arg3);
        int num_files = (file_values.size() - 8) / 16;
        assert(num_files > 0);
        Iterator **list = new Iterator *[num_files];
        FileMetaData **file_meta_list = new FileMetaData *[num_files];

        assert(num_files == DecodeFixed64(file_values.data()));
        vvstart_timer(SEEK_TITERATOR_SEQUENTIAL_TOTAL);
        for (int i = 0; i < num_files; i++) {
            int file_num_pos = i * 16 + 8;
            int file_size_pos = file_num_pos + 8;
            uint64_t file_number = DecodeFixed64(file_values.data() + file_num_pos);
            uint64_t file_size = DecodeFixed64(file_values.data() + file_size_pos);
            file_meta_list[i] = table_cache->GetFileMetaDataForFile(file_number);
            list[i] = table_cache->NewIterator(options, file_number, file_size);
        }
        vvrecord_timer2(SEEK_TITERATOR_SEQUENTIAL_TOTAL, num_files);
        Iterator *iterator = NewMergingIteratorForFiles(icmp, list, file_meta_list, num_files, icmp, vset, level);
        delete[] list;
        return iterator;
    }

    static Iterator *GetGuardIterator(void *arg1, const void *arg2, void *arg3, unsigned level,
                                      const ReadOptions &options, const Slice &file_values)
    {
        TableCache *table_cache = reinterpret_cast<TableCache *>(arg1);
        const InternalKeyComparator *icmp = reinterpret_cast<const InternalKeyComparator *>(arg2);
        VersionSet *vset = reinterpret_cast<VersionSet *>(arg3);

        int num_files = (file_values.size() - 8) / 16;
        assert(num_files > 0);

#ifdef SEEK_PARALLEL
        // As a rough estimate, use parallel threads for last level and sequential seeks for remaining levels
        if (num_files > 1 && level == config::kNumLevels - 1 && num_files <= NUM_SEEK_THREADS) {
            return GetGuardIteratorParallel(arg1, arg2, arg3, level, options, file_values);
        } else {
            return GetGuardIteratorSeq(arg1, arg2, arg3, level, options, file_values);
        }
#else
        return GetGuardIteratorSeq(arg1, arg2, arg3, level, options, file_values);
#endif
    }

    Iterator *Version::NewConcatenatingIterator(const ReadOptions &options, unsigned level, uint64_t num) const
    {
        return NewTwoLevelIteratorGuards(new LevelGuardNumIterator(vset_->icmp_, &guards_[level],
                                                                   &sentinel_files_[level], &files_[level], num,
                                                                   vset_->timer),
                                         &GetGuardIterator, vset_->table_cache_, &vset_->icmp_, vset_, level, options);
    }

    void Version::AddIterators(const ReadOptions &options, std::vector<Iterator *> *iters)
    {
        return AddSomeIterators(options, 0, iters);
    }

    void Version::AddSomeIterators(const ReadOptions &options, uint64_t num, std::vector<Iterator *> *iters)
    {
        // Merge all level zero files together since they may overlap
        for (size_t i = 0; i < files_[0].size(); i++) {
            iters->push_back(vset_->table_cache_->NewIterator(options, files_[0][i]->number, files_[0][i]->file_size));
        }

        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily.
        for (unsigned level = 1; level < config::kNumLevels; level++) {
            if (!files_[level].empty()) {
                iters->push_back(NewConcatenatingIterator(options, level, num));
            }
        }
    }

    void Version::AddSomeIteratorsGuards(const ReadOptions &options, uint64_t num, std::vector<Iterator *> *iters)
    {
        // Merge all level all files together since they may overlap
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            if (!files_[level].empty()) {
                iters->push_back(NewConcatenatingIterator(options, level, num));
            }
        }
    }

    // Callback from TableCache::Get()
    namespace {
    }
    static bool NewestFirst(FileMetaData *a, FileMetaData *b)
    {
        return a->number > b->number;
    }

    void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                     bool (*func)(void *, unsigned, FileMetaData *))
    {
        // TODO(sanjay): Change Version::Get() to use this function.
        const Comparator *ucmp = vset_->icmp_.user_comparator();

        // Search level-0 in order from newest to oldest.
        std::vector<FileMetaData *> tmp;
        tmp.reserve(files_[0].size());
        for (uint32_t i = 0; i < files_[0].size(); i++) {
            FileMetaData *f = files_[0][i];
            if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
                ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                tmp.push_back(f);
            }
        }
        if (!tmp.empty()) {
            std::sort(tmp.begin(), tmp.end(), NewestFirst);
            for (uint32_t i = 0; i < tmp.size(); i++) {
                if (!(*func)(arg, 0, tmp[i])) {
                    return;
                }
            }
        }

        // Search other levels.
        for (unsigned level = 1; level < config::kNumLevels; level++) {
            size_t num_files = files_[level].size();
            if (num_files == 0)
                continue;

            // Binary search to find earliest index whose largest key >= internal_key.
            uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
            if (index < num_files) {
                FileMetaData *f = files_[level][index];
                if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
                    // All of "f" is past any data for user_key
                } else {
                    if (!(*func)(arg, level, f)) {
                        return;
                    }
                }
            }
        }
    }

    Status Version::Get(const ReadOptions &options, const LookupKey &k, std::string *value, GetStats *stats)
    {
#ifdef READ_PARALLEL
        pthread_t current_thread = vset_->env_->GetThreadId();
#endif
        Slice ikey = k.internal_key();
        Slice user_key = k.user_key();
        const Comparator *ucmp = vset_->icmp_.user_comparator();
        Status s;

        stats->seek_file = NULL;
        stats->seek_file_level = -1;
        FileMetaData *last_file_read = NULL;
        int last_file_read_level = -1;

        num_files_read = 0;
        // We can search level-by-level since entries never hop across
        // levels.  Therefore we are guaranteed that if we find data
        // in an smaller level, later levels are irrelevant.
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            std::vector<FileMetaData *> tmp2;
            size_t num_files = files_[level].size();
            size_t num_guards = guards_[level].size();
            if (num_files == 0) {
                continue;
            }

            // Get the list of files to search in this level
            FileMetaData *const *files = &files_[level][0];

            vstart_timer(GET_FIND_GUARD, BEGIN, 1);
            // Get the guard_index in whose range the key lies in
            uint32_t guard_index = FindGuard(vset_->icmp_, guards_[level], ikey);
            vrecord_timer(GET_FIND_GUARD, BEGIN, 1);

            // Once we find the guard, we need to do binary searches inside
            // the files of each guard.
            GuardMetaData *g;
            if (num_guards > 0) {
                g = guards_[level][guard_index];
            }

            // If the guard chosen is the first in the level and if the lookup key is less
            // than the guard key of the first guard, it means that the key might be present in one
            // of the sentinel files of that level.

            vstart_timer(GET_FIND_LIST_OF_FILES, BEGIN, 1);
            if (num_guards == 0       // If there are no guards in the level, look at the sentinel files
                || (guard_index == 0  // If there are guards in the level and guard_index is 0, key can either be in
                                      // sentinel or in the first(0-index) guard
                    && num_guards > 0 && ucmp->Compare(g->guard_key.user_key(), user_key) > 0)) {
                vstart_timer(GET_CHECK_SENTINEL_FILES, BEGIN, 1);

                std::vector<FileMetaData *> files_in_sentinel = sentinel_files_[level];
                for (size_t i = 0; i < sentinel_files_[level].size(); i++) {
                    FileMetaData *f = sentinel_files_[level][i];
                    // Optimization: Adding only the files where the required key lies between smallest and largest
                    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
                        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                        tmp2.push_back(f);
                    }
                }
                vrecord_timer(GET_CHECK_SENTINEL_FILES, BEGIN, 1);

                vstart_timer(GET_SORT_SENTINEL_FILES, BEGIN, 1);
                // Sorting by newest first that will handle updates
                // TODO: If this sorting is moved to LogAndApply, sorting need not be done during read
                std::sort(tmp2.begin(), tmp2.end(), NewestFirst);
                files = &tmp2[0];
                num_files = tmp2.size();
                vrecord_timer(GET_SORT_SENTINEL_FILES, BEGIN, 1);
            } else if (g->number_segments > 0) {
                vstart_timer(GET_CHECK_GUARD_FILES, BEGIN, 1);
                for (size_t i = 0; i < g->number_segments; i++) {
                    FileMetaData *f = g->file_metas[i];
                    // Optimization: Adding only the files where the required key lies between smallest and largest
                    if (f != NULL && ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
                        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                        tmp2.push_back(f);
                    }
                }
                vrecord_timer(GET_CHECK_GUARD_FILES, BEGIN, 1);

                vstart_timer(GET_SORT_GUARD_FILES, BEGIN, 1);
                // Sorting by newest first that will handle updates
                std::sort(tmp2.begin(), tmp2.end(), NewestFirst);
                files = &tmp2[0];
                num_files = tmp2.size();
                vrecord_timer(GET_SORT_GUARD_FILES, BEGIN, 1);
            } else {
                num_files = 0;
                files = NULL;
            }
            vrecord_timer(GET_FIND_LIST_OF_FILES, BEGIN, 1);

#ifndef READ_PARALLEL
            for (uint32_t i = 0; i < num_files; ++i) {
                if (last_file_read != NULL && stats->seek_file == NULL) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    stats->seek_file = last_file_read;
                    stats->seek_file_level = last_file_read_level;
                }

                // Iterate through the files and do binary search.
                FileMetaData *f = files[i];
                last_file_read = f;
                last_file_read_level = level;

                bool key_may_match = true;

#    ifdef FILE_LEVEL_FILTER
                std::string *filter_string = vset_->file_level_bloom_filter[f->number];
                if (filter_string != NULL) {
                    vstart_timer(GET_FILE_LEVEL_FILTER_CHECK, BEGIN, 1);
                    Slice filter_slice = Slice(filter_string->data(), filter_string->size());
                    key_may_match = vset_->options_->filter_policy->KeyMayMatch(ikey, filter_slice);
                    vrecord_timer(GET_FILE_LEVEL_FILTER_CHECK, BEGIN, 1);
                    if (!key_may_match) {
                        continue;
                    }
                }
#    endif

                Saver saver;
                saver.state = kNotFound;
                saver.ucmp = ucmp;
                saver.user_key = user_key;
                saver.value = value;

                vstart_timer(GET_TABLE_CACHE_GET, BEGIN, 1);
                s = vset_->table_cache_->Get(options, f->number, f->file_size, ikey, &saver, SaveValue, vset_->timer);
                vrecord_timer(GET_TABLE_CACHE_GET, BEGIN, 1);
                num_files_read++;

                if (!s.ok()) {
                    return s;
                }

                switch (saver.state) {
                    case kNotFound:
                        break;  // Keep searching in other files
                    case kFound:
                        MyLog2("one get read %ld files\n", num_files_read);
                        return Status::OK();
                    case kDeleted:
                        s = Status::NotFound(Slice());  // Use empty error message for speed
                        return s;
                    case kCorrupt:
                        s = Status::Corruption("corrupted key for ", user_key);
                        return s;
                    default:
                        break;
                }
            }
#else
            std::vector<Saver *> savers;
            std::vector<pthread_t> pthreads;

            std::vector<int> read_thread_indices;
            int num_concurrent_reads = num_files;
            for (uint32_t i = 0; i < num_files; ++i) {
                if (last_file_read != NULL && stats->seek_file == NULL) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    stats->seek_file = last_file_read;
                    stats->seek_file_level = last_file_read_level;
                }

                // Iterate through the files and do binary search.
                FileMetaData *f = files[i];
                last_file_read = f;
                last_file_read_level = level;

                Saver saver;
                saver.state = kNotFound;
                saver.ucmp = ucmp;
                saver.user_key = user_key;
                std::string tmp_value;
                saver.value = &tmp_value;
                savers.push_back(&saver);

                vstart_timer(GET_TABLE_CACHE_GET, BEGIN, 1);
                if (num_files == 1) {
                    vstart_timer(GET_TABLE_CACHE_NUM_DIRECT_CALLS, BEGIN, 1);
                    s = vset_->table_cache_->Get(options, f->number, f->file_size, ikey, &saver, SaveValue,
                                                 vset_->timer);
                    vrecord_timer(GET_TABLE_CACHE_NUM_DIRECT_CALLS, BEGIN, 1);
                } else {
                    int index;
                    // BUSY WAITING !! To get the index of the idle thread. Optimize !!
                    vstart_timer(GET_TABLE_CACHE_NUM_THREADS_SIGNALLED, BEGIN, 1);
                    while (true) {
                        index = vset_->GetReadIdleThreadIndex(current_thread);
                        if (index >= 0 && index < NUM_READ_THREADS) {
                            break;
                        }
                    }
                    read_thread_indices.push_back(index);
                    vset_->saver_values_from_read_[index] = &saver;
                    vset_->file_numbers_to_read_[index] = f->number;
                    vset_->file_sizes_to_read_[index] = f->file_size;
                    vset_->internal_keys_to_read_[index] = ikey;
                    vset_->read_options_[index] = options;
                    vrecord_timer(GET_TABLE_CACHE_NUM_THREADS_SIGNALLED, BEGIN, 1);
                }
                vrecord_timer(GET_TABLE_CACHE_GET, BEGIN, 1);
            }

            // TODO: Change value returning method since we are concurrently writing to the same value* in parallel
            // reads. Signalling the read threads to start.
            vstart_timer(GET_TABLE_CACHE_SIGNAL_READ_THREADS, BEGIN, 1);
            for (int i = 0; i < read_thread_indices.size(); i++) {
                vset_->read_threads_cv_[read_thread_indices[i]].Signal();
            }
            vrecord_timer(GET_TABLE_CACHE_SIGNAL_READ_THREADS, BEGIN, 1);
            // BUSY WAITING ! TODO: Check the amount of time spent here and optimize !

            vstart_timer(GET_TABLE_CACHE_WAIT_FOR_READ_THREADS, BEGIN, 1);
            while (true) {
                bool completed = true;
                for (int i = 0; i < read_thread_indices.size(); i++) {
                    if (vset_->read_threads_current_workload[read_thread_indices[i]] == current_thread) {
                        // If the read thread missed the previous Signal somehow, signal again.
                        if (vset_->thread_status_[read_thread_indices[i]] == IDLE) {
                            vset_->read_threads_cv_[read_thread_indices[i]].Signal();
                        }
                        completed = false;
                        break;
                    }
                }
                if (completed) {
                    break;
                }
            }
            vrecord_timer(GET_TABLE_CACHE_WAIT_FOR_READ_THREADS, BEGIN, 1);

            for (int i = 0; i < savers.size(); i++) {
                // TODO Return status from table_cache_->Get() is lost in this model, need to incorporate that.
                /*
                if (!s.ok()) {
                  return s;
                }
                */
                switch (savers[i]->state) {
                    case kNotFound:
                        break;  // Keep searching in other files
                    case kFound:
                        value->assign(*(savers[i]->value));
                        return Status::OK();
                    case kDeleted:
                        s = Status::NotFound(Slice());  // Use empty error message for speed
                        return s;
                    case kCorrupt:
                        s = Status::Corruption("corrupted key for ", user_key);
                        return s;
                    default:
                        break;
                }
            }
#endif
        }

        return Status::NotFound(Slice());  // Use an empty error message for speed
    }

    bool Version::UpdateStats(const GetStats &stats)
    {
        FileMetaData *f = stats.seek_file;
        if (f != NULL) {
            f->allowed_seeks--;
            if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
                file_to_compact_ = f;
                file_to_compact_level_ = stats.seek_file_level;
                return true;
            }
        }
        return false;
    }

    bool Version::RecordReadSample(Slice internal_key)
    {
        ParsedInternalKey ikey;
        if (!ParseInternalKey(internal_key, &ikey)) {
            return false;
        }
        struct State {
            GetStats stats;  // Holds first matching file
            int matches;

            static bool Match(void *arg, unsigned level, FileMetaData *f)
            {
                State *state = reinterpret_cast<State *>(arg);
                state->matches++;
                if (state->matches == 1) {
                    // Remember first match.
                    state->stats.seek_file = f;
                    state->stats.seek_file_level = level;
                }
                // We can stop iterating once we have a second match.
                return state->matches < 2;
            }
        };

        State state;
        state.matches = 0;
        ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

        // Must have at least two matches since we want to merge across
        // files. But what if we have a single file that contains many
        // overwrites and deletions?  Should we have another mechanism for
        // finding such files?
        if (state.matches >= 2) {
            // 1MB cost is about 1 seek (see comment in Builder::Apply).
            return UpdateStats(state.stats);
        }
        return false;
    }

    bool Version::RecordGuardReadSample(Slice internal_key)
    {
        ParsedInternalKey ikey;
        if (!ParseInternalKey(internal_key, &ikey)) {
            return false;
        }
        if (guard_to_compact_level_ != -1 || sen_file_to_compact_level_ != -1)
            return true;
        const Comparator *ucmp = vset_->icmp_.user_comparator();

        for (unsigned level = 1; level < config::kNumLevels; level++) {
            size_t num_files = files_[level].size();
            size_t num_guards = guards_[level].size();
            if (num_files == 0) {
                continue;
            }
            uint32_t guard_index = FindGuard(vset_->icmp_, guards_[level], internal_key);
            GuardMetaData *g;
            if (num_guards > 0) {
                g = guards_[level][guard_index];
            }
            if (num_guards == 0 ||
                (guard_index == 0 && num_guards > 0 && ucmp->Compare(g->guard_key.user_key(), ikey.user_key) > 0)) {
                if (sentinel_files_[level].size() > SEEK_COM) {
                    sentinel_files_allow_seeks[level]--;
                    MyLog2("allow l%d senfile seek %d\n", level, sentinel_files_allow_seeks[level]);
                    if (sentinel_files_allow_seeks[level] <= 0) {
                        sen_file_to_compact_level_ = level;
                        guard_to_compact_ = NULL;
                        guard_to_compact_level_ = -1;
                        return true;
                    }
                }
            } else if (g->file_metas.size() > SEEK_COM) {
                g->guard_allow_seeks--;
                MyLog2("allow l%d guard %s seek %d\n", level, g->guard_key.DebugString().c_str(), g->guard_allow_seeks);
                if (g->guard_allow_seeks <= 0) {
                    sen_file_to_compact_level_ = -1;
                    guard_to_compact_ = g;
                    guard_to_compact_level_ = level;
                    return true;
                }
            }
        }
        return false;
    }

    void Version::Ref()
    {
        ++refs_;
    }

    void Version::Unref()
    {
        assert(this != &vset_->dummy_versions_);
        assert(refs_ >= 1);
        --refs_;
        if (refs_ == 0) {
            delete this;
        }
    }

    bool Version::OverlapInLevel(unsigned level, const Slice *smallest_user_key, const Slice *largest_user_key)
    {
        return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level], smallest_user_key, largest_user_key);
    }

    bool Version::OverlapInLevelGuards(unsigned level, const Slice *smallest_user_key, const Slice *largest_user_key)
    {
        return !RangeFitsWithinAGuard(vset_->icmp_, guards_[level], smallest_user_key, largest_user_key);
    }

    // Store in "*inputs" all files in "level" that overlap [begin,end]
    void Version::GetOverlappingInputs(unsigned level, const InternalKey *begin, const InternalKey *end,
                                       std::vector<FileMetaData *> *inputs)
    {
        assert(level < config::kNumLevels);
        inputs->clear();
        Slice user_begin, user_end;
        if (begin != NULL) {
            user_begin = begin->user_key();
        }
        if (end != NULL) {
            user_end = end->user_key();
        }
        const Comparator *user_cmp = vset_->icmp_.user_comparator();
        for (size_t i = 0; i < files_[level].size();) {
            FileMetaData *f = files_[level][i++];
            const Slice file_start = f->smallest.user_key();
            const Slice file_limit = f->largest.user_key();
            if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
                // "f" is completely before specified range; skip it
            } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
                // "f" is completely after specified range; skip it
            } else {
                inputs->push_back(f);
                // The files within each guard may overlap each other.  So check if the newly
                // added file has expanded the range.  If so, restart search.
                if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
                    user_begin = file_start;
                    inputs->clear();
                    i = 0;
                } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
                    user_end = file_limit;
                    inputs->clear();
                    i = 0;
                }
            }
        }
    }

    void Version::GetOverlappingInputsGuards(unsigned level, const InternalKey *begin, const InternalKey *end,
                                             std::vector<FileMetaData *> *inputs,
                                             std::vector<GuardMetaData *> *guard_inputs,
                                             std::vector<FileMetaData *> *sentinel_inputs)
    {
        assert(level < config::kNumLevels);
        inputs->clear();
        guard_inputs->clear();
        sentinel_inputs->clear();

        Slice user_begin, user_end;
        if (begin != NULL) {
            user_begin = begin->user_key();
        }
        if (end != NULL) {
            user_end = end->user_key();
        }
        const Comparator *user_cmp = vset_->icmp_.user_comparator();
        bool add_sentinel_files = false;
        for (size_t i = 0; i < sentinel_files_[level].size(); i++) {
            FileMetaData *f = sentinel_files_[level][i];
            const Slice file_start = f->smallest.user_key();
            const Slice file_limit = f->largest.user_key();

            if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
                // "f" is completely before specified range; skip it
            } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
                // "f" is completely after specified range; skip it
            } else {
                add_sentinel_files = true;
                break;
            }
        }
        if (add_sentinel_files) {
            for (size_t i = 0; i < sentinel_files_[level].size(); i++) {
                sentinel_inputs->push_back(sentinel_files_[level][i]);
                inputs->push_back(sentinel_files_[level][i]);
            }
        }
        for (size_t i = 0; i < guards_[level].size(); i++) {
            GuardMetaData *g = guards_[level][i];
            const Slice guard_start = g->smallest.user_key();
            const Slice guard_limit = g->largest.user_key();

            if (begin != NULL && user_cmp->Compare(guard_limit, user_begin) < 0) {
                // "g" is completely before specified range; skip it
            } else if (end != NULL && user_cmp->Compare(guard_start, user_end) > 0) {
                // "g" is completely after specified range; skip it
            } else {
                for (size_t j = 0; j < g->number_segments; j++) {
                    inputs->push_back(g->file_metas[j]);
                }
                guard_inputs->push_back(g);
            }
        }
    }

    struct BySmallestGuard {
        const InternalKeyComparator *internal_comparator;
        bool operator()(GuardMetaData *g1, GuardMetaData *g2) const
        {
            int r = internal_comparator->user_comparator()->Compare(g1->guard_key.user_key(), g2->guard_key.user_key());
            return (r < 0);
        }
    };

    void Version::AddDeltaGuardsToEdit(VersionEdit *edit, std::set<int> level_to_load_complete_guards)
    {
        assert(edit != NULL);

        std::set<GuardMetaData *, BySmallestGuard> new_guards_set;
        BySmallestGuard g_cmp;
        g_cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < config::kNumLevels; level++) {
            if (level_to_load_complete_guards.count(level) > 0) {
                // Insert into new set only the guards that were inserted since creation of the last version (unsorted
                // guards)
                for (int i = num_complete_guards_[level]; i < complete_guards_[level].size(); i++) {
                    new_guards_set.insert(complete_guards_[level][i]);
                }
                int i = 0, j = 0;
                while (i < num_complete_guards_[level] && j < guards_[level].size()) {
                    int compare = vset_->icmp_.user_comparator()->Compare(
                        complete_guards_[level][i]->guard_key.user_key(), guards_[level][j]->guard_key.user_key());
                    if (compare == 0) {
                        i++;
                        j++;
                    } else if (compare < 0) {
                        edit->AddGuardFromExisting(level, complete_guards_[level][i]);
                        i++;
                    } else {
                        // Ideally we shouldn't reach here since both guards and complete_guards are sorted.
                        j++;  // To prevent infinite loop
                    }
                }
                while (i < num_complete_guards_[level]) {
                    edit->AddGuardFromExisting(level, complete_guards_[level][i]);
                }

                std::set<GuardMetaData *, BySmallestGuard>::iterator set_it = new_guards_set.begin();
                std::set<GuardMetaData *, BySmallestGuard>::iterator set_end = new_guards_set.begin();
                j = 0;
                while (j < guards_[level].size() && set_it != set_end) {
                    int compare = vset_->icmp_.user_comparator()->Compare((*set_it)->guard_key.user_key(),
                                                                          guards_[level][j]->guard_key.user_key());
                    if (compare == 0) {
                        set_it++;
                        j++;
                    } else if (compare < 0) {
                        edit->AddGuardFromExisting(level, (*set_it));
                        set_it++;
                    } else {
                        j++;
                    }
                }
                while (set_it != set_end) {
                    edit->AddGuardFromExisting(level, (*set_it));
                }
            }
        }
    }

    std::string Version::DebugString() const
    {
        std::string r;
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            // E.g.,
            //   --- level 1 ---
            //   17:123['a' .. 'd']
            //   20:43['e' .. 'g']
            r.append("--------------------- level ");
            AppendNumberTo(&r, level);
            r.append(" ---------------------\n");

            r.append(" compaction_score_of_level: ");
            AppendDoubleTo(&r, compaction_scores_[level]);
            r.append("\n");

            r.append(" sentinel_compaction_score_of_level: ");
            AppendDoubleTo(&r, sentinel_compaction_scores_[level]);
            r.append("\n");

            r.append(" total file size in this level: ");
            AppendNumberTo(&r, TotalFileSize(files_[level]));
            r.append("\n");

            // Appending file information
            const std::vector<FileMetaData *> &files = files_[level];
            r.append(" ------ Files ------\n");
            r.append(" num_files: ");
            AppendNumberTo(&r, files.size());
            r.append("\n");
            for (size_t i = 0; i < files.size(); i++) {
                r.push_back(' ');
                AppendNumberTo(&r, files[i]->number);
                r.push_back(':');
                AppendNumberTo(&r, files[i]->file_size);
                r.append("[");
                r.append(files[i]->smallest.DebugString());
                r.append(" .. ");
                r.append(files[i]->largest.DebugString());
                r.append("]\n");
            }

            // Appending sentinel information
            const std::vector<FileMetaData *> &sentinel_files = sentinel_files_[level];
            r.append(" ------ Sentinel files ------\n");
            r.append(" num_sentinel_files: ");
            AppendNumberTo(&r, sentinel_files.size());
            r.append("\n");
            for (size_t i = 0; i < sentinel_files.size(); i++) {
                r.push_back(' ');
                AppendNumberTo(&r, sentinel_files[i]->number);
                r.push_back(':');
                AppendNumberTo(&r, sentinel_files[i]->file_size);
                r.append("[");
                r.append(sentinel_files[i]->smallest.DebugString());
                r.append(" .. ");
                r.append(sentinel_files[i]->largest.DebugString());
                r.append("]\n");
            }

            // Appending guard information
            const std::vector<GuardMetaData *> &guards = guards_[level];
            r.append(" ------ Guards ------\n");
            r.append(" num_guards: ");
            AppendNumberTo(&r, guards.size());
            r.append("\n");
            r.append(" num_guard_files_in_level: ");
            AppendNumberTo(&r, NumGuardFiles(level));
            r.append("\n");
            std::vector<double> guard_compaction_scores = guard_compaction_scores_[level];
            for (size_t i = 0; i < guards.size(); i++) {
                GuardMetaData *g = guards[i];
                r.push_back(' ');
                r.append("guard_key: ").append(g->guard_key.DebugString()).append("\n");
                r.append(" guard_compaction_score: ");
                const std::string guard_user_key(g->guard_key.user_key().ToString());
                AppendDoubleTo(&r, guard_compaction_scores[i]);
                r.append("\n");
                r.append(" guard_range: [");
                r.append(g->smallest.DebugString()).append(" .. ").append(g->largest.DebugString()).append("]\n");
                r.append(" num_segments: ");
                AppendNumberTo(&r, g->number_segments);
                r.append("\n");
                r.append(" files: [");
                for (size_t i = 0; i < g->files.size(); i++) {
                    if (i > 0) {
                        r.push_back(',');
                    }
                    AppendNumberTo(&r, g->files[i]);
                }
                r.append("]\n");
            }

            // Appending complete guard information
            const std::vector<GuardMetaData *> &complete_guards = complete_guards_[level];
            r.append(" ------ Complete Guards ------\n");
            r.append(" num_guards: ");
            AppendNumberTo(&r, complete_guards.size());
            r.append("\n");
            r.append(" num_complete_guard_files_in_level: ");
            AppendNumberTo(&r, NumCompleteGuardFiles(level));
            r.append("\n");

            // For readability. will be removed.
            if (level >= 4) {
                continue;
            }
            for (size_t i = 0; i < complete_guards.size(); i++) {
                GuardMetaData *g = complete_guards[i];
                r.push_back(' ');
                r.append("guard_key: ").append(g->guard_key.DebugString()).append("\n");
                r.append(" num_segments: ");
                AppendNumberTo(&r, g->number_segments);
                r.append("\n");
            }
        }
        return r;
    }

    std::string Version::DebugString2() const
    {
        std::string r;
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            // E.g.,
            //   --- level 1 ---
            //   17:123['a' .. 'd']
            //   20:43['e' .. 'g']
            r.append("--------------------- level ");
            AppendNumberTo(&r, level);
            r.append(" ---------------------\n");

            r.append(" compaction_score_of_level: ");
            AppendDoubleTo(&r, compaction_scores_[level]);
            r.append("\n");

            r.append(" sentinel_compaction_score_of_level: ");
            AppendDoubleTo(&r, sentinel_compaction_scores_[level]);
            r.append("\n");

            r.append(" total file size in this level: ");
            AppendNumberTo(&r, TotalFileSize(files_[level]));
            r.append("\n");

            // Appending file information
            const std::vector<FileMetaData *> &files = files_[level];
            r.append(" ------ Files ------\n");
            AppendNumberTo(&r, level);
            r.append("\n");
            r.append(" num_files: ");
            AppendNumberTo(&r, files.size());
            r.append("\n");

            // Appending sentinel information
            const std::vector<FileMetaData *> &sentinel_files = sentinel_files_[level];
            r.append(" ------ Sentinel files ------\n");
            AppendNumberTo(&r, level);
            r.append("\n");
            r.append(" num_sentinel_files: ");
            AppendNumberTo(&r, sentinel_files.size());
            r.append("\n");

            // Appending guard information
            const std::vector<GuardMetaData *> &guards = guards_[level];
            r.append(" ------ Guards ------\n");
            AppendNumberTo(&r, level);
            r.append("\n");
            r.append(" num_guards: ");
            AppendNumberTo(&r, guards.size());
            r.append("\n");
            r.append(" num_guard_files_in_level: ");
            AppendNumberTo(&r, NumGuardFiles(level));
            r.append("\n");

            // Appending complete guard information
            const std::vector<GuardMetaData *> &complete_guards = complete_guards_[level];
            r.append(" ------ Complete Guards in level------");
            AppendNumberTo(&r, level);
            r.append("\n");
            r.append(" num_guards: ");
            AppendNumberTo(&r, complete_guards.size());
            r.append("\n");
            r.append(" num_complete_guard_files_in_level: ");
            AppendNumberTo(&r, NumCompleteGuardFiles(level));
            r.append("\n");
        }
        return r;
    }

    // A helper class so we can efficiently apply a whole sequence
    // of edits to a particular state without creating intermediate
    // Versions that contain full copies of the intermediate state.
    class VersionSet::Builder
    {
      private:
        Builder(const Builder &);
        Builder &operator=(const Builder &);
        // Helper to sort by v->files_[file_number].smallest
        struct BySmallestKey {
            const InternalKeyComparator *internal_comparator;

            bool operator()(FileMetaData *f1, FileMetaData *f2) const
            {
                int r = internal_comparator->Compare(f1->smallest, f2->smallest);
                if (r != 0) {
                    return (r < 0);
                } else {
                    // Break ties by file number
                    return (f1->number < f2->number);
                }
            }
        };

        struct BySmallestGuard {
            const InternalKeyComparator *internal_comparator;
            bool operator()(GuardMetaData *g1, GuardMetaData *g2) const
            {
                int r =
                    internal_comparator->user_comparator()->Compare(g1->guard_key.user_key(), g2->guard_key.user_key());
                return (r < 0);
            }
        };

        struct BySmallestInternalKey {
            const InternalKeyComparator *internal_comparator;
            bool operator()(InternalKey k1, InternalKey k2) const
            {
                int r = internal_comparator->Compare(k1, k2);
                return (r < 0);
            }
        };

        typedef std::set<FileMetaData *, BySmallestKey> FileSet;
        typedef std::set<GuardMetaData *, BySmallestGuard> GuardSet;
        typedef std::set<InternalKey, BySmallestInternalKey> KeySet;
        struct LevelState {
            LevelState()
                : deleted_files(), added_files(), deleted_guards(), added_guards(), added_complete_guards(),
                  is_rewrite(0)
            {
            }
            std::set<uint64_t> deleted_files;
            GuardSet *added_guards;
            GuardSet *added_complete_guards;
            KeySet *deleted_guards;
            FileSet *added_files;
            // add the guard set that involves in whole compaction
            std::vector<GuardMetaData *> full_guard_added_in_compaction;
            bool is_rewrite;
            double dup_ratio;
            double before_size;
            double after_size;
            PiggyMain *pm;

          private:
            LevelState(const LevelState &);
            LevelState &operator=(const LevelState &);
        };

        VersionSet *vset_;
        Version *base_;
        LevelState levels_[config::kNumLevels];

      public:
        // Initialize a builder with the files from *base and other info from *vset
        Builder(VersionSet *vset, Version *base) : vset_(vset), base_(base)
        {
            base_->Ref();
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            BySmallestGuard g_cmp;
            g_cmp.internal_comparator = &vset_->icmp_;
            BySmallestInternalKey k_cmp;
            k_cmp.internal_comparator = &vset_->icmp_;
            for (unsigned level = 0; level < config::kNumLevels; level++) {
                levels_[level].added_files = new FileSet(cmp);
                levels_[level].added_guards = new GuardSet(g_cmp);
                levels_[level].added_complete_guards = new GuardSet(g_cmp);
                levels_[level].deleted_guards = new KeySet(k_cmp);
                levels_[level].pm = nullptr;
            }
        }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunsafe-loop-optimizations"

        ~Builder()
        {
            for (unsigned level = 0; level < config::kNumLevels; level++) {
                const FileSet *added = levels_[level].added_files;
                std::vector<FileMetaData *> to_unref;
                to_unref.reserve(added->size());
                for (FileSet::const_iterator it = added->begin(); it != added->end(); ++it) {
                    to_unref.push_back(*it);
                }
                delete added;
                for (uint32_t i = 0; i < to_unref.size(); i++) {
                    FileMetaData *f = to_unref[i];
                    f->refs--;
                    if (f->refs <= 0) {
                        delete f;
                    }
                }

                const GuardSet *added_guards = levels_[level].added_guards;
                const GuardSet *added_complete_guards = levels_[level].added_complete_guards;
                std::vector<GuardMetaData *> g_to_unref;
                g_to_unref.reserve(added_guards->size() + added_complete_guards->size());
                for (GuardSet::const_iterator it = added_guards->begin(); it != added_guards->end(); it++) {
                    g_to_unref.push_back(*it);
                }
                for (GuardSet::const_iterator it = added_complete_guards->begin(); it != added_complete_guards->end();
                     it++) {
                    g_to_unref.push_back(*it);
                }
                delete added_guards;
                delete added_complete_guards;

                if (levels_[level].pm != nullptr) {
                    delete levels_[level].pm;
                }

                for (uint32_t i = 0; i < g_to_unref.size(); i++) {
                    GuardMetaData *g = g_to_unref[i];
                    g->refs--;
                    if (g->refs <= 0) {
                        delete g;
                    }
                }
            }
            base_->Unref();
        }

#pragma GCC diagnostic pop

        // Apply all of the edits in *edit to the current state.
        void Apply(VersionEdit *edit)
        {
            // Update compaction pointers
            for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
                const unsigned level = edit->compact_pointers_[i].first;
                vset_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
            }

            // Delete files
            const VersionEdit::DeletedFileSet &del = edit->deleted_files_;
            for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin(); iter != del.end(); ++iter) {
                const unsigned level = iter->first;
                const uint64_t number = iter->second;
                levels_[level].deleted_files.insert(number);
            }

            for (unsigned i = 0; i < config::kNumLevels; i++) {
                levels_[i].is_rewrite = edit->is_rewrite[i];
                if (levels_[i].is_rewrite) {
                    levels_[i].pm = new PiggyMain();
                    levels_[i].pm->level = edit->piggy_level;
                    levels_[i].pm->is_sen.swap(edit->piggy_sen);
                    levels_[i].pm->g = edit->piggy_g;
                    levels_[i].pm->cg.swap(edit->piggy_cg);
                    levels_[i].pm->is_full_guard_added = edit->piggy_full;
                    levels_[i].pm->file_metas.swap(edit->piggy_file_metas);
                }
                levels_[i].dup_ratio = edit->dup_ratio[i];
                levels_[i].before_size = edit->before_size[i];
                levels_[i].after_size = edit->after_size[i];
            }

            // Add new files
            for (size_t i = 0; i < edit->new_files_.size(); i++) {
                const unsigned level = edit->new_files_[i].first;
                FileMetaData *f = new FileMetaData(edit->new_files_[i].second);
                f->refs = 1;

                // We arrange to automatically compact this file after
                // a certain number of seeks.  Let's assume:
                //   (1) One seek costs 10ms
                //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                //   (3) A compaction of 1MB does 25MB of IO:
                //         1MB read from this level
                //         10-12MB read from next level (boundaries may be misaligned)
                //         10-12MB written to next level
                // This implies that 25 seeks cost the same as the compaction
                // of 1MB of data.  I.e., one seek costs approximately the
                // same as the compaction of 40KB of data.  We are a little
                // conservative and allow approximately one seek for every 16KB
                // of data before triggering a compaction.
                f->allowed_seeks = (f->file_size / 16384);
                if (f->allowed_seeks < 100)
                    f->allowed_seeks = 100;

                levels_[level].deleted_files.erase(f->number);
                levels_[level].added_files->insert(f);
            }

            // Handle added guards and deleted guards

            // Delete guards
            const VersionEdit::DeletedGuardSet &del_g = edit->deleted_guards_;
            for (VersionEdit::DeletedGuardSet::const_iterator iter = del_g.begin(); iter != del_g.end(); ++iter) {
                const unsigned level = iter->first;
                const InternalKey key = iter->second;
                levels_[level].deleted_guards->insert(key);
            }

            // Add new guards and complete guards
            for (unsigned j = 0; j < config::kNumLevels; j++) {
                for (size_t i = 0; i < edit->new_guards_[j].size(); i++) {
                    const unsigned level = j;
                    // TODO Do we need to create new guards or just reuse the guards from the GuardSet ?? Might lead to
                    // memory leak ?
                    GuardMetaData *g = new GuardMetaData(edit->new_guards_[j][i]);
                    g->refs = 1;
                    g->level = level;
                    g->number_segments = 0;
                    g->files.clear();
                    assert(g->last_guard_size == 0);
                    levels_[level].added_guards->insert(g);
                }
                for (size_t i = 0; i < edit->new_complete_guards_[j].size(); i++) {
                    const unsigned level = j;
                    GuardMetaData *g = new GuardMetaData(edit->new_complete_guards_[j][i]);
                    g->refs = 1;
                    g->level = level;
                    g->number_segments = 0;
                    g->files.clear();
                    levels_[level].added_complete_guards->insert(g);
                }
            }

            for (unsigned i = 0; i < config::kNumLevels; i++) {
                for (unsigned j = 0; j < edit->full_guard_added_in_compaction[i].size(); j++) {
                    levels_[i].full_guard_added_in_compaction.push_back(edit->full_guard_added_in_compaction[i][j]);
                }
            }
        }

        // Save the current state in *v.
        void SaveTo(Version *v, int mtc = 0, VersionEdit *edit = NULL)
        {
            int a;
            BySmallestKey cmp;
            BySmallestGuard guard_cmp;
            cmp.internal_comparator = &vset_->icmp_;
            guard_cmp.internal_comparator = &vset_->icmp_;

            v->last_n_stale_ratio = base_->last_n_stale_ratio;
            if (edit != NULL)
                if (edit->compaction_remove_stale_ratio >= 0.0) {
                    v->last_n_stale_ratio.push_back(edit->compaction_remove_stale_ratio);
                    if (v->last_n_stale_ratio.size() > 10) {
                        v->last_n_stale_ratio.pop_front();
                    }
                }
            v->compact_level = base_->compact_level;
            if (edit != NULL) {
                if (edit->compact_level != -1) {
                    v->compact_level = edit->compact_level;
                    MyLog2("compact level %ld", v->compact_level);
                }
            }
            // add limit
            for (unsigned level = 0; level < config::kNumLevels; level++) {
                v->sentinel_files_allow_seeks[level] = base_->sentinel_files_allow_seeks[level];
                // Merge the set of added files with the set of pre-existing files.
                // Drop any deleted files.  Store the result in *v.
                vstart_timer(MTC_SAVETO_ADD_FILES, BGC_SAVETO_ADD_FILES, mtc);
                const std::vector<FileMetaData *> &base_files = base_->files_[level];
                std::vector<FileMetaData *>::const_iterator base_iter = base_files.begin();
                std::vector<FileMetaData *>::const_iterator base_end = base_files.end();
                const FileSet *added = levels_[level].added_files;
                v->files_[level].reserve(base_files.size() + added->size());

                for (FileSet::const_iterator added_iter = added->begin(); added_iter != added->end(); ++added_iter) {
                    // Add all smaller files listed in base_
                    for (std::vector<FileMetaData *>::const_iterator bpos =
                             std::upper_bound(base_iter, base_end, *added_iter, cmp);
                         base_iter != bpos; ++base_iter) {
                        MaybeAddFile(v, level, *base_iter);
                    }

                    MaybeAddFile(v, level, *added_iter);
                }

                // Add remaining base files
                for (; base_iter != base_end; ++base_iter) {
                    MaybeAddFile(v, level, *base_iter);
                }
                vrecord_timer(MTC_SAVETO_ADD_FILES, BGC_SAVETO_ADD_FILES, mtc);

                vstart_timer(MTC_SAVETO_ADD_GUARDS, BGC_SAVETO_ADD_GUARDS, mtc);
                /* Add guards for this level. */
                const std::vector<GuardMetaData *> &base_guards = base_->guards_[level];
                const std::vector<GuardMetaData *> &base_complete_guards = base_->complete_guards_[level];
                const GuardSet *added_g = levels_[level].added_guards;
                const GuardSet *added_complete_g = levels_[level].added_complete_guards;
                GuardSet added_complete_g_combined(*added_complete_g);

                v->guards_[level].reserve(base_guards.size() + added_g->size());
                v->complete_guards_[level].reserve(base_complete_guards.size() + added_complete_g->size());

                // Add all the guards in increasing order of guard key so that it will be
                // easier to add the files to the guards

                // Adding the guards to the new version
                std::vector<GuardMetaData *>::const_iterator base_iter_g = base_guards.begin();
                std::vector<GuardMetaData *>::const_iterator base_end_g = base_guards.end();
                GuardMetaData *last_inserted = NULL;

                for (GuardSet::const_iterator added_iter_g = added_g->begin(); added_iter_g != added_g->end();
                     ++added_iter_g) {
                    for (std::vector<GuardMetaData *>::const_iterator bpos =
                             std::upper_bound(base_iter_g, base_end_g, *added_iter_g, guard_cmp);
                         base_iter_g != bpos; ++base_iter_g) {
                        MaybeAddGuard(v, level, *base_iter_g, &last_inserted);
                    }
                    MaybeAddGuard(v, level, *added_iter_g, &last_inserted);
                }
                for (; base_iter_g != base_guards.end(); base_iter_g++) {
                    MaybeAddGuard(v, level, *base_iter_g, &last_inserted);
                }
                vrecord_timer(MTC_SAVETO_ADD_GUARDS, BGC_SAVETO_ADD_GUARDS, mtc);

                // Adding the complete guards to the new version
                // Complete guards are not already sorted like guards_ since they are inserted in sequential order as
                // the key comes in at MemTableInserter code. So sort the complete guards differently by adding all
                // guards to the GuardSet which automatically takes care of sorting and duplicate removal

                vstart_timer(MTC_SAVETO_ADD_COMPLETE_GUARDS, BGC_SAVETO_ADD_COMPLETE_GUARDS, mtc);
                int unsorted_start = base_complete_guards.size();
                if (base_complete_guards.size() > 0) {
                    int start = (base_->num_complete_guards_[level] <= 0) ? 1 : base_->num_complete_guards_[level];
                    for (int i = start; i < base_complete_guards.size(); i++) {
                        if (vset_->icmp_.user_comparator()->Compare(base_complete_guards[i]->guard_key.user_key(),
                                                                    base_complete_guards[i - 1]->guard_key.user_key()) <
                            0) {
                            unsorted_start = i;
                            break;
                        }
                    }
                    for (int i = unsorted_start; i < base_complete_guards.size(); i++) {
                        added_complete_g_combined.insert(base_complete_guards[i]);
                        if (edit != NULL) {
                            edit->AddCompleteGuardFromExisting(level, base_complete_guards[i]);
                        }
                    }
                    if (edit != NULL) {
                        for (int i = unsorted_start - 1; i >= base_->num_complete_guards_[level]; i--) {
                            edit->AddCompleteGuardFromExisting(level, base_complete_guards[i]);
                        }
                    }
                    std::vector<GuardMetaData *>::const_iterator base_iter_complete_g = base_complete_guards.begin();
                    std::vector<GuardMetaData *>::const_iterator base_end_complete_g =
                        base_iter_complete_g + unsorted_start;
                    GuardMetaData *last_inserted = NULL;

                    for (GuardSet::const_iterator added_iter_complete_g = added_complete_g_combined.begin();
                         added_iter_complete_g != added_complete_g_combined.end(); ++added_iter_complete_g) {
                        for (std::vector<GuardMetaData *>::const_iterator bpos = std::upper_bound(
                                 base_iter_complete_g, base_end_complete_g, *added_iter_complete_g, guard_cmp);
                             base_iter_complete_g != bpos; ++base_iter_complete_g) {
                            MaybeAddCompleteGuard(v, level, *base_iter_complete_g, &last_inserted);
                        }
                        MaybeAddCompleteGuard(v, level, *added_iter_complete_g, &last_inserted);
                    }
                    for (; base_iter_complete_g != base_end_complete_g; base_iter_complete_g++) {
                        MaybeAddCompleteGuard(v, level, *base_iter_complete_g, &last_inserted);
                    }
                    v->num_complete_guards_[level] = v->complete_guards_[level].size();
                } else {
                    for (GuardSet::iterator added_iter_complete_g = added_complete_g_combined.begin();
                         added_iter_complete_g != added_complete_g_combined.end(); ++added_iter_complete_g) {
                        AddCompleteGuard(v, level, *added_iter_complete_g);
                    }
                    v->num_complete_guards_[level] = v->complete_guards_[level].size();
                }
                vrecord_timer(MTC_SAVETO_ADD_COMPLETE_GUARDS, BGC_SAVETO_ADD_COMPLETE_GUARDS, mtc);

                // update new version ratio
                if (levels_[level].dup_ratio > 0) {
                    v->dup_ratio[level] = (base_->dup_ratio[level] + levels_[level].dup_ratio) / 2.0;
                    MyLog2("update level %d origin %lf edit %lf dupratio %lf %lf %lf\n", level, base_->dup_ratio[level],
                           levels_[level].dup_ratio, v->dup_ratio[level], levels_[level].before_size,
                           levels_[level].after_size);
                } else {
                    v->dup_ratio[level] = base_->dup_ratio[level];
                }
                v->before_size[level] = base_->before_size[level] + levels_[level].before_size;
                v->after_size[level] = base_->after_size[level] + levels_[level].after_size;

                // Adding files to guards and sentinels
                // NOTE: The files are not added to complete guards (they are not necessary)
                vstart_timer(MTC_SAVETO_POPULATE_FILES, BGC_SAVETO_POPULATE_FILES, mtc);
                PopulateFilesToGuardsAndSentinels(v, level);
                vrecord_timer(MTC_SAVETO_POPULATE_FILES, BGC_SAVETO_POPULATE_FILES, mtc);
            }
        }

        // To determine whether a file is already added to a guard
        bool IsFileAlreadyPresent(std::vector<uint64_t> files, uint64_t current_file_number)
        {
            bool already_present = false;
            for (size_t i = 0; i < files.size(); i++) {
                if (files[i] == current_file_number) {
                    return true;
                }
            }
            return false;
        }

        // To add the file information to the guards and sentinels
        void PopulateFilesToGuardsAndSentinels(Version *v, unsigned level)
        {
            std::vector<GuardMetaData *> *guards = &v->guards_[level];
            std::vector<FileMetaData *> *sentinel_files = &v->sentinel_files_[level];
            // If there are no guards in the level, add all files to sentinel
            if (guards->size() == 0) {
                for (unsigned i = 0; i < v->files_[level].size(); i++) {
                    sentinel_files->push_back(v->files_[level][i]);
                }
                if (sentinel_files->size() > SEEK_COM) {
                    v->sentinel_files_allow_seeks[level] = G_ALLOW_SEEK;
                }
                return;
            }

            std::vector<FileMetaData *> files = v->files_[level];
            unsigned file_no = 0, guard_no = 0;

            const FileSet *added = levels_[level].added_files;
            FileSet::const_iterator added_iter = added->begin();
            bool added_file = false;
            int full_guard_added_in_compaction_ind = 0;

            // for rewrite
            std::vector<GuardMetaData *> full_guard_added_in_compaction;
            std::vector<GuardMetaData *> guard_has_new_files;
            bool addsenfiles = false;
            // MyLog2("last sen file size %ld\n", last_senfile_size);

            sentinel_files->clear();
            // Loop till the penultimate guard as the last guard is handled separately since it doesn't have an
            // end_range
            for (; guard_no <= guards->size(); guard_no++) {
                // Clearing the old file information present in the guards and sentinels as we will be populating
                // the information fresh from the new set of files in this level
                if (guard_no > 0) {
                    guards->at(guard_no - 1)->files.clear();
                    guards->at(guard_no - 1)->number_segments = 0;
                }

                bool first_entry = true;
                InternalKey guard_key;
                if (guard_no < guards->size()) {
                    if (guards->at(guard_no)->file_metas.size() != 0) {
                        MyLog2("g %s fm f %ld %ld\n", guards->at(guard_no)->guard_key.DebugString().c_str(),
                               guards->at(guard_no)->file_metas.size(), guards->at(guard_no)->files.size());
                        assert(guards->at(guard_no)->file_metas.size() == 0);
                    }
                    guard_key = guards->at(guard_no)->guard_key;
                    // see if is compact whole guard
                    if (levels_[level].full_guard_added_in_compaction.size() != 0 &&
                        full_guard_added_in_compaction_ind < levels_[level].full_guard_added_in_compaction.size())
                        if (vset_->icmp_.user_comparator()->Compare(
                                guard_key.user_key(),
                                levels_[level]
                                    .full_guard_added_in_compaction[full_guard_added_in_compaction_ind]
                                    ->guard_key.user_key()) == 0) {
                            full_guard_added_in_compaction.push_back(guards->at(guard_no));
                            full_guard_added_in_compaction_ind++;
                        }
                }

                for (; file_no < files.size(); file_no++) {
                    FileMetaData *current_file = files[file_no];
                    if (guard_no == guards->size() ||
                        vset_->icmp_.user_comparator()->Compare(current_file->largest.user_key(),
                                                                guards->at(guard_no)->guard_key.user_key()) < 0) {
                        // Need to insert this file to sentinel
                        if (guard_no == 0) {
                            if (added_iter != added->end() && current_file->number == (*added_iter)->number) {
                                added_iter++;
                                addsenfiles = true;
                            }
                            sentinel_files->push_back(current_file);
                            continue;
                        } else {
                            if (!IsFileAlreadyPresent(guards->at(guard_no - 1)->files, current_file->number)) {
                                if (added_iter != added->end() && current_file->number == (*added_iter)->number) {
                                    added_iter++;
                                    added_file = true;
                                } else {
                                    added_file = false;
                                }
                                guards->at(guard_no - 1)->files.push_back(current_file->number);
                                guards->at(guard_no - 1)->file_metas.push_back(current_file);
                                guards->at(guard_no - 1)->number_segments++;
                                if (added_file == true) {
                                    guard_has_new_files.push_back(guards->at(guard_no - 1));
                                }
                                if (first_entry) {
                                    guards->at(guard_no - 1)->smallest = current_file->smallest;
                                    guards->at(guard_no - 1)->largest = current_file->largest;
                                    first_entry = false;
                                } else {
                                    // Compare the smallest and largest key in the current file and set the current
                                    // guard's smallest and largest key accordingly
                                    if (vset_->icmp_.Compare(current_file->smallest,
                                                             guards->at(guard_no - 1)->smallest) < 0) {
                                        guards->at(guard_no - 1)->smallest = current_file->smallest;
                                    }
                                    if (vset_->icmp_.Compare(current_file->largest, guards->at(guard_no - 1)->largest) >
                                        0) {
                                        guards->at(guard_no - 1)->largest = current_file->largest;
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            // debug
            guard_no = 0;
            int guard_no2 = 0;
            int has_guard;
            if (PRINTOUT_DEBUG) {
                for (; guard_no < full_guard_added_in_compaction.size(); guard_no++) {
                    GuardMetaData *g = full_guard_added_in_compaction[guard_no];
                    has_guard = 0;
                    int reset_guard_no2 = guard_no2;
                    for (; guard_no2 < guard_has_new_files.size(); guard_no2++) {
                        if (vset_->icmp_.user_comparator()->Compare(
                                g->guard_key.user_key(), guard_has_new_files[guard_no2]->guard_key.user_key()) == 0) {
                            has_guard = 1;
                            break;
                        }
                    }
                    if (has_guard != 1) {
                        if (g->file_metas.size() != 0) {
                            MyLog2("g %s\n", g->guard_key.DebugString().c_str());
                            for (int file_no = 0; file_no < g->files.size(); file_no++) {
                                MyLog2("file %ld\n", g->files[file_no]);
                            }
                            for (guard_no2 = 0; guard_no2 < guard_has_new_files.size(); guard_no2++) {
                                MyLog2("g has new files %s\n",
                                       guard_has_new_files[guard_no2]->guard_key.DebugString().c_str());
                            }
                            fflush(stdout);
                            assert(0);
                        } else {
                            guard_no2 = reset_guard_no2;
                            MyLog2("this guard original has file now has no file\n");
                        }
                    }
                }
            }

            // reset the guard allow seeks if it has been less than SEEK_COM
            for (int i = 0; i < guards->size(); i++) {
                GuardMetaData *g = (*guards)[i];
                if (g->number_segments > SEEK_COM) {
                    g->guard_allow_seeks = G_ALLOW_SEEK;
                }
            }
            if (sentinel_files->size() > SEEK_COM) {
                v->sentinel_files_allow_seeks[level] = G_ALLOW_SEEK;
            }
        }

        void MaybeAddFile(Version *v, unsigned level, FileMetaData *f)
        {
            if (levels_[level].deleted_files.count(f->number) > 0) {
                // File is deleted: do nothing
            } else {
                std::vector<FileMetaData *> *files = &v->files_[level];
                f->refs++;
                files->push_back(f);
            }
        }

        void MaybeAddGuard(Version *v, unsigned level, GuardMetaData *g, GuardMetaData **last_inserted)
        {
            if ((*last_inserted) != NULL && vset_->icmp_.user_comparator()->Compare(
                                                g->guard_key.user_key(), (*last_inserted)->guard_key.user_key()) == 0) {
                return;
            }
            if (levels_[level].deleted_guards->count(g->guard_key) > 0) {
                // Guard is deleted: do nothing
            } else {
                std::vector<GuardMetaData *> *guards = &v->guards_[level];
                /* Check that the guards isn't already there. */
                // Create a new Guard meta data because at this point the guard meta data of current_ and guard meta
                // data of new version being created will have different set of files (until finalize is done)
                // TODO Delete the guard meta data of older versions to prevent memory leak
                GuardMetaData *new_g = new GuardMetaData();
                new_g->guard_key = g->guard_key;
                new_g->level = g->level;
                new_g->refs = 1;
                // LYH
                new_g->last_guard_size = g->last_guard_size;
                new_g->guard_allow_seeks = g->guard_allow_seeks;
                guards->push_back(new_g);
                *last_inserted = g;
            }
        }

        void AddCompleteGuard(Version *v, unsigned level, GuardMetaData *g)
        {
            if (levels_[level].deleted_guards->count(g->guard_key) > 0) {
                // Guard is deleted: do nothing
            } else {
                std::vector<GuardMetaData *> *complete_guards = &v->complete_guards_[level];
                // No need to check for complete guards because the GuardSet will (and should) contain unique values of
                // guard keys
                g->refs++;
                complete_guards->push_back(g);
            }
        }

        void MaybeAddCompleteGuard(Version *v, unsigned level, GuardMetaData *g, GuardMetaData **last_inserted)
        {
            if ((*last_inserted) != NULL && vset_->icmp_.user_comparator()->Compare(
                                                g->guard_key.user_key(), (*last_inserted)->guard_key.user_key()) == 0) {
                return;
            }
            if (levels_[level].deleted_guards->count(g->guard_key) > 0) {
                // Guard is deleted: do nothing
            } else {
                std::vector<GuardMetaData *> *complete_guards = &v->complete_guards_[level];
                /* Check that the guards isn't already there. */
                g->refs++;
                complete_guards->push_back(g);
                *last_inserted = g;
            }
        }
    };

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunsafe-loop-optimizations"


#ifdef SEEK_PARALLEL
    static void ConcurrentSeekTableIteratorWrapper(void *vset)
    {
        VersionSet *vset_ = reinterpret_cast<VersionSet *>(vset);
        pthread_t thread_id = vset_->getEnv()->GetThreadId();  // Thread id of this thread
        int index =
            vset_
                ->seek_titerator_pthread_index_map_[thread_id];  // Get the static mapping of thread id to integer index
        TableCache *const table_cache = vset_->GetTableCache();

        while (vset_->stop_seek_threads_ == 0) {
            while (vset_->seek_titerator_thread_status_[index] != ASSIGNED && vset_->stop_seek_threads_ == 0) {
                vset_->seek_titerator_threads_cv_[index].Wait();
            }
            if (vset_->stop_seek_threads_ == 1) {
                break;
            }
            vset_->seek_titerator_thread_status_[index] = BUSY;
            // Get the data to work upon from shared variables at the VersionSet level
            uint64_t file_number = vset_->seek_titerator_file_numbers_[index];
            uint64_t file_size = vset_->seek_titerator_file_sizes_[index];
            const ReadOptions &options = vset_->seek_titerator_read_options_[index];
            Iterator **result = vset_->seek_titerator_result_iterators_[index];
            *result = table_cache->NewIterator(options, file_number, file_size);
            int group_index =
                vset_->seek_titerator_assigned_group_id_[index];  // This is used to coordinate between a set of threads
                                                                  // which belong to the same parallel call

#    ifdef SEEK_TWO_WAY_SIGNAL
            // Decrement the number of pending threads in this group to complete.
            bool last_thread_to_complete = false;
            vset_->seek_titerator_group_threads_mutex_[group_index].Lock();
            vset_->seek_titerator_group_num_pending_[group_index]--;
            if (vset_->seek_titerator_group_num_pending_[group_index] == 0) {
                last_thread_to_complete = true;
            }
            vset_->seek_titerator_group_threads_mutex_[group_index].Unlock();

            // The last thread to complete in this group signals the parent thread until it acknowledges.
            if (last_thread_to_complete) {
                while (vset_->seek_titerator_group_return_status_[group_index] !=
                       ACKNOWLEDGED) {  // Kind of busy waiting here.
                    vset_->seek_titerator_group_threads_cv_[group_index]
                        .Signal();  // Parent thread will be waiting on this cv. s
                }
            }
#    endif
            vset_->seek_titerator_thread_status_[index] = IDLE;
            vset_->seek_titerator_threads_current_workload[index] =
                0;  // Meaning this thread is now free for processing
        }
    }
#endif

    VersionSet::VersionSet(const std::string &dbname, const Options *options, TableCache *table_cache,
                           const InternalKeyComparator *cmp, Timer *timer)
        : env_(options->env), dbname_(dbname), options_(options), table_cache_(table_cache), icmp_(*cmp),
          next_file_number_(2), manifest_file_number_(0),  // Filled by Recover()
          last_sequence_(0), log_number_(0), prev_log_number_(0), descriptor_file_(NULL), descriptor_log_(NULL),
          dummy_versions_(this), current_(NULL), timer(timer),
#ifdef SEEK_PARALLEL
          stop_seek_threads_(0),
#endif
          num_seek_threads_(NUM_SEEK_THREADS), hm_manager_(Singleton::Gethmmanager())
    {
#ifdef SEEK_PARALLEL
        current_thread_ = GetCurrentThreadId();
#endif

        AppendVersion(new Version(this));
        PopulateFileLevelBloomFilter();

#ifdef SEEK_PARALLEL
        for (int i = 0; i < NUM_SEEK_THREADS; i++) {
            seek_thread_timers_[i] = new Timer();

            seek_threads_cv_[i].InitMutex(&seek_threads_mutex_[i]);
            seek_group_threads_cv_[i].InitMutex(&seek_group_threads_mutex_[i]);
            pthread_t t = env_->StartThreadAndReturnThreadId(&VersionSet::ConcurrentSeekWrapper, this);
            seek_pthread_index_map_[t] = i;
            seek_threads_current_workload[i] = 0;
            seek_thread_status_[i] = IDLE;
            seek_group_occupied_[i] = 0;
            seek_group_return_status_[i] = UNINITIALIZED;

            seek_titerator_threads_cv_[i].InitMutex(&seek_titerator_threads_mutex_[i]);
            seek_titerator_group_threads_cv_[i].InitMutex(&seek_titerator_group_threads_mutex_[i]);

            pthread_t t2 = env_->StartThreadAndReturnThreadId(&ConcurrentSeekTableIteratorWrapper, this);

            seek_titerator_pthread_index_map_[t2] = i;
            seek_titerator_threads_current_workload[i] = 0;
            seek_titerator_thread_status_[i] = IDLE;
            seek_titerator_group_occupied_[i] = 0;
            seek_titerator_group_return_status_[i] = UNINITIALIZED;
        }
#endif

#ifdef READ_PARALLEL
        for (int i = 0; i < NUM_READ_THREADS; i++) {
            read_threads_cv_[i].InitMutex(&read_threads_mutex_[i]);
            pthread_t t = env_->StartThreadAndReturnThreadId(&VersionSet::ConcurrentReadWrapper, this);
            pthread_index_map_[t] = i;
            read_threads_current_workload[i] = 0;
            thread_status_[i] = IDLE;
        }
#endif
    }

    VersionSet::~VersionSet()
    {
#ifdef READ_PARALLEL
        stop_read_threads_ = 1;
        for (int i = 0; i < NUM_READ_THREADS; i++) {
            read_threads_cv_[i].Signal();
        }
        // BUSY WAITING !
        while (true) {
            bool completed = true;
            for (int i = 0; i < NUM_READ_THREADS; i++) {
                if (read_threads_current_workload[i] != 0) {
                    completed = false;
                    break;
                }
            }
            if (completed) {
                break;
            }
        }
#endif

#ifdef SEEK_PARALLEL
        stop_seek_threads_ = 1;
        for (int i = 0; i < NUM_SEEK_THREADS; i++) {
            seek_threads_cv_[i].Signal();
            seek_titerator_threads_cv_[i].Signal();
        }
        // BUSY WAITING !
        while (true) {
            bool completed = true;
            for (int i = 0; i < NUM_SEEK_THREADS; i++) {
                if (seek_threads_current_workload[i] != 0 || seek_titerator_threads_current_workload[i] != 0) {
                    completed = false;
                    break;
                }
            }
            if (completed) {
                break;
            }
        }
        for (int i = 0; i < NUM_SEEK_THREADS; i++) {
            if (seek_thread_timers_[i] != NULL) {
                delete seek_thread_timers_[i];
            }
        }
#endif

#ifdef FILE_LEVEL_FILTER
        for (std::map<uint64_t, std::string *>::iterator it = file_level_bloom_filter.begin();
             it != file_level_bloom_filter.end(); ++it) {
            std::string *filter_string = (*it).second;
            if (filter_string != NULL) {
                delete filter_string;
            }
        }
#endif
        current_->Unref();
        assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
        delete descriptor_log_;
        delete descriptor_file_;
    }

#pragma GCC diagnostic pop

    void VersionSet::PrintSeekThreadsStaticTimerAuditIndividual()
    {
        table_cache_->PrintSeekThreadsStaticTimerAuditIndividual();
    }

    void VersionSet::PrintSeekThreadsStaticTimerAuditCumulative()
    {
        table_cache_->PrintSeekThreadsStaticTimerAuditCumulative();
    }

    void VersionSet::AppendVersion(Version *v)
    {
        // Make "v" current
        assert(v->refs_ == 0);
        assert(v != current_);
        if (current_ != NULL) {
            current_->Unref();
        }
        current_ = v;
        v->Ref();

        // Append to linked list
        v->prev_ = dummy_versions_.prev_;
        v->next_ = &dummy_versions_;
        v->prev_->next_ = v;
        v->next_->prev_ = v;
    }

    void VersionSet::PopulateFileLevelBloomFilter()
    {
#ifdef FILE_LEVEL_FILTER
        // TODO de-couple file level bloom filter from block level bloom filter
        const FilterPolicy *filter_policy = options_->filter_policy;
        FileLevelFilterBuilder file_level_filter_builder(filter_policy);

        if (filter_policy == NULL) {
            return;
        }

        Version *current = current_;
        current->Ref();
        for (int i = 0; i < config::kNumLevels; i++) {
            for (int j = 0; j < current->files_[i].size(); j++) {
                PopulateBloomFilterForFile(current->files_[i][j], &file_level_filter_builder);
            }
        }
        file_level_filter_builder.Destroy();
        current->Unref();
#endif
    }

    void VersionSet::PopulateBloomFilterForFile(FileMetaData *file, FileLevelFilterBuilder *file_level_filter_builder)
    {
        uint64_t file_number = file->number;
        uint64_t file_size = file->file_size;
        int cnt = 0;

        if (file_level_bloom_filter[file_number] != NULL) {
            // This means that we have already calculated the bloom filter for this file and files are immutable (wrt a
            // file number)
            return;
        }

        Iterator *iter = table_cache_->NewIterator(ReadOptions(), file_number, file_size);
        iter->SeekToFirst();
        int index = 0;
        while (iter->Valid()) {
            cnt++;
            file_level_filter_builder->AddKey(iter->key());
            index++;
            iter->Next();
        }
        if (cnt > 0) {
            std::string *filter_string = file_level_filter_builder->GenerateFilter();
            assert(filter_string != NULL);

            AddFileLevelBloomFilterInfo(file_number, filter_string);
        }
        delete iter;
    }

    void VersionSet::InitializeFileLevelBloomFilter()
    {
#ifdef FILE_LEVEL_FILTER
        PopulateFileLevelBloomFilter();
#endif
    }

    void VersionSet::InitializeTableCacheFileMetaData()
    {
        Version *current = current_;
        current->Ref();
        for (int level = 0; level < config::kNumLevels; level++) {
            for (int i = 0; i < current->files_[level].size(); i++) {
                FileMetaData *file = current->files_[level][i];
                table_cache_->SetFileMetaDataMap(file->number, file->file_size, file->smallest, file->largest);
            }
        }
        current->Unref();
    }

    void VersionSet::AddFileLevelBloomFilterInfo(uint64_t file_number, std::string *filter_string)
    {
#ifdef FILE_LEVEL_FILTER
        file_level_bloom_filter[file_number] = filter_string;
#endif
    }
    void VersionSet::RemoveFileLevelBloomFilterInfo(uint64_t file_number)
    {
#ifdef FILE_LEVEL_FILTER
        std::string *filter = file_level_bloom_filter[file_number];
        if (filter != NULL) {
            delete filter;
        }
        file_level_bloom_filter.erase(file_number);
#endif
    }

    void VersionSet::RemoveFileMetaDataFromTableCache(uint64_t file_number)
    {
        table_cache_->RemoveFileMetaDataMapForFile(file_number);
    }
    #ifdef READ_PARALLEL
        void VersionSet::ConcurrentReadWrapper(void* vset) {
            VersionSet* vset_ = reinterpret_cast<VersionSet*>(vset);
            pthread_t thread_id = vset_->env_->GetThreadId();
            int index = vset_->pthread_index_map_[thread_id];

            while (vset_->stop_read_threads_ == 0) {
                while (vset_->read_threads_current_workload[index] == 0 && vset_->stop_read_threads_ == 0) {
                    vset_->read_threads_cv_[index].Wait();
                }
                if (vset_->stop_read_threads_ == 1) {
                    break;
                }
                vset_->thread_status_[index] = BUSY;
                vset_->table_cache_->Get(vset_->read_options_[index], vset_->file_numbers_to_read_[index],
                        vset_->file_sizes_to_read_[index], vset_->internal_keys_to_read_[index],
                        vset_->saver_values_from_read_[index], SaveValue, vset_->timer);
                vset_->read_threads_current_workload[index] = 0;
                vset_->thread_status_[index] = IDLE;
            }
            int n = vset_->num_read_threads_;
            vset_->num_read_threads_--;
        }
    #endif
    Status VersionSet::LogAndApply(VersionEdit *edit, port::Mutex *mu, port::CondVar *cv, bool *wt,
                                   std::vector<uint64_t> file_numbers, std::vector<std::string *> file_level_filters,
                                   int mtc = 0)
    {
        int cg_sizes[config::kNumLevels];
        start_timer(MTC_LAA_WAIT_FOR_LIVE_BACKUP, BGC_LAA_WAIT_FOR_LIVE_BACKUP, mtc);

        while (*wt) {
            cv->Wait();
        }
        record_timer(MTC_LAA_WAIT_FOR_LIVE_BACKUP, BGC_LAA_WAIT_FOR_LIVE_BACKUP, mtc);
        *wt = true;
        if (edit->has_log_number_) {
            assert(edit->log_number_ >= log_number_);
            assert(edit->log_number_ < next_file_number_);
        } else {
            edit->SetLogNumber(log_number_);
        }

        if (!edit->has_prev_log_number_) {
            edit->SetPrevLogNumber(prev_log_number_);
        }

        edit->SetNextFile(next_file_number_);
        edit->SetLastSequence(last_sequence_);

        Version *v = new Version(this);
        v->ResetLevel();
        {
            Builder builder(this, current_);

            start_timer(MTC_LAA_APPLY_EDIT_TO_BUILDER, BGC_LAA_APPLY_EDIT_TO_BUILDER, mtc);
            builder.Apply(edit);
            record_timer(MTC_LAA_APPLY_EDIT_TO_BUILDER, BGC_LAA_APPLY_EDIT_TO_BUILDER, mtc);
            for (int level = 0; level < config::kNumLevels; level++) {
                cg_sizes[level] = current()->complete_guards_[level].size();
            }
            start_timer(MTC_LAA_SAVETO, BGC_LAA_SAVETO, mtc);
            builder.SaveTo(v, mtc, edit);
            record_timer(MTC_LAA_SAVETO, BGC_LAA_SAVETO, mtc);
        }
        start_timer(MTC_LAA_FINALIZE, BGC_LAA_FINALIZE, mtc);
        Finalize(v);
        record_timer(MTC_LAA_FINALIZE, BGC_LAA_FINALIZE, mtc);

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        std::string new_manifest_file;
        Status s;
        if (descriptor_log_ == NULL) {
            // No reason to unlock *mu here since we only hit this path in the
            // first call to LogAndApply (when opening the database).
            assert(descriptor_file_ == NULL);
            new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
            edit->SetNextFile(next_file_number_);
            s = env_->NewConcurrentWritableFile(new_manifest_file, &descriptor_file_);
            if (s.ok()) {
                descriptor_log_ = new log::Writer(descriptor_file_);

                start_timer(MTC_LAA_COMPLETE_WRITE_SNAPSHOT, BGC_LAA_COMPLETE_WRITE_SNAPSHOT, mtc);
                s = WriteSnapshot(descriptor_log_);
                record_timer(MTC_LAA_COMPLETE_WRITE_SNAPSHOT, BGC_LAA_COMPLETE_WRITE_SNAPSHOT, mtc);
            }
        }

        // Unlock during expensive MANIFEST log write
        {
            mu->Unlock();

            // Write new record to MANIFEST log
            if (s.ok()) {
                start_timer(MTC_LAA_ENCODE_EDIT, BGC_LAA_ENCODE_EDIT, mtc);
                std::string record;
                edit->EncodeTo(&record);
                record_timer(MTC_LAA_ENCODE_EDIT, BGC_LAA_ENCODE_EDIT, mtc);

                start_timer(MTC_LAA_ADD_RECORD_TO_DESC_LOG, BGC_LAA_ADD_RECORD_TO_DESC_LOG, mtc);
                s = descriptor_log_->AddRecord(record);
                record_timer(MTC_LAA_ADD_RECORD_TO_DESC_LOG, BGC_LAA_ADD_RECORD_TO_DESC_LOG, mtc);

                start_timer(MTC_LAA_SYNC_MANIFEST_LOG_WRITE, BGC_LAA_SYNC_MANIFEST_LOG_WRITE, mtc);
                if (s.ok()) {
                    // XXX Unlock during expensive MANIFEST log write
                    s = descriptor_file_->Sync();
                }
                record_timer(MTC_LAA_SYNC_MANIFEST_LOG_WRITE, BGC_LAA_SYNC_MANIFEST_LOG_WRITE, mtc);
                if (!s.ok()) {
                    Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
                }
            }

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if (s.ok() && !new_manifest_file.empty()) {
                s = SetCurrentFile(env_, dbname_, manifest_file_number_);
            }

            start_timer(MTC_LAA_GET_LOCK_AFTER_MANIFEST_SYNC, BGC_LAA_GET_LOCK_AFTER_MANIFEST_SYNC, mtc);
            mu->Lock();
            record_timer(MTC_LAA_GET_LOCK_AFTER_MANIFEST_SYNC, BGC_LAA_GET_LOCK_AFTER_MANIFEST_SYNC, mtc);
        }

#ifdef FILE_LEVEL_FILTER
        // Add file level filters to in-memory map
        // Numbers can possibly contain more values than filters because the reserved file numbers are
        // appended at the end to be cleared from pending outputs
        for (int i = 0; i < file_numbers.size() && i < file_level_filters.size(); i++) {
            AddFileLevelBloomFilterInfo(file_numbers[i], file_level_filters[i]);
        }
#endif

        // Install the new version
        if (s.ok()) {
            // Get the delta complete guards added to in-memory version in small time period of manifest write
            start_timer(MTC_LAA_GET_DELTA_COMPLETE_GUARDS, BGC_LAA_GET_DELTA_COMPLETE_GUARDS, mtc);
            std::vector<GuardMetaData *> added_complete_guards[config::kNumLevels];
            for (int level = 0; level < config::kNumLevels; level++) {
                for (int i = cg_sizes[level]; i < current()->complete_guards_[level].size(); i++) {
                    GuardMetaData *g = new GuardMetaData;
                    g->guard_key = current()->complete_guards_[level][i]->guard_key;
                    g->level = level;
                    g->refs = 1;
                    added_complete_guards[level].push_back(g);
                }
            }
            record_timer(MTC_LAA_GET_DELTA_COMPLETE_GUARDS, BGC_LAA_GET_DELTA_COMPLETE_GUARDS, mtc);

            start_timer(MTC_LAA_APPEND_VERSION, BGC_LAA_APPEND_VERSION, mtc);
            AppendVersion(v);
            record_timer(MTC_LAA_APPEND_VERSION, BGC_LAA_APPEND_VERSION, mtc);

            start_timer(MTC_LAA_SYNC_COMPLETE_GUARDS, BGC_LAA_SYNC_COMPLETE_GUARDS, mtc);

            for (int level = 0; level < config::kNumLevels; level++) {
                for (int i = 0; i < added_complete_guards[level].size(); i++) {
                    current_->complete_guards_[level].push_back(added_complete_guards[level][i]);
                }
            }
            record_timer(MTC_LAA_SYNC_COMPLETE_GUARDS, BGC_LAA_SYNC_COMPLETE_GUARDS, mtc);
            log_number_ = edit->log_number_;
            prev_log_number_ = edit->prev_log_number_;
        } else {
            delete v;
            if (!new_manifest_file.empty()) {
                delete descriptor_log_;
                delete descriptor_file_;
                descriptor_log_ = NULL;
                descriptor_file_ = NULL;
                env_->DeleteFile(new_manifest_file);
            }
        }

        *wt = false;
        cv->Signal();
        return s;
    }

    Status VersionSet::Recover()
    {
        struct LogReporter : public log::Reader::Reporter {
            LogReporter() : status()
            {
            }
            Status *status;
            virtual void Corruption(size_t /*bytes*/, const Status &s)
            {
                if (this->status->ok())
                    *this->status = s;
            }

          private:
            LogReporter(const LogReporter &);
            LogReporter &operator=(const LogReporter &);
        };

        // Read "CURRENT" file, which contains a pointer to the current manifest file
        std::string curfile;
        Status s = ReadFileToString(env_, CurrentFileName(dbname_), &curfile);
        if (!s.ok()) {
            return s;
        }
        if (curfile.empty() || curfile[curfile.size() - 1] != '\n') {
            return Status::Corruption("CURRENT file does not end with newline");
        }
        curfile.resize(curfile.size() - 1);

        std::string dscname = dbname_ + "/" + curfile;
        SequentialFile *file;
        s = env_->NewSequentialFile(dscname, &file);
        if (!s.ok()) {
            return s;
        }

        bool have_log_number = false;
        bool have_prev_log_number = false;
        bool have_next_file = false;
        bool have_last_sequence = false;
        uint64_t next_file = 0;
        uint64_t last_sequence = 0;
        uint64_t log_number = 0;
        uint64_t prev_log_number = 0;
        Builder builder(this, current_);

        {
            LogReporter reporter;
            reporter.status = &s;
            log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
            Slice record;
            std::string scratch;
            while (reader.ReadRecord(&record, &scratch) && s.ok()) {
                VersionEdit edit;
                s = edit.DecodeFrom(record);
                if (s.ok()) {
                    if (edit.has_comparator_ && edit.comparator_ != icmp_.user_comparator()->Name()) {
                        s = Status::InvalidArgument(edit.comparator_ + " does not match existing comparator ",
                                                    icmp_.user_comparator()->Name());
                    }
                }

                if (s.ok()) {
                    builder.Apply(&edit);
                }

                if (edit.has_log_number_) {
                    log_number = edit.log_number_;
                    have_log_number = true;
                }

                if (edit.has_prev_log_number_) {
                    prev_log_number = edit.prev_log_number_;
                    have_prev_log_number = true;
                }

                if (edit.has_next_file_number_) {
                    next_file = edit.next_file_number_;
                    have_next_file = true;
                }

                if (edit.has_last_sequence_) {
                    last_sequence = edit.last_sequence_;
                    have_last_sequence = true;
                }
            }
        }
        delete file;
        file = NULL;

        if (s.ok()) {
            if (!have_next_file) {
                s = Status::Corruption("no meta-nextfile entry in descriptor");
            } else if (!have_log_number) {
                s = Status::Corruption("no meta-lognumber entry in descriptor");
            } else if (!have_last_sequence) {
                s = Status::Corruption("no last-sequence-number entry in descriptor");
            }

            if (!have_prev_log_number) {
                prev_log_number = 0;
            }

            MarkFileNumberUsed(prev_log_number);
            MarkFileNumberUsed(log_number);
        }

        if (s.ok()) {
            Version *v = new Version(this);
            builder.SaveTo(v, 1);
            // Install recovered version
            Finalize(v);
            AppendVersion(v);
            manifest_file_number_ = next_file;
            next_file_number_ = next_file + 1;
            last_sequence_ = last_sequence;
            log_number_ = log_number;
            prev_log_number_ = prev_log_number;
            RecoverDisk();
        }

        return s;
    }

    void VersionSet::RecoverDisk()
    {
        std::vector<struct Ldbfile *> files;
        for (int level = 0; level < config::kNumLevels; level++) {
            for (int i = 0; i < current_->files_[level].size(); i++) {
                FileMetaData *f = current_->files_[level][i];
                struct Ldbfile *file =
                    new Ldbfile(f->number, f->disk_offset / (256 * 1024 * 2), f->disk_offset, f->file_size, level, "");
                files.push_back(file);
            }
        }
        hm_manager_->rebuild_data(files);
        for (int i = 0; i < files.size(); i++) {
            delete files[i];
        }
    }
    void VersionSet::MarkFileNumberUsed(uint64_t number)
    {
        if (next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }

    unsigned int VersionSet::RangeDifference(Slice a, Slice b)
    {
        void *input_a = (void *)a.data();
        size_t size_a = a.size();
        void *input_b = (void *)b.data();
        size_t size_b = b.size();
        const unsigned int murmur_seed = 42;
        unsigned int hash_a;
        MurmurHash3_x86_32(input_a, size_a, murmur_seed, &hash_a);
        unsigned int hash_b;
        MurmurHash3_x86_32(input_b, size_b, murmur_seed, &hash_b);
        // abs makes no fucking sense here
        if (hash_a < hash_b) {
            return hash_b - hash_a;
        } else {
            return hash_a - hash_b;
        }
    }

    uint64_t VersionSet::GetOverlappingRangeBetweenFiles(FileMetaData *f1, FileMetaData *f2)
    {
        int ss = icmp_.Compare(f1->smallest, f2->smallest);
        int sl = icmp_.Compare(f1->smallest, f2->largest);
        int ls = icmp_.Compare(f1->largest, f2->smallest);
        int ll = icmp_.Compare(f1->largest, f2->largest);

        if (ls < 0 || sl > 0) {  // No overlap
            return 0;
        }
        if (ss < 0 && ll > 0) {
            return RangeDifference(f2->smallest.user_key(), f2->largest.user_key());
        }
        if (ss > 0 && ll < 0) {
            return RangeDifference(f1->smallest.user_key(), f1->largest.user_key());
        }
        if (ss < 0 && ls > 0) {  // Second file's start is in the range of first file
            return RangeDifference(f2->smallest.user_key(), f1->largest.user_key());
        }
        if (ss > 0 && sl < 0) {  // First file's start is in the range of second file
            return RangeDifference(f1->smallest.user_key(), f2->largest.user_key());
        }
        return 0;
    }

    void VersionSet::Finalize(Version *v)
    {
        int max_level = 1;
        for (unsigned level = 1; level < config::kNumLevels; ++level) {
            if (v->sentinel_files_[level].size() || v->files_[level].size() != 0) {
                max_level = level;
            }
        }

        // Compute the ratio of disk usage to its limit
        for (unsigned level = 0; level < config::kNumLevels; ++level) {
            int max_files_per_segment = config::kMaxFilesPerGuardSentinel;
            if (MaxFilesPerGuardForLevel(level) > 0) {
                max_files_per_segment = MaxFilesPerGuardForLevel(level);
            }

            v->guard_compaction_scores_[level].clear();
            double score;
            if (level == 0) {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).

                // Compute the compaction scores for sentinels and guards
                v->sentinel_compaction_scores_[level] =
                    v->sentinel_files_[level].size() / static_cast<double>(config::kL0_SentinelCompactionTrigger);
                double max_score_in_level = v->sentinel_compaction_scores_[level];
                for (unsigned i = 0; i < v->guards_[level].size(); i++) {
                    GuardMetaData *g = v->guards_[level][i];
                    std::string guard_user_key = g->guard_key.user_key().ToString();
                    v->guard_compaction_scores_[level].push_back(
                        g->files.size() / static_cast<double>(config::kL0_GuardCompactionTrigger));
                    max_score_in_level = std::max(max_score_in_level, v->guard_compaction_scores_[level][i]);
                }

                v->compaction_scores_[level] =
                    v->sentinel_files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);
                MyLog2("level %d score %lf\n", level, v->compaction_scores_[level]);
            } else {
                // Compute the ratio of current size to size limit.
                double score1, score2;
                const uint64_t max_bytes = MaxBytesForLevel(level);
                const uint64_t avg_file_sz = (MaxFileSizeForLevel(level) + MinFileSizeForLevel(level)) >> 1;
                const int num_guards = v->guards_[level].size();
                uint64_t level_bytes = 0;
                int total_files = 0;
                // Compute the compaction scores for sentinel files and guards
                const int num_sentinel_files = v->sentinel_files_[level].size();
                total_files += num_sentinel_files;
                const uint64_t sentinel_bytes = TotalFileSize(v->sentinel_files_[level]);
                level_bytes += sentinel_bytes;
                score1 = sentinel_bytes / MaxBytesPerGuardForLevel(level);
                score2 = static_cast<double>(num_sentinel_files) / static_cast<double>(max_files_per_segment + 1);
                score = std::max(score1, score2);
                v->sentinel_compaction_scores_[level] = score;
                double max_score_in_level = v->sentinel_compaction_scores_[level];
                int64_t num_sst_of_guard = SEEK_COM;
                v->largest_guards[level] = nullptr;
                for (unsigned i = 0; i < num_guards; i++) {
                    GuardMetaData *g = v->guards_[level][i];
                    uint64_t guard_file_bytes = TotalFileSize(g->file_metas);
                    g->last_guard_size = guard_file_bytes;  // added
                    level_bytes += guard_file_bytes;
                    score1 = guard_file_bytes / MaxBytesPerGuardForLevel(level);
                    score2 = static_cast<double>(g->files.size()) / static_cast<double>(max_files_per_segment + 1);
                    score = std::max(score1, score2);
                    v->guard_compaction_scores_[level].push_back(score);

                    max_score_in_level = std::max(max_score_in_level, v->guard_compaction_scores_[level][i]);
                    total_files += g->files.size();

                    if (g->files.size() > num_sst_of_guard) {
                        num_sst_of_guard = g->files.size();
                        v->largest_guards[level] = g;
                    }
                }
                v->compaction_scores_[level] = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
                MyLog2("level %d score %lf\n", level, v->compaction_scores_[level]);
            }
        }
    }

    Status VersionSet::WriteSnapshot(log::Writer *log)
    {
        // Save metadata
        VersionEdit edit;
        edit.SetComparatorName(icmp_.user_comparator()->Name());

        // Save compaction pointers
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            if (!compact_pointer_[level].empty()) {
                InternalKey key;
                key.DecodeFrom(compact_pointer_[level]);
                edit.SetCompactPointer(level, key);
            }
        }

        for (unsigned level = 0; level < config::kNumLevels; level++) {
            // Save files
            const std::vector<FileMetaData *> &files = current_->files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest, f->disk_offset);
            }

            // Save sentinel files
            const std::vector<uint64_t> &sentinel_file_nos = current_->sentinel_file_nos_[level];
            for (size_t i = 0; i < sentinel_file_nos.size(); i++) {
                edit.AddSentinelFileNo(level, sentinel_file_nos[i]);
            }

            // Save guards
            const std::vector<GuardMetaData *> &guards = current_->guards_[level];
            for (size_t i = 0; i < guards.size(); i++) {
                const GuardMetaData *g = guards[i];
                if (g->number_segments > 0) {
                    edit.AddGuardWithFiles(level, g->number_segments, g->guard_key, g->smallest, g->largest, g->files);
                } else {
                    edit.AddGuard(level, g->guard_key);
                }
            }

            // Save complete guards (For complete guards, we do not store the file information)
            const std::vector<GuardMetaData *> &complete_guards = current_->complete_guards_[level];
            for (size_t i = 0; i < complete_guards.size(); i++) {
                const GuardMetaData *cg = complete_guards[i];
                edit.AddCompleteGuard(level, cg->guard_key);
            }
        }

        std::string record;
        edit.EncodeTo(&record);
        return log->AddRecord(record);
    }

    int VersionSet::NumLevelFiles(unsigned level) const
    {
        assert(level < config::kNumLevels);
        return current_->files_[level].size();
    }

    int VersionSet::NumGuards(unsigned level) const
    {
        assert(level < config::kNumLevels);
        return current_->guards_[level].size();
    }

    std::vector<GuardMetaData *> VersionSet::GetGuardsAtLevel(unsigned level)
    {
        assert(level < config::kNumLevels);
        return current_->guards_[level];
    }

    std::vector<GuardMetaData *> VersionSet::GetCompleteGuardsAtLevel(unsigned level)
    {
        assert(level < config::kNumLevels);
        return current_->complete_guards_[level];
    }

    std::string VersionSet::GetCurrentVersionState()
    {
        if (current_ == NULL) {
            MyLog2("current_ is NULL !!\n");
        }
        return current_->DebugString2();
    }

    int VersionSet::NumGuardFiles(unsigned level) const
    {
        assert(level < config::kNumLevels);
        int num_guard_files = 0;
        std::vector<GuardMetaData *> guards = current_->guards_[level];
        for (unsigned i = 0; i < guards.size(); i++) {
            num_guard_files += guards[i]->number_segments;
        }
        return num_guard_files;
    }

    int VersionSet::NumSentinelFiles(unsigned level) const
    {
        assert(level < config::kNumLevels);
        return current_->sentinel_files_[level].size();
    }

    std::string VersionSet::GuardDetailsAtLevel(unsigned level) const
    {
        assert(level < config::kNumLevels);
        std::vector<GuardMetaData *> guard_meta_data_list = current_->guards_[level];
        std::string result = "{\"level\":";
        result.append(NumberToString(level)).append(",");
        result.append("\"num_guards\":").append(NumberToString(NumGuards(level))).append(",");
        result.append("\"guards\":[");
        for (unsigned i = 0; i < guard_meta_data_list.size(); i++) {
            if (i > 0) {
                result.append("},");
            }
            result.append("{\"guard_key\":\"")
                .append(guard_meta_data_list[i]->guard_key.user_key().ToString())
                .append("\",\"num_segments\":")
                .append(NumberToString(guard_meta_data_list[i]->number_segments))
                .append(",\"size\":")
                //.append(NumberToString(TotalFileSize(guard_meta_data_list[i]->file_metas)/1024/1024)) // origin
                .append(NumberToString(TotalFileSize(guard_meta_data_list[i]->file_metas) / 1024))
                //.append(",\"files\":[");
                .append(",\"files and size\":{");
            for (unsigned j = 0; j < guard_meta_data_list[i]->files.size(); j++) {
                if (j > 0) {
                    result.append(",");
                }
                result.append("[");  // add
                result.append(NumberToString(guard_meta_data_list[i]->files[j]));
                result.append(",");  // add
                result.append(NumberToString(guard_meta_data_list[i]->file_metas[j]->file_size / 1024));
                result.append("KB");  // add  // add
                result.append("]");   // add
            }
            // result.append("]");
            result.append("}");
        }
        result.append("}]}\n");
        return result;
    }

    std::string VersionSet::SentinelDetailsAtLevel(unsigned level) const
    {
        assert(level < config::kNumLevels);
        std::vector<FileMetaData *> file_meta_data_list = current_->sentinel_files_[level];
        std::string result = "{\"level\":";
        result.append(NumberToString(level)).append(",");
        result.append("\"num_sentinel_files\":").append(NumberToString(file_meta_data_list.size())).append(",");
        // result.append("\"size\":").append(NumberToString(TotalFileSize(file_meta_data_list)/1024/1024)).append(",");
        // //origin
        result.append("\"size\":")
            .append(NumberToString(TotalFileSize(file_meta_data_list) / 1024))
            .append(",");  // origin
        // result.append("\"files\":[");
        result.append("\"files\":{");
        for (unsigned i = 0; i < file_meta_data_list.size(); i++) {
            if (i > 0) {
                result.append(",");
            }
            result.append("[");  // add
            result.append(NumberToString(file_meta_data_list[i]->number));
            result.append(",");                                                       // add
            result.append(NumberToString(file_meta_data_list[i]->file_size / 1024));  // add
            result.append("KB");                                                      // add
            result.append("]");                                                       // add
        }
        result.append("]}\n");
        return result;
    }

    const char *VersionSet::LevelSummary(LevelSummaryStorage *scratch) const
    {
        // Update code if kNumLevels changes
        // assert(config::kNumLevels == 7);
        snprintf(scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
                 int(current_->files_[0].size()), int(current_->files_[1].size()), int(current_->files_[2].size()),
                 int(current_->files_[3].size()), int(current_->files_[4].size()), int(current_->files_[5].size()),
                 int(current_->files_[6].size()));
        return scratch->buffer;
    }

    uint64_t VersionSet::ApproximateOffsetOf(Version *v, const InternalKey &ikey)
    {
        uint64_t result = 0;
        for (unsigned level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &files = v->files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += files[i]->file_size;
                } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta->smallest, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    Table *tableptr;
                    Iterator *iter =
                        table_cache_->NewIterator(ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
                    if (tableptr != NULL) {
                        result += tableptr->ApproximateOffsetOf(ikey.Encode());
                    }
                    delete iter;
                }
            }
        }
        return result;
    }

    void VersionSet::AddLiveFiles(std::set<uint64_t> *live)
    {
        for (Version *v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
            for (unsigned level = 0; level < config::kNumLevels; level++) {
                const std::vector<FileMetaData *> &files = v->files_[level];
                for (size_t i = 0; i < files.size(); i++) {
                    live->insert(files[i]->number);
                }
            }
        }
    }

    int64_t VersionSet::NumLevelBytes(unsigned level) const
    {
        assert(level < config::kNumLevels);
        return TotalFileSize(current_->files_[level]);
    }

    int64_t VersionSet::MaxNextLevelOverlappingBytes()
    {
        int64_t result = 0;
        std::vector<FileMetaData *> overlaps;
        for (unsigned level = 1; level < config::kNumLevels - 1; level++) {
            for (size_t i = 0; i < current_->files_[level].size(); i++) {
                const FileMetaData *f = current_->files_[level][i];
                current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
                const int64_t sum = TotalFileSize(overlaps);
                if (sum > result) {
                    result = sum;
                }
            }
        }
        return result;
    }

    // Stores the minimal range that covers all entries in inputs in
    // *smallest, *largest.
    // REQUIRES: inputs is not empty
    void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs, InternalKey *smallest, InternalKey *largest)
    {
        assert(!inputs.empty());
        smallest->Clear();
        largest->Clear();
        for (size_t i = 0; i < inputs.size(); i++) {
            FileMetaData *f = inputs[i];
            if (i == 0) {
                *smallest = f->smallest;
                *largest = f->largest;
            } else {
                if (icmp_.Compare(f->smallest, *smallest) < 0) {
                    *smallest = f->smallest;
                }
                if (icmp_.Compare(f->largest, *largest) > 0) {
                    *largest = f->largest;
                }
            }
        }
    }

    // Stores the minimal range that covers all entries in inputs1 and inputs2
    // in *smallest, *largest.
    // REQUIRES: inputs is not empty
    void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1, const std::vector<FileMetaData *> &inputs2,
                               InternalKey *smallest, InternalKey *largest)
    {
        std::vector<FileMetaData *> all = inputs1;
        all.insert(all.end(), inputs2.begin(), inputs2.end());
        GetRange(all, smallest, largest);
    }

    void VersionSet::PrintGuardsStaleAndValid()
    {
        TableCache *table_cache = table_cache_;
        const InternalKeyComparator *icmp = &icmp_;
        VersionSet *vset = this;
        ReadOptions options;
        options.verify_checksums = options_->paranoid_checks;
        options.fill_cache = false;

        Version *current = vset->current_;

        for (int l = 0; l < config::kNumLevels; l++) {
            if (current->files_[l].size() == 0)
                continue;
            // print the info guard in a level
            std::vector<GuardMetaData *> guards = current->guards_[l];
            for (int g = 0; g < guards.size() + 1; g++) {
                Iterator **list;
                FileMetaData **file_meta_list;
                int num_files = 0;
                if (g == guards.size()) {
                    // print sentinel file
                    num_files = current->sentinel_files_[l].size();
                    if (num_files <= 0)
                        continue;
                    list = new Iterator *[num_files];
                    file_meta_list = new FileMetaData *[num_files];
                    for (int i = 0; i < num_files; i++) {
                        uint64_t file_number = current->sentinel_files_[l][i]->number;
                        uint64_t file_size = current->sentinel_files_[l][i]->file_size;
                        file_meta_list[i] = table_cache->GetFileMetaDataForFile(file_number);
                        list[i] = table_cache->NewIterator(options, file_number, file_size);
                    }
                } else {
                    // get the iterator of files in a guard
                    GuardMetaData *guard = guards[g];
                    num_files = guard->number_segments;
                    if (num_files <= 0)
                        continue;
                    list = new Iterator *[num_files];
                    file_meta_list = new FileMetaData *[num_files];

                    for (int i = 0; i < num_files; i++) {
                        uint64_t file_number = guard->file_metas[i]->number;
                        uint64_t file_size = guard->file_metas[i]->file_size;
                        file_meta_list[i] = table_cache->GetFileMetaDataForFile(file_number);
                        list[i] = table_cache->NewIterator(options, file_number, file_size);
                    }
                }

                Iterator *iterator = NewMergingIteratorForFiles(icmp, list, file_meta_list, num_files, icmp, vset,
                                                                l);  // level is not neccessary, just for seek parrallel
                // get the size of stale and not stale data (B)
                iterator->SeekToFirst();
                Status status;
                ParsedInternalKey ikey;
                ParsedInternalKey current_key;
                std::string current_key_backing;
                bool has_current_key = false;
                SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
                bool drop;
                int64_t valid_size = 0;
                int64_t stale_size = 0;
                int64_t total_size = 0;
                for (; iterator->Valid();) {
                    Slice key = iterator->key();
                    if (!ParseInternalKey(key, &ikey)) {
                        // Do not hide error keys
                        current_key_backing.clear();
                        has_current_key = false;
                        last_sequence_for_key = kMaxSequenceNumber;
                    } else {
                        if (!has_current_key ||
                            icmp->user_comparator()->Compare(ikey.user_key, current_key.user_key) != 0) {
                            // First occurrence of this user key
                            current_key_backing.assign(key.data(), key.size());
                            bool x = ParseInternalKey(Slice(current_key_backing), &current_key);
                            assert(x);
                            has_current_key = true;
                            last_sequence_for_key = kMaxSequenceNumber;
                        }
                        drop = true;
                        if (last_sequence_for_key == kMaxSequenceNumber) {
                            drop = false;
                        }
                        last_sequence_for_key = ikey.sequence;
                    }
                    if (drop) {
                        stale_size += key.size() + iterator->value().size();
                        // MyLog2("stale key size %ld value size %ld\n", key.size(), iterator->value().size());
                    } else {
                        valid_size += key.size() + iterator->value().size();
                        // MyLog2("valid key size %ld value size %ld\n", key.size(), iterator->value().size());
                    }
                    total_size += key.size() + iterator->value().size();
                    iterator->Next();
                }
                delete[] list;
                // delete[] file_meta_list;
                delete iterator;
                if (g != guards.size()) {
                    MyLog2("level %d g num %d valid %lf stale %lf total %lf (MB) v/t %lf s/t %lf\n", l, num_files,
                           valid_size * 1.0 / 1024.0 / 1024.0, stale_size * 1.0 / 1024.0 / 1024.0,
                           total_size * 1.0 / 1024.0 / 1024.0, valid_size * 1.0 / total_size * 1.0,
                           stale_size * 1.0 / total_size * 1.0);
                } else {
                    MyLog2("level %d s num %d valid %lf stale %lf total %lf (MB) v/t %lf s/t %lf\n", l, num_files,
                           valid_size * 1.0 / 1024.0 / 1024.0, stale_size * 1.0 / 1024.0 / 1024.0,
                           total_size * 1.0 / 1024.0 / 1024.0, valid_size * 1.0 / total_size * 1.0,
                           stale_size * 1.0 / total_size * 1.0);
                }
            }
        }
        fflush(stdout);
    }
    Iterator *VersionSet::MakeInputIteratorForGuardsInALevel(Compaction *c)
    {
        ReadOptions options;
        options.verify_checksums = options_->paranoid_checks;
        options.fill_cache = false;
        options.fill_geardb_cache = true;  // this is geardb design, by default, disable it (read whole table == false)

        const int space = 2;
        Iterator **list = new Iterator *[space];
        int num = 0;
        uint64_t min_file_number = 0;
        for (int which = 0; which < 2; which++) {
            if (!c->inputs_[which].empty()) {
                const std::vector<FileMetaData *> *files = &c->inputs_[which];
                const std::vector<FileMetaData *> *sentinel_files = &c->sentinel_inputs_[which];
                const std::vector<GuardMetaData *> *guards = &c->guard_inputs_[which];

                Iterator *guard_iterator =
                    new Version::LevelGuardNumIterator(icmp_, guards, sentinel_files, files, 0, timer);
                list[num++] = NewTwoLevelIteratorGuards(guard_iterator, &GetGuardIterator, table_cache_, &icmp_, this,
                                                        which + c->level(), options);
            }
        }
        assert(num <= space);
        Iterator *result = NewMergingIterator(&icmp_, list, num, this);
        delete[] list;
        return result;
    }

    struct CompactionBoundary {
        size_t start;
        size_t limit;
        CompactionBoundary() : start(0), limit(0)
        {
        }
        CompactionBoundary(size_t s, size_t l) : start(s), limit(l)
        {
        }
    };

    struct CmpByRange {
        CmpByRange(const InternalKeyComparator *cmp) : cmp_(cmp)
        {
        }
        bool operator()(const FileMetaData *lhs, const FileMetaData *rhs)
        {
            int smallest = cmp_->Compare(lhs->smallest, rhs->smallest);
            if (smallest == 0) {
                return cmp_->Compare(lhs->largest, rhs->largest) < 0;
            }
            return smallest < 0;
        }

      private:
        const InternalKeyComparator *cmp_;
    };

    // Stores the compaction boundaries between level and level + 1
    void VersionSet::GetCompactionBoundaries(Version *v, unsigned level, std::vector<FileMetaData *> *LA,
                                             std::vector<FileMetaData *> *LB, std::vector<uint64_t> *LA_sizes,
                                             std::vector<uint64_t> *LB_sizes,
                                             std::vector<CompactionBoundary> *boundaries)
    {
        const Comparator *user_cmp = icmp_.user_comparator();
        *LA = v->files_[level + 0];
        *LB = v->files_[level + 1];
        *LA_sizes = std::vector<uint64_t>(LA->size() + 1, 0);
        *LB_sizes = std::vector<uint64_t>(LB->size() + 1, 0);
        std::sort(LA->begin(), LA->end(), CmpByRange(&icmp_));
        std::sort(LB->begin(), LB->end(), CmpByRange(&icmp_));
        boundaries->resize(LA->size());

        // compute sizes
        for (size_t i = 0; i < LA->size(); ++i) {
            (*LA_sizes)[i + 1] = (*LA_sizes)[i] + (*LA)[i]->file_size;
        }
        for (size_t i = 0; i < LB->size(); ++i) {
            (*LB_sizes)[i + 1] = (*LB_sizes)[i] + (*LB)[i]->file_size;
        }

        // compute boundaries
        size_t start = 0;
        size_t limit = 0;
        // figure out which range of LB each LA covers
        for (size_t i = 0; i < LA->size(); ++i) {
            // find smallest start s.t. LB[start] overlaps LA[i]
            while (start < LB->size() &&
                   user_cmp->Compare((*LB)[start]->largest.user_key(), (*LA)[i]->smallest.user_key()) < 0) {
                ++start;
            }
            limit = std::max(start, limit);
            // find smallest limit >= start s.t. LB[limit] does not overlap LA[i]
            while (limit < LB->size() &&
                   user_cmp->Compare((*LB)[limit]->smallest.user_key(), (*LA)[i]->largest.user_key()) <= 0) {
                ++limit;
            }
            (*boundaries)[i].start = start;
            (*boundaries)[i].limit = limit;
        }
    }

    unsigned VersionSet::NumUncompactedLevels()
    {
        int num_uncompacted_levels = 0;
        for (int i = 0; i < config::kNumLevels; i++) {
            if (current_->compaction_scores_[i] >= 1.0) {
                num_uncompacted_levels++;
            }
        }
        return num_uncompacted_levels;
    }
    bool VersionSet::IsAllLevelsCompacted()
    {
        for (int i = 0; i < config::kNumLevels; i++) {
            if (current_->compaction_scores_[i] >= 1.0) {
                return false;
            }
        }
        return true;
    }

    unsigned VersionSet::PickCompactionLevel(bool *locked, bool seek_driven, bool *is_rewrite,
                                             bool *guard_seek_compaction) const
    {
        // Find an unlocked level has score >= 1 where level + 1 has score < 1.
        unsigned level = config::kNumLevels;
        double best_score = 1.0;
        for (int i = 0; i < config::kNumLevels - 1; ++i) {
            if (locked[i] || locked[i + 1]) {
                continue;
            }
            // if (current_->compaction_scores_[i] >= 1.0 && current_->compaction_scores_[i] >= best_score &&
            if (current_->compaction_scores_[i] >= best_score && current_->files_[i].size() != 0 &&
                current_->compaction_scores_[i] >= 1.0) {
                level = i;
                best_score = current_->compaction_scores_[i];
            }
        }

        // seek if it contains too much sst for a guard
        for (int i = 1; i < config::kNumLevels; i++) {
            if (i == config::kNumLevels - 1 && locked[i])
                continue;
            if (locked[i] || locked[i + 1]) {
                continue;
            }
            if (current_->files_[i].size() != 0) {
                int max_sst = current_->LevelGuardMaxSSTNum(i);
                if (max_sst * 1.0 / MAX_SST_ALLOWED_IN_GUARD > best_score) {
                    level = i;
                    best_score = max_sst * 1.0 / MAX_SST_ALLOWED_IN_GUARD;
                    MyLog2("pick %d level with max sst guard num %d f/t exceed threashold %ld\n", i, max_sst,
                           MAX_SST_ALLOWED_IN_GUARD);
                }
            }
        }

        double totalzone = hm_manager_->get_total_zone() * 1.0;
        double validzone = hm_manager_->get_zone_num() * 1.0;
        double freezone = totalzone - validzone;
        double ratio = freezone / totalzone;
        double last_n_stale_ratio = 0.0;
        bool may_has_dupratio = false;
        *is_rewrite = false;
        if (current_->last_n_stale_ratio.size() != 0) {
            MyLog2("last stale ");
            for (int i = 0; i < current_->last_n_stale_ratio.size(); i++) {
                MyLog2("%lf ", current_->last_n_stale_ratio[i]);
                last_n_stale_ratio += current_->last_n_stale_ratio[i];
                if (1 - current_->last_n_stale_ratio[i] > 0.001) {
                    may_has_dupratio = true;
                    break;
                }
            }
            last_n_stale_ratio = last_n_stale_ratio / current_->last_n_stale_ratio.size();
            MyLog2(" avg dupratio %lf\n", last_n_stale_ratio);
        }

        if (best_score < BASE_SCORE_LIMIT && may_has_dupratio) {
            int total_ssts = 0;
            int guard_contains_sst = 0;

            for (int i = 1; i < config::kNumLevels; i++) {
                if (current_->sentinel_files_[i].size() > 0) {
                    total_ssts += current_->sentinel_files_[i].size();
                    guard_contains_sst += 1;
                }
                std::vector<GuardMetaData *> guards = current_->guards_[i];
                for (int j = 0; j < guards.size(); j++) {
                    GuardMetaData *g = guards[j];
                    int sst_num = g->files.size();
                    if (sst_num > 0) {
                        total_ssts += sst_num;
                        guard_contains_sst += 1;
                    }
                }
            }

            if (guard_contains_sst != 0) {
                MyLog2("now totalsst %ld ghassst %ld\n", total_ssts, guard_contains_sst);
                // double fx = pow(guard_contains_sst*1.0/total_ssts*1.0, 0.25);
                double g_divide_s = guard_contains_sst * 1.0 / total_ssts * 1.0;
                double fx = pow(ratio, FX_RATIO);
                if (g_divide_s < 1 - fx) {
                    int current_max_sst = 1;
                    for (int i = 1; i < config::kNumLevels; i++) {
                        if (current_->files_[i].size() != 0) {
                            int max_sst = current_->LevelGuardMaxSSTNum(i);
                            if (max_sst > current_max_sst) {
                                level = i;
                                current_max_sst = max_sst;
                                MyLog2(
                                    "pick %d level with max sst guard num %d f/t %lf fx %lf totalsst %ld ghassst %ld freezone %ld totalzone %ld last_n_stale_ratio %lf\n",
                                    i, max_sst, ratio, fx, total_ssts, guard_contains_sst, (int64_t)freezone,
                                    (int64_t)totalzone, last_n_stale_ratio);
                            }
                        }
                    }
                }
            }
        }

        const bool size_compaction = (level != config::kNumLevels);
        *guard_seek_compaction = false;
        if (size_compaction) {
            assert(level >= 0);
        } else if (seek_driven && (level == config::kNumLevels || current_->compaction_scores_[level] < 1.0)) {
            if (current_->sen_file_to_compact_level_ != -1) {
                if (current_->sen_file_to_compact_level_ == config::kNumLevels - 1) {
                    if (!locked[current_->sen_file_to_compact_level_]) {
                        level = current_->sen_file_to_compact_level_;
                        MyLog2("senfile size %ld\n", current_->sentinel_files_[level].size());
                        fflush(stdout);
                        *guard_seek_compaction = true;
                    }
                } else {
                    if (!locked[current_->sen_file_to_compact_level_] &&
                        !locked[current_->sen_file_to_compact_level_ + 1]) {
                        level = current_->sen_file_to_compact_level_;
                        *guard_seek_compaction = true;
                        MyLog2("senfile size %ld\n", current_->sentinel_files_[level].size());
                        fflush(stdout);
                    }
                }
            } else {
                if (current_->guard_to_compact_level_ == config::kNumLevels - 1) {
                    if (!locked[current_->guard_to_compact_level_]) {
                        level = current_->guard_to_compact_level_;
                        MyLog2("guard file size %s %ld\n", current_->guard_to_compact_->guard_key.DebugString().c_str(),
                               current_->guard_to_compact_->file_metas.size());
                        fflush(stdout);
                        *guard_seek_compaction = true;
                    }
                } else {
                    if (!locked[current_->guard_to_compact_level_] && !locked[current_->guard_to_compact_level_ + 1]) {
                        level = current_->guard_to_compact_level_;
                        MyLog2("guard file size %s %ld\n", current_->guard_to_compact_->guard_key.DebugString().c_str(),
                               current_->guard_to_compact_->file_metas.size());
                        fflush(stdout);
                        *guard_seek_compaction = true;
                    }
                }
            }

            if (level == config::kNumLevels) {
                // Check if any level has very low data that can be compacted
                int max_level = -1;
                for (int i = config::kNumLevels - 1; i >= 0; i--) {
                    if (current_->files_[i].size() > 0) {
                        max_level = i;
                        break;
                    }
                }
                if (max_level == -1) {  // If all levels are empty
                    return level;
                }

                for (int i = max_level; i >= 0; i--) {
                    if (i >= config::kNumLevels - 1) {
                        continue;
                    }
                    if (locked[i] || locked[i + 1] || current_->files_[i].size() == 0) {
                        continue;
                    }
                    int64_t current_level_size = TotalFileSize(current_->files_[i]);
                    int64_t next_level_size = TotalFileSize(current_->files_[i + 1]);
                    int64_t next_level_size_in_mb = next_level_size / (1024 * 1024);

                    if (current_level_size == 0) {
                        continue;
                    }
                    double inter_level_ratio = next_level_size * 1.0 / current_level_size;
                    MyLog2("next level size %ld current level size %ld\n", next_level_size, current_level_size);
                    /*
                     * If the total amount of data in the level is less than threshold or if the next level has huge
                     * amount of data compared to this level, compact this level.
                     * */
                    if (inter_level_ratio <= 25.0 || next_level_size_in_mb < FORCE_COMPACT_SIZE_THRESHOLD_IN_MB) {
                        level = i;
                        MyLog2("change to 25 compaction\n");
                        break;
                    }
                }
            }
            current_->file_to_compact_ = NULL;
            current_->file_to_compact_level_ = -1;
        }

        if (level == config::kNumLevels - 1)
            *is_rewrite = true;
        MyLog2("compact level %d\n", level);
        return level;
    }

    static bool OldestFirst(FileMetaData *a, FileMetaData *b)
    {
        return a->number < b->number;
    }

    Compaction *
    VersionSet::PickCompactionForGuards(Version *v, unsigned level,
                                        std::vector<GuardMetaData *> *complete_guards_used_in_bg_compaction,
                                        std::vector<GuardMetaData *> *complete_guards_used_in_bg_compaction_old_level,
                                        std::vector<GuardMetaData *> *full_guard_added_in_compaction, PiggyMain *pm,
                                        bool seek_compaction, bool guard_seek_compaction)
    {
        assert(level < config::kNumLevels);
        MyLog2("compact version %p\n", v);
        int max_level = config::kNumLevels - 1;
        /*
         * If horizontal_compaction is true, only smaller files in the last level are compacted (to reduce write
         * amplification)
         */
        bool horizontal_compaction = false;
        if (level == config::kNumLevels - 1) {
            horizontal_compaction = true;
        }
        int num_input_levels_for_compaction = 2;
        if (horizontal_compaction) {
            num_input_levels_for_compaction = 1;
        }

        std::vector<GuardMetaData *> complete_guards_copy[2];
        int reduce_com_num[2] = {0, 0};
        // sort the complete_guards because the guards added later might not be in the right sorted position
        SortBySmallestGuard guard_comparator;
        guard_comparator.internal_comparator = &icmp_;

        for (int which = 0; which < num_input_levels_for_compaction; which++) {
            unsigned current_level = level + which;
            MyLog2("---current_level %d total complete guard---\n", current_level);
            MyLog2("complete guard size %ld\n", v->complete_guards_[current_level].size());
            if (current_level >= config::kNumLevels) {
                break;
            }

            if (PRINTOUT_DEBUG) {
                // debug: check if file are not in the range of old guard
                std::vector<GuardMetaData *> tmpguards = v->guards_[current_level];
                for (int i = 0; i < tmpguards.size(); i++) {
                    GuardMetaData *g = tmpguards[i];
                    Slice guard_key = g->guard_key.user_key(), next_guard_key;
                    if (i + 1 < tmpguards.size()) {
                        next_guard_key = tmpguards[i + 1]->guard_key.user_key();
                    }
                    for (unsigned j = 0; j < g->files.size(); j++) {
                        FileMetaData *file = g->file_metas[j];
                        Slice file_smallest = file->smallest.user_key();
                        Slice file_largest = file->largest.user_key();
                        if ((i < tmpguards.size() - 1  // If it is not the last guard, checking for smallest and largest
                                                       // to fit in the range
                             && (icmp_.user_comparator()->Compare(file_smallest, guard_key) < 0 ||
                                 icmp_.user_comparator()->Compare(file_largest, next_guard_key) >= 0)) ||
                            (i == tmpguards.size() -
                                      1  // If it is the last guard, checking for the smallest to fit in the guard
                             && icmp_.user_comparator()->Compare(file_smallest, guard_key) < 0)) {
                            MyLog2("level %d\n", current_level);
                            MyLog2("guard key %s\n", guard_key.ToString().c_str());
                            MyLog2("file %ld %s %s\n", file->number, file_smallest.ToString().c_str(),
                                   file_largest.ToString().c_str());
                            fflush(stdout);
                            assert(0);
                            break;  // No need to check other files
                        }
                    }
                }

                // debug: check if senfile are smaller than the first old guard
                Slice first_guard_key;
                if (tmpguards.size() > 0) {
                    first_guard_key = tmpguards[0]->guard_key.user_key();
                }
                for (unsigned i = 0; i < v->sentinel_files_[current_level].size(); i++) {
                    Slice largest_key = v->sentinel_files_[current_level][i]->largest.user_key();
                    if (tmpguards.size() > 0 && icmp_.user_comparator()->Compare(largest_key, first_guard_key) >= 0) {
                        MyLog2("senlargest %s key %s", largest_key.ToString().c_str(),
                               first_guard_key.ToString().c_str());
                        assert(0);
                    }
                }
            }

            complete_guards_copy[which].insert(complete_guards_copy[which].begin(),
                                               v->complete_guards_[current_level].begin(),
                                               v->complete_guards_[current_level].end());
            std::sort(complete_guards_copy[which].begin(), complete_guards_copy[which].end(), guard_comparator);

            // Remove duplicates
            if (complete_guards_copy[which].size() > 1) {
                GuardMetaData *prev = complete_guards_copy[which][0];
                std::vector<GuardMetaData *>::iterator it = complete_guards_copy[which].begin();
                it++;
                for (; it != complete_guards_copy[which].end();) {
                    if (icmp_.user_comparator()->Compare((*it)->guard_key.user_key(), prev->guard_key.user_key()) ==
                        0) {
                        reduce_com_num[which]++;
                        it = complete_guards_copy[which].erase(it);
                    } else {
                        prev = (*it);
                        ++it;
                    }
                }
            }

            if (PRINTOUT_DEBUG) {
                // debug
                std::string r;
                for (int i = 0; i < complete_guards_copy[which].size(); i++) {
                    GuardMetaData *g = complete_guards_copy[which][i];
                    r.append("complete guard_key: ").append(g->guard_key.DebugString()).append("\n");
                }
                MyLog2("%s", r.c_str());
                MyLog2("complete guard size %d %ld\n", current_level, complete_guards_copy[which].size());
                r.clear();
                for (int i = 0; i < v->guards_[current_level].size(); i++) {
                    GuardMetaData *g = v->guards_[current_level][i];
                    r.append("persisted guard_key: ").append(g->guard_key.DebugString());
                    r.append(" file: ");
                    for (int j = 0; j < g->number_segments; j++) {
                        FileMetaData *f = g->file_metas[j];
                        AppendNumberTo(&r, f->number);
                        r.append(" ");
                    }
                    r.append("\n");
                }
                MyLog2("%s", r.c_str());
                MyLog2("persisted guard size %d %ld\n", current_level, v->guards_[current_level].size());
            }
        }

        Compaction *c = new Compaction(level);
        c->input_version_ = v;
        c->input_version_->Ref();
        c->is_horizontal_compaction = horizontal_compaction;
        // 1 get the guard / complete guard that can incur split
        // 2 get the guard that score over 1 but not trigger split
        // 3 get the complete guard will not incur split
        //
        std::vector<GuardMetaData *> complete_guards_added_to_compaction[2];

        std::vector<GuardMetaData *> candidate_guards_added_to_compaction[2];
        std::vector<GuardMetaData *>
            candidate_complete_guards_added_to_compaction[2];  // when guards is score, then complete guards is nullptr
        std::vector<GuardMetaData *> complete_guard_not_split_sstable[2];
        for (int which = 0; which < num_input_levels_for_compaction; which++) {
            unsigned current_level = level + which;
            // sentinel largest key
            Slice largest_key;
            if (v->sentinel_files_[current_level].size() > 0) {
                largest_key = v->sentinel_files_[current_level][0]->largest.user_key();
                for (unsigned i = 1; i < v->sentinel_files_[current_level].size(); i++) {
                    Slice tmp_largest = v->sentinel_files_[current_level][i]->largest.user_key();
                    if (icmp_.user_comparator()->Compare(tmp_largest, largest_key) >=
                        0) {  // user key please use this comparator, don't use vset_->icmp_.Compare
                        largest_key = tmp_largest;
                    }
                }
            }
            if (current_level >= config::kNumLevels) {
                break;
            }
            std::vector<GuardMetaData *> complete_guards = complete_guards_copy[which];
            std::vector<GuardMetaData *> guards = v->guards_[current_level];
            int guard_index_iter = 0;
            int last_guard_index = 0;
            Slice max_key = largest_key;
            bool reset = true;
            int no_split_candidate_complete_guard_num = 0;
            int split_candidate_complete_guard_num = 0;
            int persisted_candidate_complete_guard_num = 0;

            for (size_t i = 0; i < complete_guards.size(); i++) {
                GuardMetaData *cg = complete_guards[i];
                int guard_index = -1;
                Slice cg_guard_key = cg->guard_key.user_key(), next_cg_guard_key;
                if (i + 1 < complete_guards.size()) {
                    next_cg_guard_key = complete_guards[i + 1]->guard_key.user_key();
                }

                // Assuming that both guards and complete_guards are sorted !
                for (; guard_index_iter < guards.size(); guard_index_iter++) {
                    int compare =
                        icmp_.user_comparator()->Compare(guards[guard_index_iter]->guard_key.user_key(), cg_guard_key);
                    if (compare == 0) {
                        guard_index = guard_index_iter;
                        last_guard_index = guard_index_iter;
                        guard_index_iter++;
                        reset = false;
                        complete_guards_added_to_compaction[which].push_back(cg);
                        break;
                    } else if (compare > 0) {
                        break;
                    } else {
                        // Ideally it should never reach here since there are no duplicates in complete_guards and
                        // complete_guards is a superset of guards
                    }
                }

                if (guard_index == -1) {
                    // If complete guard is smaller than current guard but larger than previous guard
                    if (reset == true) {
                        if (icmp_.user_comparator()->Compare(cg_guard_key, max_key) > 0) {
                            // if the complete guard's guard key is larger than largest key of files in
                            // previous guard, it won't cause split sstable
                            complete_guard_not_split_sstable[which].push_back(complete_guards[i]);
                            no_split_candidate_complete_guard_num++;
                            complete_guards_added_to_compaction[which].push_back(complete_guards[i]);
                        } else {
                            // if the complete guard's guard key is smaller than largest key of files in
                            // previous guard, it causes split sstable
                            candidate_complete_guards_added_to_compaction[which].push_back(
                                complete_guards[i]);  // this complete guards will cause guards[last_guard_index] to
                                                      // split
                            split_candidate_complete_guard_num++;
                            if (guard_index_iter != 0)
                                candidate_guards_added_to_compaction[which].push_back(guards[last_guard_index]);
                            else
                                candidate_guards_added_to_compaction[which].push_back(
                                    nullptr);  // this complete guard will make sentinel file to split
                        }
                    } else {
                        // when reset == false, this complete guard won't cause split, because its guard key must larger
                        // than largest key in files of previous guard (checked by (1)) than the largest files
                        complete_guard_not_split_sstable[which].push_back(complete_guards[i]);
                        no_split_candidate_complete_guard_num++;
                        complete_guards_added_to_compaction[which].push_back(complete_guards[i]);
                    }
                    continue;
                }

                GuardMetaData *g = guards[guard_index];
                bool guard_added = false;
                max_key.clear();
                for (unsigned j = 0; j < g->files.size(); j++) {
                    FileMetaData *file = g->file_metas[j];
                    Slice file_smallest = file->smallest.user_key();
                    Slice file_largest = file->largest.user_key();
                    if (max_key.empty()) {
                        max_key = file_largest;
                    } else {
                        max_key = icmp_.user_comparator()->Compare(file_largest, max_key) >= 0 ? file_largest : max_key;
                    }
                    if (guard_added == false) {              // (1)
                        if ((i < complete_guards.size() - 1  // If it is not the last guard, checking for smallest and
                                                             // largest to fit in the range
                             && (icmp_.user_comparator()->Compare(file_smallest, cg_guard_key) < 0 ||
                                 icmp_.user_comparator()->Compare(file_largest, next_cg_guard_key) >= 0)) ||
                            (i == complete_guards.size() - 1 &&
                             icmp_.user_comparator()->Compare(file_smallest, cg_guard_key) <
                                 0)) {  // If it is the last guard, checking for the smallest to fit in the guard
                            reset = true;
                            assert(icmp_.user_comparator()->Compare(file_smallest, cg_guard_key) >= 0);
                            guard_added = true;
                            // No need to check other files
                        }
                    }
                }
                persisted_candidate_complete_guard_num++;
                if (!guard_added && which == 0 &&
                    g->number_segments > 0) {  // when a guard only contain one sst or we consider it no duplicate then
                                               // its score is 0.01 (set in finalize)
                    candidate_guards_added_to_compaction[which].push_back(g);
                    candidate_complete_guards_added_to_compaction[which].push_back(nullptr);
                    continue;
                }
            }
            // currently complete_guard_added_to_compaction only contain 1. the complete guard that will not cause
            // SSTable split 2. the complete guard that have been persisted into FLSM-tree
            if (complete_guards_added_to_compaction[which].size() - complete_guard_not_split_sstable[which].size() !=
                guards.size()) {
                MyLog2("%ld %ld\n", complete_guards_added_to_compaction[which].size(), guards.size());
                fflush(stdout);
                assert(0);
            }
            if (no_split_candidate_complete_guard_num + split_candidate_complete_guard_num +
                    persisted_candidate_complete_guard_num !=
                complete_guards.size()) {
                MyLog2("%d %d %d %ld\n", no_split_candidate_complete_guard_num, split_candidate_complete_guard_num,
                       persisted_candidate_complete_guard_num, complete_guards.size());
                fflush(stdout);
                assert(0);
            }
        }

        if (PRINTOUT_DEBUG) {
            for (int which = 0; which < num_input_levels_for_compaction; which++) {
                // debug
                std::string r;
                MyLog2("level %d no split guard size %ld\n", level + which,
                       complete_guard_not_split_sstable[which].size());
                r.append("no split guard_key: ");
                for (int i = 0; i < complete_guard_not_split_sstable[which].size(); i++) {
                    GuardMetaData *g = complete_guard_not_split_sstable[which][i];
                    r.push_back(' ');
                    r.append(g->guard_key.DebugString());
                }
                MyLog2("%s\n", r.c_str());

                MyLog2("level candidate guards %d %ld\n", level + which,
                       candidate_guards_added_to_compaction[0].size());
                MyLog2("level guards size %d %ld\n", level + which, v->guards_[which + level].size());
                fflush(stdout);
            }
        }

        // choose the the most suitable guard to do compaction, only for which == 0
        std::vector<GuardMetaData *> guards = v->guards_[level];
        std::vector<GuardMetaData *> cg_guards = candidate_guards_added_to_compaction[0];  // based on socre to choose
        GuardMetaData *main_compaction_guard = nullptr;  // the one we pick to do compaction initiatively
        bool main_compaction_guard_is_sentinel = false;  // the sentinel one we pick to do compaction initiatively, if
                                                         // this set, the main_compaction_guard must be nullptr

        if (guard_seek_compaction) {
            int number_segments = 0;
            // GuardMetaData* guard = v->largest_guards[level];
            GuardMetaData *guard = v->guard_to_compact_;
            // no need to reset v->largest_guard_[level] since it will update in finalize
            if (guard == NULL) {
                main_compaction_guard = nullptr;
                main_compaction_guard_is_sentinel = true;
                number_segments = v->sentinel_files_[level].size();
                MyLog2("do guard seek compaction for level %d sentinel", level);
            } else {
                main_compaction_guard = guard;
                main_compaction_guard_is_sentinel = false;
                number_segments = guard->number_segments;
                MyLog2("do guard seek compaction for level %d", level);
            }
            v->guard_to_compact_ = NULL;
            v->guard_to_compact_level_ = -1;
            v->sen_file_to_compact_level_ = -1;
            assert(number_segments > SEEK_COM);
        } else {
            // pick guard contain max number
            int max_num_segment = v->sentinel_files_[level].size();
            main_compaction_guard = nullptr;
            for (int cguard_index_iter = 0; cguard_index_iter < cg_guards.size(); cguard_index_iter++) {
                if (cg_guards[cguard_index_iter] != nullptr) {
                    if (cg_guards[cguard_index_iter]->number_segments > max_num_segment) {
                        main_compaction_guard = cg_guards[cguard_index_iter];
                        main_compaction_guard_is_sentinel = false;
                        max_num_segment = cg_guards[cguard_index_iter]->number_segments;
                    }
                }
            }
            if (main_compaction_guard == nullptr) {
                main_compaction_guard = nullptr;
                main_compaction_guard_is_sentinel = true;
            }
        }
        // get the sst file that involve in the score max zone
        std::map<uint64_t, uint64_t> files_in_main_compaction_guards;
        // get the free zone left in drive to control the compaction size
        int64_t max_num_compaction_zone = hm_manager_->get_total_zone() - hm_manager_->get_zone_num();
        // to control the compaction size, how much zone can be involved in compaction
        int64_t burst_write_control_zone_num = max_num_compaction_zone - 1 <= 0 ? 1 : max_num_compaction_zone - 1;
        if (main_compaction_guard_is_sentinel) {
		MyLog2("pick sen in\n");
            pm->g = nullptr;
            pm->is_sen = v->sentinel_files_[level];
            pm->level = level;

            std::vector<uint64_t> tmp_sentinel_files;
            for (int i = 0; i < v->sentinel_files_[level].size(); i++) {
                tmp_sentinel_files.push_back(current_->sentinel_files_[level][i]->number);
            }
            if (burst_write_control_zone_num >= hm_manager_->get_guard_accross_zone(level, tmp_sentinel_files)) {
                MyLog2("pick sentinel guard of level %d sstcount %ld\n", level, v->sentinel_files_[level].size());
                for (int i = 0; i < v->sentinel_files_[level].size(); i++) {
                    files_in_main_compaction_guards[v->sentinel_files_[level][i]->number] = 1;
                }
            } else {
                if (RAM_PICK_FILE) {
		MyLog2("ran pick file\n");
		// randomly pick the files to do compaction until the zones involved have reached the limit
                    unsigned seed = (unsigned)time(NULL);
                    std::default_random_engine e(seed);
                    std::vector<int> candidate_files;
                    for (int i = 0; i < v->sentinel_files_[level].size(); i++) {
                        candidate_files.push_back(i);
                    }
                    std::shuffle(candidate_files.begin(), candidate_files.end(), e);
                    tmp_sentinel_files.clear();
                    for (int i = 0; i < candidate_files.size(); i++) {
                        tmp_sentinel_files.push_back(current_->sentinel_files_[level][candidate_files[i]]->number);
                        uint64_t cross_zone = hm_manager_->get_guard_accross_zone(level, tmp_sentinel_files);
                        if (tmp_sentinel_files.size() != 1 && cross_zone > burst_write_control_zone_num) {
                            MyLog2("pick sentinel guard of level %dcross %ld max %ld\n", level, cross_zone,
                                   burst_write_control_zone_num);
                            break;
                        }
                        files_in_main_compaction_guards[v->sentinel_files_[level][candidate_files[i]]->number] = 1;
                        // pm->file_metas.push_back(current_->sentinel_files_[level][candidate_files[i]]);
                    }
                    assert(tmp_sentinel_files.size() >= 1);
                } else {
                    // sequentially pick the files to do compaction until the zones involved have reached the limit
                    tmp_sentinel_files.clear();
                    for (int i = 0; i < v->sentinel_files_[level].size(); i++) {
                        tmp_sentinel_files.push_back(current_->sentinel_files_[level][i]->number);
                        uint64_t cross_zone = hm_manager_->get_guard_accross_zone(level, tmp_sentinel_files);
                        if (tmp_sentinel_files.size() != 1 && cross_zone > burst_write_control_zone_num) {
                            MyLog2("pick sentinel guard of level %d cross %ld max %ld\n", level, cross_zone,
                                   burst_write_control_zone_num);
                            break;
                        }
                        files_in_main_compaction_guards[v->sentinel_files_[level][i]->number] = 1;
                    }
                    assert(tmp_sentinel_files.size() >= 1);
                }
            }
        } else {
		MyLog2("not pick sen\n");
            assert(main_compaction_guard != nullptr);
            pm->g = main_compaction_guard;
            // no need to add to is_sen
            pm->level = level;

            if (burst_write_control_zone_num >
                hm_manager_->get_guard_accross_zone(level, main_compaction_guard->files)) {
                MyLog2("pick guard %s of level %d sstcount %ld\n",
                       main_compaction_guard->guard_key.DebugString().c_str(), level,
                       main_compaction_guard->number_segments);
                for (int i = 0; i < main_compaction_guard->number_segments; i++) {
                    files_in_main_compaction_guards[main_compaction_guard->file_metas[i]->number] = 1;
                }
            } else {
                if (RAM_PICK_FILE) {
			MyLog2("ran pick file\n");
                    unsigned seed = (unsigned)time(NULL);
                    std::default_random_engine e(seed);
                    std::vector<int> candidate_files;
                    // LYH ADD
                    for (int i = 0; i < main_compaction_guard->file_metas.size(); i++) {
                        candidate_files.push_back(i);
                    }
	MyLog2("ran pick file1\n");
                                   std::shuffle(candidate_files.begin(), candidate_files.end(), e);
                    std::vector<uint64_t> tmp_files;
                    for (int i = 0; i < candidate_files.size(); i++) {
	MyLog2("ran pick file3 %d\n", i);
                         tmp_files.push_back(main_compaction_guard->file_metas[candidate_files[i]]->number);
                        uint64_t cross_zone = hm_manager_->get_guard_accross_zone(level, tmp_files);
                        if (tmp_files.size() != 1 && cross_zone > burst_write_control_zone_num) {
                            MyLog2("randomly pick guard %s of level %d cross %ld max %ld\n",
                                   main_compaction_guard->guard_key.DebugString().c_str(), level, cross_zone,
                                   burst_write_control_zone_num);
                            fflush(stdout);
                            break;
                        }
                        files_in_main_compaction_guards[main_compaction_guard->file_metas[candidate_files[i]]->number] =
                            1;
                    }
                    if (tmp_files.size() == 0)
                        for (int i = 0; i < main_compaction_guard->number_segments; i++) {
                            MyLog2("fn %ld\n", main_compaction_guard->file_metas[i]->number);
                        }
                    assert(tmp_files.size() != 0);
                } else {
                    // sequentially pick the files to do compaction until the zones involved have reached the limit
                    std::vector<uint64_t> tmp_files;
                    for (int i = 0; i < main_compaction_guard->number_segments; i++) {
                        tmp_files.push_back(main_compaction_guard->file_metas[i]->number);
                        uint64_t cross_zone = hm_manager_->get_guard_accross_zone(level, tmp_files);
                        if (tmp_files.size() != 1 && cross_zone > burst_write_control_zone_num) {
                            MyLog2("pick guard %s of level %d cross %ld max %ld\n",
                                   main_compaction_guard->guard_key.DebugString().c_str(), level, cross_zone,
                                   burst_write_control_zone_num);
                            fflush(stdout);
                            break;
                        }
                        files_in_main_compaction_guards[main_compaction_guard->file_metas[i]->number] = 1;
                    }
                    if (tmp_files.size() == 0)
                        for (int i = 0; i < main_compaction_guard->number_segments; i++) {
                            MyLog2("fn %ld\n", main_compaction_guard->file_metas[i]->number);
                        }
                    assert(tmp_files.size() != 0);
                }
            }
        }

        // reset the file to compact
        current_->file_to_compact_ = NULL;
        current_->file_to_compact_level_ = -1;
MyLog2("what what \n");
        // finally get all files involved in compaction
        std::map<uint64_t, uint64_t> zone_added_in_compaction;
        std::map<uint64_t, uint64_t> files_added_in_compaction = hm_manager_->get_zone_files_based_on_files(
            level, files_in_main_compaction_guards, zone_added_in_compaction);
        // debug
        MyLog2("level %d files used to do compaction ", level);
        assert(files_added_in_compaction.size() != 0);
        for (std::map<uint64_t, uint64_t>::iterator files_added_in_compaction_it = files_added_in_compaction.begin();
             files_added_in_compaction_it != files_added_in_compaction.end(); files_added_in_compaction_it++) {
            MyLog2("%ld ", files_added_in_compaction_it->first);
        }
        MyLog2("\n");
        // see if all the sentinel file are involved in compaction
        // if yes, add the complete guards that cause split sentinel files
        int sen_files_added = 0;
        uint64_t total_filesize_added = 0;
        uint64_t full_guard_file_size_added = 0;
        int candidate_guards_iter = 0;
        for (unsigned i = 0; i < v->sentinel_files_[level].size(); i++) {
            if (files_added_in_compaction[v->sentinel_files_[level][i]->number] == 1) {
                // push file
                // no need to push guard
                // push senfile
                if (main_compaction_guard_is_sentinel)
                    pm->file_metas.push_back(v->sentinel_files_[level][i]);
                c->inputs_[0].push_back(v->sentinel_files_[level][i]);
                c->sentinel_inputs_[0].push_back(v->sentinel_files_[level][i]);
                sen_files_added++;
                total_filesize_added += v->sentinel_files_[level][i]->file_size;
                // this file has been delete
                if (current_->file_to_compact_ != NULL) {
                    if (current_->file_to_compact_->number == v->sentinel_files_[level][i]->number) {
                        current_->file_to_compact_ = NULL;
                        current_->file_to_compact_level_ = -1;
                    }
                }
            }
        }
        if (sen_files_added == v->sentinel_files_[level].size()) {
            // if sentinel will be splited by the candidate complete guard, then add this candidate complete guard as
            // real complete guard without cost
            full_guard_file_size_added += total_filesize_added;
            if (pm->is_sen.size() != 0)
                pm->is_full_guard_added = true;
            for (candidate_guards_iter = 0; candidate_guards_iter < candidate_guards_added_to_compaction[0].size();
                 candidate_guards_iter++) {
                if (candidate_guards_added_to_compaction[0][candidate_guards_iter] == nullptr) {
                    complete_guards_added_to_compaction[0].push_back(
                        candidate_complete_guards_added_to_compaction
                            [0][candidate_guards_iter]);  // add the complete guards that cause split sentinel files
                    if (pm->is_sen.size() != 0)
                        pm->cg.push_back(candidate_complete_guards_added_to_compaction[0][candidate_guards_iter]);
                } else {
                    break;
                }
            }
        }

        // see if all the file of a guard are involved in compaction
        // if yes, add the complete guards that cause split guard files
        guards = v->guards_[level];
        candidate_guards_iter = 0;
        for (int guard_index_iter = 0; guard_index_iter < guards.size(); guard_index_iter++) {
            if (guards[guard_index_iter] == nullptr)
                continue;
            GuardMetaData *g = guards[guard_index_iter];
            int files_added = 0;
            int last_input_size = c->inputs_[0].size();
            uint64_t tmp_file_size_added = 0;
            for (int j = 0; j < g->number_segments; j++) {
                if (files_added_in_compaction[g->file_metas[j]->number] == 1) {
                    if (main_compaction_guard == g) {
                        pm->file_metas.push_back(g->file_metas[j]);
                    }
                    c->inputs_[0].push_back(g->file_metas[j]);
                    files_added++;
                    total_filesize_added += g->file_metas[j]->file_size;
                    tmp_file_size_added += g->file_metas[j]->file_size;
                    // push file

                    // this file has been delete
                    if (current_->file_to_compact_ != NULL) {
                        if (current_->file_to_compact_->number == g->file_metas[j]->number) {
                            current_->file_to_compact_ = NULL;
                            current_->file_to_compact_level_ = -1;
                        }
                    }
                }
            }
            // put guard
            if (files_added == 0)
                continue;
            else if (files_added == g->number_segments) {
                // used old guard to iterate
                c->guard_inputs_[0].push_back(g);
                full_guard_added_in_compaction->push_back(g);
                full_guard_file_size_added += tmp_file_size_added;
                // if this guard will be splited by the candidate complete guard, then add this candidate complete guard
                // as real complete guard without cost assume both are sorted!
                if (pm->is_sen.size() == 0 && g == main_compaction_guard) {
                    pm->is_full_guard_added = true;
                }
                int new_add_com_guard = 0;
                for (candidate_guards_iter = 0; candidate_guards_iter < candidate_guards_added_to_compaction[0].size();
                     candidate_guards_iter++) {
                    if (candidate_guards_added_to_compaction[0][candidate_guards_iter] != nullptr) {
                        int compare = icmp_.user_comparator()->Compare(
                            guards[guard_index_iter]->guard_key.user_key(),
                            candidate_guards_added_to_compaction[0][candidate_guards_iter]->guard_key.user_key());
                        if (compare == 0) {
                            if (candidate_complete_guards_added_to_compaction[0][candidate_guards_iter] != nullptr) {
                                complete_guards_added_to_compaction[0].push_back(
                                    candidate_complete_guards_added_to_compaction
                                        [0][candidate_guards_iter]);  // add the complete guards that cause split
                                                                      // guard files

                                new_add_com_guard++;
                                if (pm->is_sen.size() == 0 && g == main_compaction_guard) {
                                    pm->cg.push_back(
                                        candidate_complete_guards_added_to_compaction[0][candidate_guards_iter]);
                                }
                            }
                        } else if (compare < 0) {
                            break;
                        }
                    }
                }
                MyLog2("going to persist %d complete guard\n", new_add_com_guard);
            } else {
                MyLog2("guard %s added files %d of %ld\n", g->guard_key.DebugString().c_str(), files_added,
                       g->number_segments);
                // new guard to iterate
                // WATCH OUT. You are creating a new object, make sure to delete it after processing.
                GuardMetaData *new_g = new GuardMetaData;
                new_g->guard_key = g->guard_key;
                new_g->smallest = g->smallest;
                new_g->largest = g->largest;
                new_g->number_segments = 0;

                for (int k = last_input_size; k < c->inputs_[0].size(); k++) {
                    FileMetaData *f = c->inputs_[0][k];
                    // Add this file to the guard
                    new_g->file_metas.push_back(f);
                    new_g->files.push_back(f->number);
                    new_g->number_segments++;
                }
                c->guard_inputs_[0].push_back(new_g);
                assert(files_added == new_g->number_segments);
            }
        }
        MyLog2(" total_filesize_added %ld full_guard_file_size_added %ld ratio %lf", total_filesize_added,
               full_guard_file_size_added, (full_guard_file_size_added * 1.0) / total_filesize_added);
        // for horizontal compactioin, it can place into the same zone. This will cause invalid SSTable, to avoid this,
        // we need to open a new open zone.
        if (horizontal_compaction) {
            uint64_t out_zone = hm_manager_->move_out_open_zone(level, zone_added_in_compaction);
            MyLog2("move out level 6 zone num %ld\n", out_zone);
        }
        // invalid SSTable can come from "flush", but very little
        if (MOVE_OUT_LEVEL_0_ZONE && level == 0) {
            uint64_t out_zone = hm_manager_->move_out_open_zone(level, zone_added_in_compaction);
            MyLog2("move out level 0 zone num %ld\n", out_zone);
        }

        for (int which = 0; which < num_input_levels_for_compaction; which++) {
            // add all complete guard involved in compaction
            std::sort(complete_guards_added_to_compaction[which].begin(),
                      complete_guards_added_to_compaction[which].end(), guard_comparator);
            // Remove duplicates
            if (complete_guards_added_to_compaction[which].size() > 1) {
                GuardMetaData *prev = complete_guards_added_to_compaction[which][0];
                std::vector<GuardMetaData *>::iterator it = complete_guards_added_to_compaction[which].begin();
                it++;
                for (; it != complete_guards_added_to_compaction[which].end();) {
                    if (icmp_.user_comparator()->Compare((*it)->guard_key.user_key(), prev->guard_key.user_key()) ==
                        0) {
                        it = complete_guards_added_to_compaction[which].erase(it);
                    } else {
                        prev = (*it);
                        ++it;
                    }
                }
            }

            // debug
            if (PRINTOUT_DEBUG) {
                std::string r;
                MyLog2("level %d ", level + which);
                r.append("final guard use to do compaction: \n");
                for (int i = 0; i < complete_guards_added_to_compaction[which].size(); i++) {
                    GuardMetaData *g = complete_guards_added_to_compaction[which][i];
                    assert(g != nullptr);
                    r.append(": ").append(g->guard_key.DebugString()).append("\n");
                }
                MyLog2("%s", r.c_str());
            }
        }
        std::sort(full_guard_added_in_compaction->begin(), full_guard_added_in_compaction->end(), guard_comparator);
        if (PRINTOUT_DEBUG) {
            // debug
            std::string r;
            MyLog2("level %d ", level);
            r.append("full guard do compaction: \n");
            for (int i = 0; i < full_guard_added_in_compaction->size(); i++) {
                GuardMetaData *g = (*full_guard_added_in_compaction)[i];
                assert(g != nullptr);
                r.append(": ").append(g->guard_key.DebugString()).append("\n");
            }
            MyLog2("%s\n", r.c_str());
        }

        if (!horizontal_compaction) {
            complete_guards_used_in_bg_compaction->insert(complete_guards_used_in_bg_compaction->begin(),
                                                          complete_guards_added_to_compaction[1].begin(),
                                                          complete_guards_added_to_compaction[1].end());
            complete_guards_used_in_bg_compaction_old_level->insert(
                complete_guards_used_in_bg_compaction_old_level->begin(),
                complete_guards_added_to_compaction[0].begin(), complete_guards_added_to_compaction[0].end());
        } else {
            complete_guards_used_in_bg_compaction->insert(complete_guards_used_in_bg_compaction->begin(),
                                                          complete_guards_added_to_compaction[0].begin(),
                                                          complete_guards_added_to_compaction[0].end());
        }

        // debug
        MyLog2("file need to do comp ");
        for (int i = 0; i < c->inputs_[0].size(); i++) {
            MyLog2("%ld ", c->inputs_[0][i]->number);
        }
        MyLog2("\n");
        if (PRINTOUT_DEBUG) {
            int total_files = v->sentinel_files_[level].size();
            for (int i = 0; i < v->guards_[level].size(); i++) {
                total_files += v->guards_[level][i]->file_metas.size();
            }
            if (total_files == c->inputs_[0].size()) {
                if (!horizontal_compaction) {
                    if (complete_guards_used_in_bg_compaction_old_level[0].size() + reduce_com_num[0] !=
                        v->complete_guards_[level].size()) {
                        MyLog2("used to compaction guard %ld complete guard %ld rm duplicate com %d",
                               complete_guards_used_in_bg_compaction_old_level[0].size(),
                               v->complete_guards_[level].size(), reduce_com_num[0]);
                        fflush(stdout);
                        assert(0);
                    }
                } else {
                    if (complete_guards_used_in_bg_compaction[0].size() != v->complete_guards_[level].size()) {
                        MyLog2("used to compaction guard %ld complete guard %ld",
                               complete_guards_used_in_bg_compaction[0].size(), v->complete_guards_[level].size());
                        fflush(stdout);
                        assert(0);
                    }
                }
            }
        }
        return c;
    }

    // Below commented functions are used by HyperLevelDB
    /*
     * Compaction* VersionSet::PickCompaction(Version* v, unsigned level) {
      assert(level < config::kNumLevels);
      bool trivial = false;

      if (v->files_[level].empty()) {
        return NULL;
      }

      Compaction* c = new Compaction(level);
      c->input_version_ = v;
      c->input_version_->Ref();

      if (level > 0) {
        std::vector<FileMetaData*> LA;
        std::vector<FileMetaData*> LB;
        std::vector<uint64_t> LA_sizes;
        std::vector<uint64_t> LB_sizes;
        std::vector<CompactionBoundary> boundaries;
        GetCompactionBoundaries(v, level, &LA, &LB, &LA_sizes, &LB_sizes, &boundaries);

        // find the best set of files: maximize the ratio of sizeof(LA)/sizeof(LB)
        // while keeping sizeof(LA)+sizeof(LB) < some threshold.  If there's a tie
        // for ratio, minimize size.
        size_t best_idx_start = 0;
        size_t best_idx_limit = 0;
        uint64_t best_size = 0;
        double best_ratio = -1;
        for (size_t i = 0; i < boundaries.size(); ++i) {
          for (size_t j = i; j < boundaries.size(); ++j) {
            uint64_t sz_a = LA_sizes[j + 1] - LA_sizes[i];
            uint64_t sz_b = LB_sizes[boundaries[j].limit] - LB_sizes[boundaries[i].start];
            if (boundaries[j].start == boundaries[j].limit) {
              trivial = true;
              break;
            }
            if (sz_a + sz_b >= MaxCompactionBytesForLevel(level)) {
              break;
            }
            assert(sz_b > 0); // true because we exclude trivial moves
            double ratio = double(sz_a) / double(sz_b);
            if (ratio > best_ratio ||
                (ratio >= best_ratio && sz_a + sz_b < best_size)) {
              best_ratio = ratio;
              best_size = sz_a + sz_b;
              best_idx_start = i;
              best_idx_limit = j + 1;
            }
          }
        }

        // Trivial moves have a near-0 cost, so do them first.
        if (trivial) {
          for (size_t i = 0; i < LA.size(); ++i) {
            if (boundaries[i].start == boundaries[i].limit) {
              c->inputs_[0].push_back(LA[i]);
            }
          }
          trivial = level != 0;
        // go with the best ratio
        } else if (best_ratio >= 0.0) {
          for (size_t i = best_idx_start; i < best_idx_limit; ++i) {
            assert(i < LA.size());
            c->inputs_[0].push_back(LA[i]);
          }
          for (size_t i = boundaries[best_idx_start].start;
              i < boundaries[best_idx_limit - 1].limit; ++i) {
            assert(i < LB.size());
            c->inputs_[1].push_back(LB[i]);
          }
        // pick the file to compact in this level
        } else if (v->file_to_compact_ != NULL) {
          c->inputs_[0].push_back(v->file_to_compact_);
        // otherwise just pick the file with least overlap
        } else {
          assert(level+1 < config::kNumLevels);
          // Pick the file that overlaps with the fewest files in the next level
          size_t smallest = boundaries.size();
          for (size_t i = 0; i < boundaries.size(); ++i) {
            if (smallest == boundaries.size() ||
                boundaries[smallest].limit - boundaries[smallest].start >
                boundaries[i].limit - boundaries[i].start) {
              smallest = i;
            }
          }
          assert(smallest < boundaries.size());
          c->inputs_[0].push_back(LA[smallest]);
          for (size_t i = boundaries[smallest].start; i < boundaries[smallest].limit; ++i) {
            c->inputs_[1].push_back(LB[i]);
          }
        }
      } else {
        std::vector<FileMetaData*> tmp(v->files_[0]);
        std::sort(tmp.begin(), tmp.end(), OldestFirst);
        for (size_t i = 0; i < tmp.size() && c->inputs_[0].size() < 32; ++i) {
            c->inputs_[0].push_back(tmp[i]);
        }
        trivial = false;
      }

      if (!trivial) {
        SetupOtherInputs(c);
      }
      return c;
    }

    void VersionSet::SetupOtherInputs(Compaction* c) {
      const unsigned level = c->level();
      assert(level + 1 < config::kNumLevels);
      InternalKey smallest, largest;
      GetRange(c->inputs_[0], &smallest, &largest);
      c->input_version_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);
      if (level + 2 < config::kNumLevels) {
        const Comparator* user_cmp = icmp_.user_comparator();
        std::vector<FileMetaData*> tmp;
        c->input_version_->GetOverlappingInputs(level + 2, &smallest, &largest, &tmp);
        for (size_t i = 0; i < tmp.size(); ++i) {
          leveldb::Slice boundary1 = tmp[i]->smallest.user_key();
          leveldb::Slice boundary2 = tmp[i]->largest.user_key();
          c->boundaries_.push_back(std::make_pair(user_cmp->KeyNum(boundary1), boundary1));
          c->boundaries_.push_back(std::make_pair(user_cmp->KeyNum(boundary2), boundary2));
        }
      }

      // Update the place where we will do the next compaction for this level.
      // We update this immediately instead of waiting for the VersionEdit
      // to be applied so that if the compaction fails, we will try a different
      // key range next time.
      //compact_pointer_[level] = largest.Encode().ToString();
      c->edit_.SetCompactPointer(level, largest);
    }
    */

    Compaction *VersionSet::CompactRange(unsigned level, const InternalKey *begin, const InternalKey *end)
    {
        std::vector<FileMetaData *> inputs[2];
        std::vector<FileMetaData *> sentinel_inputs[2];
        std::vector<GuardMetaData *> guard_inputs[2];

        current_->GetOverlappingInputsGuards(level, begin, end, &inputs[0], &guard_inputs[0], &sentinel_inputs[0]);
        current_->GetOverlappingInputsGuards(level + 1, begin, end, &inputs[1], &guard_inputs[1], &sentinel_inputs[1]);
        if (inputs[0].empty()) {
            return NULL;
        }

        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file if the
        // two files overlap.
        // TODO Commenting below lines - not taking care of optimization for now
        /*if (level > 0) {
          const uint64_t limit = MaxFileSizeForLevel(level);
          uint64_t total = 0;
          for (size_t i = 0; i < inputs.size(); i++) {
            uint64_t s = inputs[i]->file_size;
            total += s;
            if (total >= limit) {
              inputs.resize(i + 1);
              break;
            }
          }
        }*/

        Compaction *c = new Compaction(level);
        c->input_version_ = current_;
        c->input_version_->Ref();
        c->inputs_[0] = inputs[0];
        c->guard_inputs_[0] = guard_inputs[0];
        c->sentinel_inputs_[0] = sentinel_inputs[0];

        c->inputs_[1] = inputs[1];
        c->guard_inputs_[1] = guard_inputs[1];
        c->sentinel_inputs_[1] = sentinel_inputs[1];
        c->is_horizontal_compaction = false;  // Not sure if we should do horizontal compaction during manual compaction
        return c;
    }

    Compaction::Compaction(unsigned l)
        : level_(l), min_output_file_size_(MinFileSizeForLevel(l)), max_output_file_size_(MaxFileSizeForLevel(l)),
          input_version_(NULL), edit_(), boundaries_()
    {
        for (unsigned i = 0; i < config::kNumLevels; i++) {
            level_ptrs_[i] = 0;
        }
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunsafe-loop-optimizations"

    Compaction::~Compaction()
    {
        if (input_version_ != NULL) {
            input_version_->Unref();
        }
        // TODO delete guard_inputs_ and sentinel_inputs_ here?
    }

#pragma GCC diagnostic pop

    bool Compaction::CrossesBoundary(const ParsedInternalKey &old_key, const ParsedInternalKey &new_key,
                                     size_t *hint) const
    {
        if (boundaries_.empty()) {
            return false;
        }
        const Comparator *user_cmp = input_version_->vset_->icmp_.user_comparator();
        uint64_t lower_num = user_cmp->KeyNum(old_key.user_key);
        uint64_t upper_num = user_cmp->KeyNum(new_key.user_key);
        while (*hint < boundaries_.size()) {
            assert(lower_num < upper_num ||
                   (lower_num == upper_num && user_cmp->Compare(old_key.user_key, new_key.user_key) <= 0));
            bool lower = lower_num < boundaries_[*hint].first ||
                         (lower_num == boundaries_[*hint].first &&
                          user_cmp->Compare(old_key.user_key, boundaries_[*hint].second) <= 0);
            bool upper = upper_num > boundaries_[*hint].first ||
                         (upper_num == boundaries_[*hint].first &&
                          user_cmp->Compare(new_key.user_key, boundaries_[*hint].second) > 0);
            if (lower && upper) {
                return true;
            } else if (!upper) {
                return false;
            } else if (!lower) {
                ++(*hint);
            }
        }
        return false;
    }

    bool Compaction::IsTrivialMove() const
    {
        return num_input_files(1) == 0;
    }

    void Compaction::AddInputDeletions(VersionEdit *ed)
    {
        for (int which = 0; which < 2; which++) {
            for (size_t i = 0; i < inputs_[which].size(); i++) {
                ed->DeleteFile(level_ + which, inputs_[which][i]->number);
            }
        }
    }

    std::string Compaction::DebugString()
    {
        std::string r;
        r.append("Compaction level --> ");
        AppendNumberTo(&r, level_);
        r.append("\n");
        for (unsigned level = 0; level < 2; level++) {
            // E.g.,
            //   --- level 1 ---
            //   17:123['a' .. 'd']
            //   20:43['e' .. 'g']
            r.append("--------------------- which ");
            AppendNumberTo(&r, level);
            r.append(" ---------------------\n");

            // Appending file information
            const std::vector<FileMetaData *> &files = inputs_[level];
            r.append(" ------ Input files ------\n");
            r.append(" num_files: ");
            AppendNumberTo(&r, files.size());
            r.append("\n");
            for (size_t i = 0; i < files.size(); i++) {
                r.push_back(' ');
                AppendNumberTo(&r, files[i]->number);
                r.push_back(':');
                AppendNumberTo(&r, files[i]->file_size);
                r.append("[");
                r.append(files[i]->smallest.DebugString());
                r.append(" .. ");
                r.append(files[i]->largest.DebugString());
                r.append("]\n");
            }

            // Appending sentinel information
            const std::vector<FileMetaData *> &sentinel_files = sentinel_inputs_[level];
            r.append(" ------ Sentinel files ------\n");
            r.append(" num_sentinel_files: ");
            AppendNumberTo(&r, sentinel_files.size());
            r.append("\n");
            for (size_t i = 0; i < sentinel_files.size(); i++) {
                r.push_back(' ');
                AppendNumberTo(&r, sentinel_files[i]->number);
                r.push_back(':');
                AppendNumberTo(&r, sentinel_files[i]->file_size);
                r.append("[");
                r.append(sentinel_files[i]->smallest.DebugString());
                r.append(" .. ");
                r.append(sentinel_files[i]->largest.DebugString());
                r.append("]\n");
            }

            // Appending guard information
            const std::vector<GuardMetaData *> &guards = guard_inputs_[level];
            r.append(" ------ Guards ------\n");
            r.append(" num_guards: ");
            AppendNumberTo(&r, guards.size());
            r.append("\n");
            r.append(" num_guard_files_in_level: ");
            r.append("\n");
            for (size_t i = 0; i < guards.size(); i++) {
                GuardMetaData *g = guards[i];
                r.push_back(' ');
                r.append("guard_key: ").append(g->guard_key.DebugString()).append("\n");
                r.append(" guard_range: [");
                r.append(g->smallest.DebugString()).append(" .. ").append(g->largest.DebugString()).append("]\n");
                r.append(" num_segments: ");
                AppendNumberTo(&r, g->number_segments);
                r.append("\n");
                r.append(" files: [");
                for (size_t i = 0; i < g->files.size(); i++) {
                    if (i > 0) {
                        r.push_back(',');
                    }
                    AppendNumberTo(&r, g->files[i]);
                }
                r.append("]\n");
            }
        }
        return r;
    }

    bool Compaction::IsBaseLevelForKey(const Slice &user_key)
    {
        // Maybe use binary search to find right entry instead of linear search?
        const Comparator *user_cmp = input_version_->vset_->icmp_.user_comparator();
        for (unsigned lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
            const std::vector<FileMetaData *> &files = input_version_->files_[lvl];
            for (; level_ptrs_[lvl] < files.size();) {
                FileMetaData *f = files[level_ptrs_[lvl]];
                if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
                    // We've advanced far enough
                    if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
                        // Key falls in this file's range, so definitely not base level
                        return false;
                    }
                    break;
                }
                level_ptrs_[lvl]++;
            }
        }
        return true;
    }

    void Compaction::ReleaseInputs()
    {
        if (input_version_ != NULL) {
            input_version_->Unref();
            input_version_ = NULL;
        }
    }

}  // namespace leveldb
