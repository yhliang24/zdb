// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include "hm/my_log.h"

#define G_ALLOW_SEEK 5

namespace leveldb {

    class VersionSet;

    struct GuardMetaData;

    struct FileMetaData {
        int refs;
        int allowed_seeks;  // Seeks allowed until compaction
        uint64_t number;
        uint64_t file_size;      // File size in bytes
        InternalKey smallest;    // Smallest internal key served by table
        InternalKey largest;     // Largest internal key served by table
        GuardMetaData *guard;    // The guard that the file belongs to.
        int64_t been_rewritten;  // tmperaliy not using this parameter
        int64_t disk_offset;

        FileMetaData()
            : refs(0), allowed_seeks(1 << 30), number(0), file_size(0), smallest(), largest(), guard(nullptr),
              been_rewritten(0), disk_offset(-1)
        {
        }
    };

    /*
    guard_key is the smallest key served by the guard file. In each level,
    there can be only one guard starting with a given key, so (level, key)
    uniquely identifies a guard.
    */
    struct GuardMetaData {
        int refs;
        int level;
        uint64_t number_segments;
        InternalKey guard_key;  // guard key is selected before any keys are inserted
        /* Need not be same as guard_key. Ex: g: 100, smallest: 102 */
        InternalKey smallest;
        InternalKey largest;  // Largest internal key served by table
        // The list of file numbers that form a part of this guard.
        std::vector<uint64_t> files;
        std::vector<FileMetaData *> file_metas;
        int guard_allow_seeks;

        // LYH
        uint64_t last_guard_size;

        GuardMetaData()
            : refs(0), level(-1), guard_key(), smallest(), largest(), number_segments(0), last_guard_size(0),
              guard_allow_seeks(G_ALLOW_SEEK)
        {
            files.clear();
        }
    };

    struct PiggyMain {
        int level;
        std::vector<FileMetaData *> is_sen;
        int is_full_guard_added;
        GuardMetaData *g;
        std::vector<GuardMetaData *> cg;
        std::vector<FileMetaData *> file_metas;
        PiggyMain() : g(nullptr), level(-1), is_full_guard_added(false)
        {
            file_metas.clear();
            cg.clear();
            is_sen.clear();
        }
        void Debug()
        {
            if (is_sen.size() != 0)
                MyLog2("pm level %d sen %d full %d g sen cgsize %d filecount %d totalfile %d\n", level, is_sen.size(),
                       is_full_guard_added, cg.size(), file_metas.size(), is_sen.size());
            else
                MyLog2("pm level %d sen %d full %d g %s cgsize %d filecount %d totalfile %d\n", level, is_sen.size(),
                       is_full_guard_added, g->guard_key.DebugString().c_str(), cg.size(), file_metas.size(),
                       g->file_metas.size());
        }
    };

    class VersionEdit
    {
      public:
        VersionEdit()
            : comparator_(), log_number_(), prev_log_number_(), next_file_number_(), last_sequence_(),
              has_comparator_(), has_log_number_(), has_prev_log_number_(), has_next_file_number_(),
              has_last_sequence_(), compact_pointers_(), deleted_files_(), new_files_()
        {
            Clear();
        }
        ~VersionEdit()
        {
        }

        void Clear();

        void SetComparatorName(const Slice &name)
        {
            has_comparator_ = true;
            comparator_ = name.ToString();
        }
        void SetLogNumber(uint64_t num)
        {
            has_log_number_ = true;
            log_number_ = num;
        }
        void SetPrevLogNumber(uint64_t num)
        {
            has_prev_log_number_ = true;
            prev_log_number_ = num;
        }
        void SetNextFile(uint64_t num)
        {
            has_next_file_number_ = true;
            next_file_number_ = num;
        }
        void SetLastSequence(SequenceNumber seq)
        {
            has_last_sequence_ = true;
            last_sequence_ = seq;
        }
        void SetCompactPointer(int level, const InternalKey &key)
        {
            compact_pointers_.push_back(std::make_pair(level, key));
        }

        // Add the specified file at the specified number.
        // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
        // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
        void AddFile(int level, uint64_t file, uint64_t file_size, const InternalKey &smallest,
                     const InternalKey &largest, int64_t disk_offset = -1)
        {
            FileMetaData f;
            f.number = file;
            f.file_size = file_size;
            f.smallest = smallest;
            f.largest = largest;
            f.disk_offset = disk_offset;
            new_files_.push_back(std::make_pair(level, f));
        }

        /* Add the file to the appropriate sentinel. */
        void AddFileToSentinel(FileMetaData *f, int level)
        {
            assert(level >= 0 && level < config::kNumLevels);
            FileMetaData ff = *f;
            sentinel_files_[level].push_back(ff);
        }

        void AddSentinelFile(int level, int allowed_seeks, uint64_t file_size, GuardMetaData *g, InternalKey largest,
                             InternalKey smallest, uint64_t number, int refs, int64_t been_rewritten)
        {
            FileMetaData meta;
            meta.allowed_seeks = allowed_seeks;
            meta.file_size = file_size;
            meta.guard = g;
            meta.largest = largest;
            meta.smallest = smallest;
            meta.number = number;
            meta.refs = refs;
            meta.been_rewritten = been_rewritten;
            sentinel_files_[level].push_back(meta);
        }

        void AddSentinelFileNo(int level, uint64_t number)
        {
            sentinel_file_nos_[level].push_back(number);
        }

        void AddGuard(int level, const InternalKey &guard_key)
        {
            assert(level >= 0 && level < config::kNumLevels);
            GuardMetaData g;
            g.guard_key = guard_key;
            g.level = level;
            g.number_segments = 0;
            new_guards_[level].push_back(g);
        }

        void AddCompleteGuard(int level, const InternalKey &guard_key)
        {
            assert(level >= 0 && level < config::kNumLevels);
            GuardMetaData g;
            g.guard_key = guard_key;
            g.level = level;
            g.number_segments = 0;
            new_complete_guards_[level].push_back(g);
        }

        void AddGuardFromExisting(int level, GuardMetaData *g)
        {
            assert(level >= 0 && level < config::kNumLevels);
            GuardMetaData new_g(*g);
            new_guards_[level].push_back(new_g);
        }

        void AddCompleteGuardFromExisting(int level, GuardMetaData *g)
        {
            assert(level >= 0 && level < config::kNumLevels);
            GuardMetaData new_g(*g);
            new_complete_guards_[level].push_back(new_g);
        }

        void AddFullGuardAddedInCompaction(int level, GuardMetaData *g)
        {
            assert(level >= 0 && level < config::kNumLevels);
            full_guard_added_in_compaction[level].push_back(g);
        }

        void SetIsRewrite(int level, bool a)
        {
            is_rewrite[level] = a;
        }

        void SetComLevel(int level)
        {
            compact_level = level;
        }

        void SetDupRatio(int level, double ratio)
        {
            dup_ratio[level] = ratio;
        }

        void SetBeforeSize(int level, double ratio)
        {
            before_size[level] = ratio;
        }

        void SetAfterSize(int level, double ratio)
        {
            after_size[level] = ratio;
        }

        void SetPM(PiggyMain *pm)
        {
            piggy_level = pm->level;
            piggy_sen.swap(pm->is_sen);
            piggy_full = pm->is_full_guard_added;
            piggy_g = pm->g;
            piggy_cg.swap(pm->cg);
            piggy_file_metas.swap(pm->file_metas);
        }

        void SetStaleRatio(double ratio = -1.0)
        {
            compaction_remove_stale_ratio = ratio;
            // printf("set compaction_remove_stale_ratio %lf\n", ratio);
            return;
        }

        /* A version of AddGuard that contains files. */
        void AddGuardWithFiles(int level, uint64_t number_segments, const InternalKey &guard_key,
                               const InternalKey &smallest, const InternalKey &largest,
                               const std::vector<uint64_t> files)
        {
            assert(level >= 0 && level < config::kNumLevels);
            GuardMetaData g;
            g.guard_key = guard_key;
            g.level = level;
            g.smallest = smallest;
            g.largest = largest;
            g.number_segments = number_segments;
            g.files.insert(g.files.end(), files.begin(), files.end());
            new_guards_[level].push_back(g);
        }

        // Delete the specified "file" from the specified "level".
        void DeleteFile(int level, uint64_t file)
        {
            deleted_files_.insert(std::make_pair(level, file));
        }

        void DeleteGuard(int level, InternalKey guard)
        {
            deleted_guards_.insert(std::make_pair(level, guard));
        }

        void DeleteSentinelFile(int level, uint64_t file)
        {
            deleted_sentinel_files_.insert(std::make_pair(level, file));
        }

        void EncodeTo(std::string *dst) const;
        Status DecodeFrom(const Slice &src);

        std::string DebugString() const;

      private:
        friend class VersionSet;

        typedef std::pair<int, InternalKey> GuardPair;

        // Create struct to allow comparing two pairs of level and internal keys.
        struct BySmallestPair {
            const InternalKeyComparator *internal_comparator;

            bool operator()(GuardPair p1, GuardPair p2) const
            {
                if (p1.first != p2.first) {
                    return (p1.first < p2.first);
                } else {
                    int r = internal_comparator->Compare(p1.second, p2.second);
                    return (r < 0);
                }
            }
        };

        typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;
        typedef std::set<std::pair<int, InternalKey>, BySmallestPair> DeletedGuardSet;

        std::string comparator_;
        uint64_t log_number_;
        uint64_t prev_log_number_;
        uint64_t next_file_number_;
        SequenceNumber last_sequence_;
        bool has_comparator_;
        bool has_log_number_;
        bool has_prev_log_number_;
        bool has_next_file_number_;
        bool has_last_sequence_;

        std::vector<std::pair<int, InternalKey>> compact_pointers_;
        DeletedFileSet deleted_files_;
        std::vector<std::pair<int, FileMetaData>> new_files_;

        /* Structures to contain files for the sentinel guards. */
        std::vector<FileMetaData> sentinel_files_[config::kNumLevels];
        std::vector<uint64_t> sentinel_file_nos_[config::kNumLevels];
        DeletedFileSet deleted_sentinel_files_;

        std::vector<GuardMetaData> new_guards_[config::kNumLevels];
        std::vector<GuardMetaData> new_complete_guards_[config::kNumLevels];
        DeletedGuardSet deleted_guards_;

        // Compact full guard
        std::vector<GuardMetaData *> full_guard_added_in_compaction[config::kNumLevels];

        bool is_rewrite[config::kNumLevels];

        // used to calculate the piggy main
        int piggy_level;
        std::vector<FileMetaData *> piggy_sen;
        bool piggy_full;
        GuardMetaData *piggy_g;
        std::vector<FileMetaData *> piggy_file_metas;
        std::vector<GuardMetaData *> piggy_cg;  // the file in g may split into multiple guard
        //

        double dup_ratio[config::kNumLevels];
        double before_size[config::kNumLevels];  // must be set
        double after_size[config::kNumLevels];   // must be set
        int64_t compact_level;
        double compaction_remove_stale_ratio;  // -1.0: memtable->sstable, other compaction stale ratio
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
