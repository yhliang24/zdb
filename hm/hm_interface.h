
#include "../hm/my_log.h"
#include "../hm/hm_status.h"
#include <stdio.h>
#include <stdlib.h>
#include <cstring>

extern "C"
{
#include <libzbc/zbc.h>
#include <libzbd/zbd.h>
}

#define SECTOR_SIZE 4096
#define READ_MAX_SIZE_PER_IO 4 * 1024 * 1024
namespace leveldb
{

    // describe a zone
    struct hm_zone
    {
        /**
         * Zone length in number of get_library_unit Unit
         */
        uint64_t hm_length;

        /**
         * First sector of the zone (get_library_unit Unit)
         */
        uint64_t hm_start;

        /**
         * Zone write pointer sector position (get_library_unit Unit).
         */
        uint64_t hm_write_pointer;

        /**
         * Zone type (enum zbc_zone_type).
         */
        uint64_t hm_type;
    };

    class hm_interface
    {
    public:
        hm_interface(){};
        virtual ~hm_interface(){};
        virtual int hm_reset_zone_wait_for_complete(uint64_t sector) = 0; // in get_library_unit unit
        virtual int hm_reset_all_zone_wait_for_complete() = 0;
        virtual int hm_finish_zone_wait_for_complete(uint64_t sector) = 0; // in get_library_unit unit
        virtual void get_one_zone_info_wait_for_complete(uint64_t zone, struct hm_zone *h_zone) = 0;
        virtual void get_all_zone_info_wait_for_complete(std::vector<struct hm_zone *> &zones) = 0;
        virtual uint32_t get_open_zone_resource_wait_for_complete() = 0;
        virtual ssize_t hm_interface_write(void *buf, size_t count, uint64_t offset) = 0; // in ALIGN_BLOCK_SIZE unit
        virtual ssize_t hm_interface_read(void *buf, size_t count, uint64_t offset) = 0;  // in ALIGN_BLOCK_SIZE unit
        virtual uint64_t get_zone_disk_num() = 0;
        virtual int64_t get_first_seq_zone_num() = 0;
        virtual int64_t get_library_unit() = 0; // the unit of hm_start/wp/capcity and write/read
    };

    class hmsmr_interface : public hm_interface
    {
    public:
        hmsmr_interface()
        {
            zbc_open(smr_filename, O_RDWR | O_DIRECT, &dev_);
            int ret = zbc_list_zones(dev_, 0, ZBC_RO_ALL, &zone_, &zonenum_); // get zone info
            if (ret < 0)
            {
                printf("open failed\n");
                fflush(stdout);
                exit(1);
            }
        };

        virtual ~hmsmr_interface()
        {
            printf("releasing ctrl...\n");
            if (zone_)
            {
                free(zone_);
            }
            if (dev_)
            {
                zbc_close(dev_);
            }
        };

        virtual int hm_reset_zone_wait_for_complete(uint64_t sector);
        virtual int hm_reset_all_zone_wait_for_complete();
        virtual int hm_finish_zone_wait_for_complete(uint64_t sector); // make zone full so that the active zone will
                                                                       // not exceed the resourse limit
        virtual void get_one_zone_info_wait_for_complete(uint64_t zone, struct hm_zone *h_zone);
        virtual uint32_t get_open_zone_resource_wait_for_complete();
        virtual void get_all_zone_info_wait_for_complete(std::vector<struct hm_zone *> &zones);
        virtual ssize_t hm_interface_write(void *buf, size_t count, uint64_t offset);
        virtual ssize_t hm_interface_read(void *buf, size_t count, uint64_t offset);
        virtual uint64_t get_zone_disk_num();
        virtual int64_t get_first_seq_zone_num();
        virtual int64_t get_library_unit()
        {
            return 512;
        }

    private:
        struct zbc_device *dev_;
        struct zbc_zone *zone_;
        unsigned int zonenum_;
    };

    class zbd_interface : public hm_interface
    {
    public:
        zbd_interface()
        {
            fd_ = zbd_open(drive_name, O_RDWR | O_DIRECT, &info_);
            uint64_t addr_space_sz = (uint64_t)(zonenum_ * info_.zone_size);
            int ret = zbd_list_zones(fd_, 0, 0, ZBD_RO_ALL, &zone_, &zonenum_); // get zone info
            block_sz_ = info_.lblock_size;
            printf("open %s zonenum %ld zone size %ld logical block size %ld", smr_filename, zonenum_, info_.zone_size,
                   block_sz_);
            if (ret < 0)
            {
                printf("open failed\n");
                fflush(stdout);
                exit(1);
            }
        };

        virtual ~zbd_interface()
        {
            printf("releasing ctrl...\n");
            if (zone_)
            {
                free(zone_);
            }
            zbd_close(fd_);
        };

        virtual int hm_reset_zone_wait_for_complete(uint64_t sector);
        virtual int hm_reset_all_zone_wait_for_complete();
        virtual int hm_finish_zone_wait_for_complete(uint64_t sector); // make zone full so that the active zone will
                                                                       // not exceed the resourse limit
        virtual void get_one_zone_info_wait_for_complete(uint64_t zone, struct hm_zone *h_zone);
        virtual uint32_t get_open_zone_resource_wait_for_complete();
        virtual void get_all_zone_info_wait_for_complete(std::vector<struct hm_zone *> &zones);
        virtual ssize_t hm_interface_write(void *buf, size_t count, uint64_t offset);
        virtual ssize_t hm_interface_read(void *buf, size_t count, uint64_t offset);
        virtual uint64_t get_zone_disk_num();
        virtual int64_t get_first_seq_zone_num();
        virtual int64_t get_library_unit()
        {
            return 1;
        }

    private:
        zbd_info info_;
        int fd_;
        struct zbd_zone *zone_;
        unsigned int zonenum_;
        uint64_t block_sz_;
    };

} // namespace leveldb
