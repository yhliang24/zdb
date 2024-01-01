#include "hm_interface.h"

namespace leveldb
{

    // HM-SMR
    ssize_t hmsmr_interface::hm_interface_write(void *buf, size_t count, uint64_t offset)
    {
        uint64_t sector_count = count * ALIGN_BLOCK_SIZE / 512;
        uint64_t sector_offset = offset * ALIGN_BLOCK_SIZE / 512;
        return zbc_pwrite(dev_, buf, sector_count, sector_offset);
    }

    ssize_t hmsmr_interface::hm_interface_read(void *buf, size_t count, uint64_t offset)
    {
        uint64_t sector_count = count * ALIGN_BLOCK_SIZE / 512;
        uint64_t sector_offset = offset * ALIGN_BLOCK_SIZE / 512;
        return zbc_pread(dev_, buf, sector_count, sector_offset);
    }

    uint64_t hmsmr_interface::get_zone_disk_num()
    {
        return zonenum_;
    }

    int64_t hmsmr_interface::get_first_seq_zone_num()
    {
        int i;
        for (i = 0; i < zonenum_; i++)
        {
            if (zone_[i].zbz_type == 2 || zone_[i].zbz_type == 3)
            {
                return i;
            }
        }
        return 0;
    }

    int hmsmr_interface::hm_reset_zone_wait_for_complete(uint64_t sector)
    {
        int ret = zbc_reset_zone(dev_, sector, 0);
        if (ret != 0)
        {
            MyLog("reset error\n");
            return ret;
        }
        return 0;
    }
    int hmsmr_interface::hm_reset_all_zone_wait_for_complete()
    {
        int ret = zbc_reset_zone(dev_, 0, 1);
        if (ret < 0)
        {
            MyLog("reset error\n");
            return ret;
        }
        return 0;
    }
    int hmsmr_interface::hm_finish_zone_wait_for_complete(uint64_t sector)
    {
        // hm-smr is no need to finish zone
        return 0;
    }
    void hmsmr_interface::get_one_zone_info_wait_for_complete(uint64_t zone, struct hm_zone *h_zone)
    {
        enum zbc_reporting_options ro = ZBC_RO_ALL;
        struct zbc_zone *zone_buf = (struct zbc_zone *)malloc(sizeof(struct zbc_zone));
        unsigned int num = 1;
        zbc_report_zones(dev_, zone_[zone].zbz_start, ro, zone_buf, &num);
        h_zone->hm_start = zone_buf->zbz_start;                 // get_library_unit() unit
        h_zone->hm_length = zone_buf->zbz_length;               // get_library_unit() unit
        h_zone->hm_write_pointer = zone_buf->zbz_write_pointer; // get_library_unit() unit
        h_zone->hm_type = zone_buf->zbz_type;
        free(zone_buf);
        return;
    }
    void hmsmr_interface::get_all_zone_info_wait_for_complete(std::vector<struct hm_zone *> &zones)
    {
        zbc_zone *tmp_zone_;
        zbc_list_zones(dev_, 0, ZBC_RO_ALL, &tmp_zone_, &zonenum_); // get zone info
        for (int i = 0; i < zonenum_; i++)
        {
            struct hm_zone *h_zone = new struct hm_zone();
            h_zone->hm_start = tmp_zone_[i].zbz_start;                 // get_library_unit() unit
            h_zone->hm_length = tmp_zone_[i].zbz_length;               // get_library_unit() unit
            h_zone->hm_write_pointer = tmp_zone_[i].zbz_write_pointer; // get_library_unit() unit
            h_zone->hm_type = tmp_zone_[i].zbz_type;
            zones.push_back(h_zone);
        }
        free(tmp_zone_);
        return;
    }
    uint32_t hmsmr_interface::get_open_zone_resource_wait_for_complete()
    {
        // hm-smr is no need to finish zone
        return zonenum_;
    }

    // libzbd
    ssize_t zbd_interface::hm_interface_write(void *buf, size_t count, uint64_t offset)
    {
        return pwrite(fd_, buf, count * ALIGN_BLOCK_SIZE, offset * ALIGN_BLOCK_SIZE);
    }

    ssize_t zbd_interface::hm_interface_read(void *buf, size_t count, uint64_t offset)
    {
        return pread(fd_, buf, count * ALIGN_BLOCK_SIZE, offset * ALIGN_BLOCK_SIZE);
    }

    uint64_t zbd_interface::get_zone_disk_num()
    {
        return zonenum_;
    }

    int64_t zbd_interface::get_first_seq_zone_num()
    {
        int i;
        for (i = 0; i < zonenum_; i++)
        {
            if (zone_[i].type == ZBD_ZONE_TYPE_SWR)
            {
                return i;
            }
        }
        return 0;
    }

    int zbd_interface::hm_reset_zone_wait_for_complete(uint64_t sector)
    {
        int ret = zbd_reset_zones(fd_, sector, info_.zone_size);
        if (ret != 0)
        {
            MyLog("reset error\n");
            return ret;
        }
        return 0;
    }
    int zbd_interface::hm_reset_all_zone_wait_for_complete()
    {
        int ret = zbd_reset_zones(fd_, 0, 0);
        if (ret < 0)
        {
            MyLog("reset error\n");
            return ret;
        }
        return 0;
    }
    int zbd_interface::hm_finish_zone_wait_for_complete(uint64_t sector)
    {
        // hm-smr is no need to finish zone
        zbd_finish_zones(fd_, sector, info_.zone_size);
        return 0;
    }
    void zbd_interface::get_one_zone_info_wait_for_complete(uint64_t zone, struct hm_zone *h_zone)
    {
        enum zbd_report_option ro = ZBD_RO_ALL;
        struct zbd_zone *zone_buf = (struct zbd_zone *)malloc(sizeof(struct zbd_zone));
        unsigned int num = 1;
        zbd_report_zones(fd_, zone_[zone].start, zone_[zone].len, ro, zone_buf, &num);
        h_zone->hm_start = zone_buf->start;      // get_library_unit() unit
        h_zone->hm_length = zone_buf->capacity;  // get_library_unit() unit
        h_zone->hm_write_pointer = zone_buf->wp; // get_library_unit() unit
        h_zone->hm_type = zone_buf->type;
        free(zone_buf);
        return;
    }
    void zbd_interface::get_all_zone_info_wait_for_complete(std::vector<struct hm_zone *> &zones)
    {
        zbd_zone *tmp_zone_;
        uint64_t addr_space_sz = (uint64_t)(zonenum_ * info_.zone_size);
        zbd_list_zones(fd_, 0, addr_space_sz, ZBD_RO_ALL, &tmp_zone_, &zonenum_); // get zone info
        for (int i = 0; i < zonenum_; i++)
        {
            struct hm_zone *h_zone = new struct hm_zone();
            h_zone->hm_start = tmp_zone_[i].start;      // get_library_unit() unit
            h_zone->hm_length = tmp_zone_[i].capacity;  // get_library_unit() unit
            h_zone->hm_write_pointer = tmp_zone_[i].wp; // get_library_unit() unit
            h_zone->hm_type = tmp_zone_[i].type;
            zones.push_back(h_zone);
        }
        free(tmp_zone_);
        return;
    }

    uint32_t zbd_interface::get_open_zone_resource_wait_for_complete()
    {
        // hm-smr is no need to finish zone
        return zonenum_;
    }

} // namespace leveldb
