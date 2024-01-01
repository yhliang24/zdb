#ifndef LEVELDB_HM_LOG_H
#define LEVELDB_HM_LOG_H

//////
// Module function: output debug and statistics
//////

#include <string>
#include <cstdio>
#include <cstdarg>

#define GEARDB_DBBUG 1 // 1 output some information to the file; 0 cancel

namespace leveldb
{

    const std::string log_name("./MyLOG");
    const std::string log_name2("./COP_LOG");

    void init_log_file();
    void MyLog(const char *format, ...);
    void MyLog2(const char *format, ...);

} // namespace leveldb

#endif // HM_LEVELDB_HM_DEBUG_H
