#include "../hm/my_log.h"

namespace leveldb
{

    void init_log_file()
    {
#if GEARDB_DBBUG
        FILE *fp;
        fp = fopen(log_name.c_str(), "w");
        if (fp == nullptr)
            printf("log failed\n");
        fclose(fp);

        fp = fopen(log_name2.c_str(), "w");
        if (fp == nullptr)
            printf("log failed\n");
        fclose(fp);

#endif
    }

    void MyLog(const char *format, ...)
    {
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[102400];
        vsprintf(buf, format, ap);
        va_end(ap);

        FILE *fp = fopen(log_name.c_str(), "a");
        if (fp == nullptr)
            printf("log failed\n");
        fprintf(fp, "%s", buf);
        fclose(fp);
#endif
    }

    void MyLog2(const char *format, ...)
    {
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[1024000];

        vsprintf(buf, format, ap);

        va_end(ap);

        FILE *fp = fopen(log_name2.c_str(), "a");
        if (fp == nullptr)
            printf("log failed\n");
        fprintf(fp, "%s", buf);
        fclose(fp);
#endif
    }

} // namespace leveldb
