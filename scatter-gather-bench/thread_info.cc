#include "thread_info.h"
#include "debug.h"
#include "stats.h"
#include "stdio.h"
#include "string.h"

#ifdef __cplusplus 
extern "C" {
#endif

    void write_threads_info(char *filename, size_t num_threads, Summary_Statistics_t thread_info[8]) {
        static __thread int thread_id;
        NETPERF_INFO("Dumping thread info into %s for %lu threads", filename, num_threads);
        FILE *fp = fopen(filename, "w");
        NETPERF_INFO("Opened threads file");
        fprintf(fp, "{\n");
        // write threads json into file
        // TODO: currently manually writes json in. would be better and more
        // portable to write it via some library.
        for (size_t i = 0; i < num_threads; i++) {
            Summary_Statistics_t *info = &thread_info[i];
            NETPERF_INFO("Writing in info for thread %lu", i);
            fprintf(fp, "\"%lu\":\t{", i);
            fprintf(fp, "\n\t\t\"id\": %u,", info->id);
            fprintf(fp, "\n\t\t\"sent\": %lu,", info->sent);
            fprintf(fp, "\n\t\t\"recved\": %lu,", info->recved);
            fprintf(fp, "\n\t\t\"runtime\": %f,", info->runtime);
            fprintf(fp, "\n\t\t\"offered_load_gbps\": %f,", info->offered_rate_gbps);
            fprintf(fp, "\n\t\t\"offered_load_pps\": %f,", info->offered_rate_pps);
            fprintf(fp, "\n\t\t\"achieved_load_gbps\": %f,", info->achieved_rate_gbps);
            fprintf(fp, "\n\t\t\"achieved_load_pps\": %f,", info->achieved_rate_pps);
            fprintf(fp, "\n\t\t\"latencies\": {");
            fprintf(fp, "\n\t\t\t\"max\": %lu,", info->max);
            fprintf(fp, "\n\t\t\t\"min\": %lu,", info->min);
            fprintf(fp, "\n\t\t\t\"median\": %lu,", info->median);
            fprintf(fp, "\n\t\t\t\"p99\": %lu,", info->p99);
            fprintf(fp, "\n\t\t\t\"p999\": %lu,", info->p999);
            fprintf(fp, "\n\t\t\t\"avg\": %lu", info->avg);
            fprintf(fp, "\n\t\t}");
            fprintf(fp, "\n\t}");
            if (i != (num_threads - 1)) {
                fprintf(fp, ",\n");
            }
        }
        fprintf(fp, "\n}\n");
        fclose(fp);
}


#ifdef __cplusplus 
}
#endif
