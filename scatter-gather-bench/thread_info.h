#include <stdlib.h>
#include <stdint.h>
#include "stats.h"

#pragma once
#ifdef __cplusplus 
extern "C" {
#endif

    void write_threads_info(char *filename, size_t num_threads, Summary_Statistics_t thread_info[8]);

#ifdef __cplusplus 
}
#endif

