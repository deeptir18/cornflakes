#pragma once
#include <stdlib.h>
#include <stdint.h>

typedef struct Summary_Statistics_t
{
    uint16_t id;
    uint64_t sent;
    uint64_t recved;
    uint64_t retries;
    float runtime;
    uint64_t min;
    uint64_t max;
    uint64_t median;
    uint64_t p99;
    uint64_t p999;
    uint64_t avg;
    float offered_rate_gbps;
    float achieved_rate_gbps;
    float offered_rate_pps;
    float achieved_rate_pps;
    float percent_rate;
} Summary_Statistics_t;

