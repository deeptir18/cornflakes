#include <stdlib.h>
#include <stdint.h>

#pragma once
#ifdef __cplusplus 
extern "C" {
#endif
    void sample_exp_distribution(double lambda, size_t num_rolls, uint64_t *timestamps_ptr);
#ifdef __cplusplus 
}
#endif
