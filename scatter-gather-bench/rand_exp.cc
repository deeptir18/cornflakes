#include "rand_exp.h"
#include <random>

extern "C"
void sample_exp_distribution(double lambda, size_t num_rolls, uint64_t *timestamps_ptr) {
    thread_local std::mt19937 generator(std::random_device{}());
    std::poisson_distribution<int> distribution(lambda);

    for (size_t i = 0; i < num_rolls; i++) {
        timestamps_ptr[i] = (uint64_t)distribution(generator);
    }
}
