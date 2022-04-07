/*
 * time.c - timekeeping utilities
 */

#include <time.h>

#include <base/time.h>
#include <base/debug.h>

int custom_mlx5_cycles_per_us __attribute__(( aligned(CACHE_LINE_SIZE) ));
float custom_mlx5_cycles_per_ns;
uint64_t custom_mlx5_start_tsc;

/**
 * __timer_delay_us - spins the CPU for the specified delay
 * @us: the delay in microseconds
 */
void __custom_mlx5_time_delay_us(uint64_t us)
{
	uint64_t cycles = us * custom_mlx5_cycles_per_us;
	unsigned long start = rdtsc();

	while (rdtsc() - start < cycles)
		cpu_relax();
}

static void custom_mlx5_rdtsc_benchmark(void) {

	struct timespec t_start, t_end;
    uint64_t ns;
    clock_gettime(CLOCK_MONOTONIC_RAW, &t_start);
    int calls = 1000000;
    for (int i = 0; i < calls; i++) {
        rdtsc();
    }
	clock_gettime(CLOCK_MONOTONIC_RAW, &t_end);
	ns = ((t_end.tv_sec - t_start.tv_sec) * 1E9);
	ns += (t_end.tv_nsec - t_start.tv_nsec);
    float time_per_call = (float)ns / (float)calls;
    NETPERF_INFO("%d calls to rdstc took %lu; %f per call\n", calls, ns, time_per_call);
}

/* derived from DPDK */
static int custom_mlx5_time_calibrate_tsc(void)
{
    custom_mlx5_rdtsc_benchmark();
	/* TODO: New Intel CPUs report this value in CPUID */
	struct timespec sleeptime = {.tv_nsec = 5E8 }; /* 1/2 second */
	struct timespec t_start, t_end;

	cpu_serialize();
	if (clock_gettime(CLOCK_MONOTONIC_RAW, &t_start) == 0) {
		uint64_t ns, end, start;
		double secs;

		start = rdtsc();
		nanosleep(&sleeptime, NULL);
		clock_gettime(CLOCK_MONOTONIC_RAW, &t_end);
		end = rdtscp(NULL);
		ns = ((t_end.tv_sec - t_start.tv_sec) * 1E9);
		ns += (t_end.tv_nsec - t_start.tv_nsec);

		secs = (double)ns / 1000;
		custom_mlx5_cycles_per_us = (uint64_t)((end - start) / secs);
        custom_mlx5_cycles_per_ns = (float)((float)(end - start) / (float)ns);
        NETPERF_INFO("cycles: %lu, time: %lu ns", (end - start), ns);
		NETPERF_INFO("time: detected %d ticks / us", custom_mlx5_cycles_per_us);
        NETPERF_INFO("time: detected %f ticks / ns", custom_mlx5_cycles_per_ns);

		/* record the start time of the binary */
		custom_mlx5_start_tsc = rdtsc();
		return 0;
	}

	return -1;
}

/**
 * time_init - global time initialization
 *
 * Returns 0 if successful, otherwise fail.
 */
int custom_mlx5_time_init(void)
{
	return custom_mlx5_time_calibrate_tsc();
}
