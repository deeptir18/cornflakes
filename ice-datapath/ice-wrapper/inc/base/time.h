/*
 * time.h - timekeeping utilities
 */

#pragma once

#include <base/types.h>
#include <asm/ops.h>

#define ONE_SECOND	1000000
#define ONE_MS		1000
#define ONE_US		1

extern uint64_t custom_mlx5_cycles_per_sec;
extern int custom_mlx5_cycles_per_us;
extern float custom_mlx5_cycles_per_ns;
extern uint64_t custom_mlx5_start_tsc; 

static inline uint64_t custom_mlx5_cycles_to_us(uint64_t a)
{
    return a / custom_mlx5_cycles_per_us;
}

static inline uint64_t custom_mlx5_us_to_cycles(uint64_t a)
{
    return a * custom_mlx5_cycles_per_us;
}

static inline uint64_t custom_mlx5_ns_to_cycles(uint64_t a) {
    return (uint64_t)((float)a * custom_mlx5_cycles_per_ns);
}

static inline uint64_t custom_mlx5_cycles_offset(uint64_t base)
{
    return (rdtsc() - custom_mlx5_start_tsc) - base;
}

static inline uint64_t custom_mlx5_cycles_intersend(uint64_t rate_pps)
{
    return (ONE_SECOND / rate_pps) * (custom_mlx5_cycles_per_us);
}

static inline uint64_t custom_mlx5_time_intersend(uint64_t rate_pps)
{
    return ONE_SECOND * 1000 / rate_pps;
}

/** 
 * Converts seconds to cycles
 */
static inline uint64_t custom_mlx5_seconds_to_cycles(uint64_t sec)
{
    return sec * custom_mlx5_cycles_per_us * ONE_SECOND;
}

/**
 * Return the number of cycles since program init.
 * */
static inline uint64_t custom_mlx5_microcycles(void)
{
    return rdtsc() - custom_mlx5_start_tsc;
}

static inline uint64_t custom_mlx5_cycletime(void)
{
    return rdtsc() - custom_mlx5_start_tsc;
}

static inline uint64_t custom_mlx5_cycles_to_ns(uint64_t cycles)
{
    float nanos = (float)cycles / (float)custom_mlx5_cycles_per_ns;
    return (uint64_t)nanos;
}

/**
 * microtime - gets the number of microseconds since the process started
 * This routine is very inexpensive, even compared to clock_gettime().
 */
static inline uint64_t custom_mlx5_nanotime(void)
{
	return (uint64_t)((float)(rdtsc() - custom_mlx5_start_tsc) / (float)custom_mlx5_cycles_per_ns);
}

/**
 * microtime - gets the number of microseconds since the process started
 * This routine is very inexpensive, even compared to clock_gettime().
 */
static inline uint64_t custom_mlx5_microtime(void)
{
	return (rdtsc() - custom_mlx5_start_tsc) / custom_mlx5_cycles_per_us;
}

extern void __custom_mlx5_time_delay_us(uint64_t us);

/**
 * delay_us - pauses the CPU for microseconds
 * @us: the number of microseconds
 */
static inline void custom_mlx5_delay_us(uint64_t us)
{
	__custom_mlx5_time_delay_us(us);
}

/**
 * delay_ms - pauses the CPU for milliseconds
 * @ms: the number of milliseconds
 */
static inline void custom_mlx5_delay_ms(uint64_t ms)
{
	/* TODO: yield instead of spin */
	__custom_mlx5_time_delay_us(ms * ONE_MS);
}

/**
 * time_init - global time initialization
 *
 * Returns 0 if successful, otherwise fail.
 */
extern int custom_mlx5_time_init(void);
