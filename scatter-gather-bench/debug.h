#pragma once
#include <stdlib.h>
#include <stdio.h>

#define RED   "\x1B[31m"
#define GRN   "\x1B[32m"
#define YEL   "\x1B[33m"
#define BLU   "\x1B[34m"
#define MAG   "\x1B[35m"
#define CYN   "\x1B[36m"
#define WHT   "\x1B[37m"
#define RESET "\x1B[0m"

#ifndef NETPERF_DEBUG_
#define NETPERF_DEBUG_
/***************************************************************/
// DEBUG MACROS
#ifdef __DEBUG__
#define DEBUG_ERR(reason) (cerr << "Function " << __FUNCTION__ << ": Error ( " << reason << ")\n")
#else
#define DEBUG_ERR(reason) do{}while(0)
#endif
#define EXIT (exit(1))
#ifdef __DEBUG__
#define NETPERF_DEBUG(msg, ...) \
        printf(CYN "[%s, %s, line %d, thread %d] DEBUG: " RESET, __FILE__, __FUNCTION__, __LINE__, thread_id); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n");
#else
#define NETPERF_DEBUG(msg, ...) do{}while(0)
#endif
#ifdef __DEBUG__
#define NETPERF_ASSERT(cond, msg, ...) \
    if (!(cond)) {  \
        printf(RED "[%s, %s, line %d] *NETPERF Assertion failed**: \n" RESET, __FILE__, __FUNCTION__, __LINE__); \
        printf("\t\u2192 "); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n"); \
        exit(1); \
    }
#else
#define NETPERF_ASSERT(cond, msg, ...) do{}while(0)
#endif
#define PLAIN_ASSERT(cond, msg, ...) \
    if (!(cond)) { \
        printf("\u2192**Assertion failed**: file (%s), function (%s), line (%d)\n\n", __FILE__, __FUNCTION__, __LINE__); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n"); \
        exit(1); \
    }
#define NETPERF_INFO(msg, ...) \
        printf(GRN "[%s, %s, line %d, thread_id %d] INFO: " RESET, __FILE__, __FUNCTION__, __LINE__, thread_id); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n");
#define NETPERF_WARN(msg, ...) \
        printf(YEL "[%s, %s, line %d, thread_id %d] WARN: " RESET, __FILE__, __FUNCTION__, __LINE__, thread_id); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n");
#define NETPERF_ERROR(msg, ...) \
        printf(MAG "[%s, %s, line %d, thread_id %d] ERROR: " RESET, __FILE__, __FUNCTION__, __LINE__, thread_id); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n");
#define NETPERF_PANIC(msg, ...) \
    printf(RED "[%s, %s, line %d, thread_id %d] PANIC: " RESET, __FILE__, __FUNCTION__, __LINE__, thread_id); \
    printf(msg, ##__VA_ARGS__); \
    printf("\n"); \
    exit(1);
#define NETPERF_IS_ONE(return_var, msg, ...) \
    if (return_var != 1) { \
        printf(RED "[%s, %s, line %d] NOT EQUAL TO 1: " RESET, __FILE__, __FUNCTION__, __LINE__); \
        printf(msg, ##__VA_ARGS__); \
        printf("\n"); \
        exit(1); \
    }

#define RETURN_ON_ERR(return_var, msg, ...) \
    if (ret) { \
        NETPERF_ERROR(msg, ##__VA_ARGS__); \
        return ret; \
    }

/**
 * PANIC_ON_TRUE - a fatal check that doesn't compile out in release builds
 * @condition: the condition to check (fails on true)
 */
#define PANIC_ON_TRUE(cond, msg, ...)						\
	do {							\
		__build_assert_if_constant(!(cond));		\
		if (unlikely(cond)) {				\
			NETPERF_WARN(msg, ##__VA_ARGS__);		\
			__builtin_unreachable();		\
		}						\
	} while (0)

/***************************************************************/
#endif /* NETPERF_DEBUG_ */
