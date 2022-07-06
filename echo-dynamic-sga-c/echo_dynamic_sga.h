#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

void SingleBufferCF_new(void **return_ptr);

void SingleBufferCF_get_message(void *self_,
                                const unsigned char **return_ptr,
                                uintptr_t *return_len_ptr);

void SingleBufferCF_set_message(void *self_, const unsigned char *message, uintptr_t message_len);

void SingleBufferCF_num_scatter_gather_entries(void *self_, uintptr_t *return_ptr);

uint32_t SingleBufferCF_deserialize(void *self_, const unsigned char *buffer, uintptr_t buffer_len);

uint32_t SingleBufferCF_serialize_into_sga(void *self_, void *ordered_sga, void *conn);
