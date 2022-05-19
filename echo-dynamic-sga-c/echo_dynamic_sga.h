#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct SingleBufferCF {
  uintptr_t bitmap_len;
  char *bitmap;
  char *message;
} SingleBufferCF;

struct SingleBufferCF *SingleBufferCF_new(void);

char *SingleBufferCF_get_message(struct SingleBufferCF *single_buffer_cf);

void SingleBufferCF_set_message(struct SingleBufferCF *single_buffer_cf, char *message);

void SingleBufferCF_deserialize(struct SingleBufferCF *single_buffer_cf, char *buffer);

void SingleBufferCF_serialize_into_sga(struct SingleBufferCF *single_buffer_cf,
                                       void *ordered_sga,
                                       void *conn);
