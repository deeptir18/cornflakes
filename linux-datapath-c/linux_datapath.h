#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct ReceivedPkt {
  const unsigned char *data;
  uintptr_t data_len;
  uint32_t msg_id;
  uintptr_t conn_id;
} ReceivedPkt;

void *OrderedSga_allocate(uintptr_t size);

void *LinuxConnection_new(const char *config_file, const char *server_ip);

void LinuxConnection_set_copying_threshold(void *conn, uintptr_t copying_threshold);

void LinuxConnection_set_inline_mode(void *conn, uintptr_t inline_mode);

void LinuxConnection_add_memory_pool(void *conn, uintptr_t buf_size, uintptr_t min_elts);

struct ReceivedPkt *LinuxConnection_pop(void *conn, uintptr_t *n);

void LinuxConnection_push_ordered_sgas(void *conn,
                                       uintptr_t n,
                                       uint32_t *msg_ids,
                                       uintptr_t *conn_ids,
                                       void *ordered_sgas);
