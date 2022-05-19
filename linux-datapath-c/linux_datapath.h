#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct ReceivedPkt {
  char *data;
  uintptr_t data_len;
  int32_t msg_id;
  uintptr_t conn_id;
} ReceivedPkt;

void *OrderedSga_allocate(uintptr_t size);

void *LinuxConnection_new(const char *config_file, const char *server_ip);

void LinuxConnection_set_copying_threshold(void *conn, uintptr_t copying_threshold);

void LinuxConnection_set_inline_mode(void *conn, uintptr_t inline_mode);

void LinuxConnection_add_memory_pool(void *conn, uintptr_t buf_size, uintptr_t min_elts);

struct ReceivedPkt **LinuxConnection_pop(void *conn, int *n);

void LinuxConnection_push_ordered_sgas(void *conn,
                                       int n,
                                       int32_t *msg_ids,
                                       uintptr_t *conn_ids,
                                       void *ordered_sgas);
