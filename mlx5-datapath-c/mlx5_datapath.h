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

void OrderedSga_allocate(uintptr_t size, void **return_ptr);

void *Mlx5Connection_new(const char *config_file, const char *server_ip);

void Mlx5Connection_set_copying_threshold(void *conn, uintptr_t copying_threshold);

void Mlx5Connection_set_inline_mode(void *conn, uintptr_t inline_mode);

void Mlx5Connection_add_memory_pool(void *conn, uintptr_t buf_size, uintptr_t min_elts);

void Mlx5Connection_add_tx_mempool(void *conn, uintptr_t size, uintptr_t min_elts);

struct ReceivedPkt *Mlx5Connection_pop(void *conn, uintptr_t *n);

void Mlx5Connection_push_ordered_sgas(void *conn,
                                      uintptr_t n,
                                      uint32_t *msg_ids,
                                      uintptr_t *conn_ids,
                                      void *ordered_sgas);

uint32_t Mlx5Connection_queue_arena_ordered_rcsga(void *conn,
                                                  uint32_t msg_id,
                                                  uintptr_t conn_id,
                                                  void *arena_ordered_rc_sga,
                                                  bool end_batch);

uint32_t Mlx5Connection_queue_single_buffer_with_copy(void *conn,
                                                      uint32_t msg_id,
                                                      uintptr_t conn_id,
                                                      const unsigned char *buffer,
                                                      uintptr_t buffer_len,
                                                      bool end_batch);
