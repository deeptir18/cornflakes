#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

void Mlx5_global_debug_init();

void OrderedSga_allocate(uintptr_t size, void **return_ptr);

void *Mlx5Connection_new(const char *config_file, const char *server_ip);

void Mlx5Connection_set_copying_threshold(void *conn, uintptr_t copying_threshold);

void Mlx5Connection_set_inline_mode(void *conn, uintptr_t inline_mode);

void Mlx5Connection_add_memory_pool(void *conn, uintptr_t buf_size, uintptr_t min_elts);

void **Mlx5Connection_pop_raw_packets(void *conn, uintptr_t *n);

const unsigned char *Mlx5Connection_RxPacket_data(const void *pkt);

uintptr_t Mlx5Connection_RxPacket_data_len(const void *pkt);

uint32_t Mlx5Connection_RxPacket_msg_id(const void *pkt);

uintptr_t Mlx5Connection_RxPacket_conn_id(const void *pkt);

void Mlx5Connection_RxPacket_free(const void *pkt);

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

} // extern "C"
