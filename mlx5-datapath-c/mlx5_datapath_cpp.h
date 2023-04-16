#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

void Mlx5_global_debug_init();

void OrderedSga_allocate(uintptr_t size, void **return_ptr);

void *Mlx5Connection_new(const char *config_file, const char *server_ip);

void Mlx5Connection_new_cfbytes_with_copy(void *ptr, uintptr_t len, void *box_arena, void **ret);

void Mlx5Connection_new_cfstring_with_copy(void *ptr, uintptr_t len, void *box_arena, void **ret);

void Mlx5Connection_set_copying_threshold(void *conn, uintptr_t copying_threshold);

void Mlx5Connection_set_inline_mode(void *conn, uintptr_t inline_mode);

void Mlx5Connection_free_datapath_buffer(void *datapath_buffer);

void Mlx5Connection_retrieve_raw_ptr(void *datapath_buffer, void **return_ptr);

void *Mlx5Connection_allocate_and_copy_into_original_datapath_buffer(void *conn,
                                                                     void *mempool_ids_vec,
                                                                     const unsigned char *data_buffer,
                                                                     uintptr_t data_buffer_len,
                                                                     void **return_raw_ptr);

void *Mlx5Connection_allocate_datapath_buffer(void *conn,
                                              uintptr_t size,
                                              void *mempool_ids_vec,
                                              void **return_ptr);

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

/// returns box of datapath buffer
void Mlx5Connection_prepare_single_buffer_with_udp_header(void *conn,
                                                          uint32_t msg_id,
                                                          uintptr_t conn_id,
                                                          uintptr_t data_len,
                                                          void **raw_data_ptr,
                                                          void **smart_data_ptr);

/// Sends boxed buffer, setting it's length
void Mlx5Connection_transmit_single_datapath_buffer_with_header(void *conn,
                                                                void *box_buffer,
                                                                uintptr_t data_len,
                                                                uintptr_t end_batch);

uint32_t Mlx5Connection_queue_single_buffer_with_copy(void *conn,
                                                      uint32_t msg_id,
                                                      uintptr_t conn_id,
                                                      const unsigned char *buffer,
                                                      uintptr_t buffer_len,
                                                      bool end_batch);

void ReceivedPkt_msg_type(const void *pkt, uint16_t *size_ptr, uint16_t *return_ptr);

void ReceivedPkt_size(const void *pkt, uint16_t *return_ptr);

void ReceivedPkt_data(const void *pkt, const unsigned char **return_ptr);

void ReceivedPkt_data_len(const void *pkt, uintptr_t *return_ptr);

void ReceivedPkt_msg_id(void *self_, uint32_t *return_ptr);

void ReceivedPkt_conn_id(void *self_, uintptr_t *return_ptr);

void ReceivedPkt_free(const void *self_);

uintptr_t Mlx5Connection_load_retwis_db(void *conn,
                                        uintptr_t key_size,
                                        uintptr_t value_size,
                                        uintptr_t num_keys,
                                        void **db_ptr,
                                        void **mempools_ptr);

uintptr_t Mlx5Connection_load_twitter_db(void *conn,
                                         const char *trace_file,
                                         uintptr_t end_time,
                                         uintptr_t min_keys_to_load,
                                         void **db_ptr,
                                         void **list_db_ptr,
                                         void **mempools_ptr);

void Mlx5Connection_drop_db(void *db_ptr);

void Mlx5Connection_drop_dbs(void *db_ptr, void *list_db_ptr);

uint32_t Mlx5Connection_load_ycsb_db(void *conn,
                                     const char *trace_file,
                                     void **db_ptr,
                                     void **list_db_ptr,
                                     void **mempool_ids_ptr,
                                     uintptr_t num_keys,
                                     uintptr_t num_values,
                                     const char *value_size,
                                     bool use_linked_list);

void Mlx5Connection_get_db_keys_vec(void *db, void **db_keys_vec, uintptr_t *db_keys_len);

void Mlx5Connection_get_list_db_keys_vec(void **list_db,
                                         void **list_db_keys_vec,
                                         uintptr_t *list_db_keys_len);

void Mlx5Connection_get_db_value_at(void *db,
                                    void *db_keys_vec,
                                    uintptr_t key_idx,
                                    void **key_ptr,
                                    uintptr_t *key_len,
                                    void **value_ptr,
                                    uintptr_t *value_len,
                                    void **value_box_ptr);

void Mlx5Connection_list_db_get_list_size(void *list_db,
                                          void *list_db_keys_vec,
                                          uintptr_t key_idx,
                                          void **key_ptr,
                                          uintptr_t *key_len,
                                          uintptr_t *list_size);

void Mlx5Connection_list_db_get_value_at_idx(void *list_db,
                                             void *list_db_keys_vec,
                                             uintptr_t key_idx,
                                             uintptr_t list_idx,
                                             void **value_ptr,
                                             uintptr_t *value_len,
                                             void **value_box_ptr);

} // extern "C"
