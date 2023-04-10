#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

void *Bump_with_capacity(uintptr_t batch_size, uintptr_t max_packet_size, uintptr_t max_entries);

void Bump_reset(void *self_);

void print_hello();

void ReplyInconsistentMessage_new_in(void *arena, void **return_ptr);

void ReplyInconsistentMessage_get_view(void *self_, uint64_t *return_ptr);

void ReplyInconsistentMessage_set_view(void *self_, uint64_t view);

void ReplyInconsistentMessage_get_replicaIdx(void *self_, uint32_t *return_ptr);

void ReplyInconsistentMessage_set_replicaIdx(void *self_, uint32_t replica_idx);

void ReplyInconsistentMessage_get_finalized(void *self_, uint32_t *return_ptr);

void ReplyInconsistentMessage_set_finalized(void *self_, uint32_t finalized);

void ReplyInconsistentMessage_get_mut_opid(void *self_, void **return_ptr);

uint32_t ReplyInconsistentMessage_deserialize(void *self_,
                                              const void *data,
                                              uintptr_t data_len,
                                              uintptr_t offset,
                                              void *arena);

uint32_t Mlx5Connection_ReplyInconsistentMessage_queue_cornflakes_arena_object(void *self_,
                                                                               uint32_t msg_id,
                                                                               uintptr_t conn_id,
                                                                               void *cornflakes_obj,
                                                                               bool end_batch);

void ReplyInconsistentMessage_free(const void *self_);

void OpID_get_clientid(void *self_, uint64_t *return_ptr);

void OpID_set_clientid(void *self_, uint64_t clientid);

void OpID_get_clientreqid(void *self_, uint64_t *return_ptr);

void OpID_set_clientreqid(void *self_, uint64_t clientreqid);

void ConfirmMessage_new_in(void *arena, void **return_ptr);

void ConfirmMessage_get_view(void *self_, uint64_t *return_ptr);

void ConfirmMessage_set_view(void *self_, uint64_t view);

void ConfirmMessage_get_replicaIdx(void *self_, uint32_t *return_ptr);

void ConfirmMessage_set_replicaIdx(void *self_, uint32_t replica_idx);

void ConfirmMessage_get_mut_opid(void *self_, void **return_ptr);

uint32_t ConfirmMessage_deserialize(void *self_,
                                    const void *data,
                                    uintptr_t data_len,
                                    uintptr_t offset,
                                    void *arena);

uint32_t Mlx5Connection_ConfirmMessage_queue_cornflakes_arena_object(void *self_,
                                                                     uint32_t msg_id,
                                                                     uintptr_t conn_id,
                                                                     void *cornflakes_obj,
                                                                     bool end_batch);

void ConfirmMessage_free(const void *self_);

void ReplyConsensusMessage_new_in(void *arena, void **return_ptr);

void ReplyConsensusMessage_get_view(void *self_, uint64_t *return_ptr);

void ReplyConsensusMessage_set_view(void *self_, uint64_t view);

void ReplyConsensusMessage_get_replicaIdx(void *self_, uint32_t *return_ptr);

void ReplyConsensusMessage_set_replicaIdx(void *self_, uint32_t replica_idx);

void ReplyConsensusMessage_get_result(void *self_, void **return_ptr);

void ReplyConsensusMessage_set_result(void *self_, const void *val);

void ReplyConsensusMessage_get_finalized(void *self_, uint32_t *return_ptr);

void ReplyConsensusMessage_set_finalized(void *self_, uint32_t finalized);

void ReplyConsensusMessage_get_mut_opid(void *self_, void **return_ptr);

uint32_t ReplyConsensusMessage_deserialize(void *self_,
                                           const void *data,
                                           uintptr_t data_len,
                                           uintptr_t offset,
                                           void *arena);

uint32_t Mlx5Connection_ReplyConsensusMessage_queue_cornflakes_arena_object(void *self_,
                                                                            uint32_t msg_id,
                                                                            uintptr_t conn_id,
                                                                            void *cornflakes_obj,
                                                                            bool end_batch);

void ReplyConsensusMessage_free(const void *self_);

} // extern "C"
