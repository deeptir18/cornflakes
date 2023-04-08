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

void ReplyInconsistentMessage_set_replicaIdx(void *self_, uint32_t replicaIdx);

void ReplyInconsistentMessage_get_finalized(void *self_, uint32_t *return_ptr);

void ReplyInconsistentMessage_set_finalized(void *self_, uint32_t finalized);

void ReplyInconsistentMessage_get_mut_opid(void *self_, void **return_ptr);

uint32_t ReplyInconsistentMessage_deserialize(void *self_,
                                              void *pkt,
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

} // extern "C"
