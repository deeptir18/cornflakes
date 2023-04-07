#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

void *Bump_with_capacity(uintptr_t batch_size, uintptr_t max_packet_size, uintptr_t max_entries);

void Bump_reset(void *self_);

void print_hello();

} // extern "C"
