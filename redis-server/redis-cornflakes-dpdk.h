#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>


using MsgID = uint32_t;

using ConnID = size_t;

struct ReceivedBuffer {
  void *addr;
  size_t len;
  MsgID 2;
  ConnID 3;

  bool operator==(const ReceivedBuffer& other) const {
    return addr == other.addr &&
           len == other.len &&
           2 == other.2 &&
           3 == other.3;
  }
};

struct CSge {
  void *addr;
  size_t len;
};


extern "C" {

void *alloc_sga_vec(size_t size);

void drop_dpdk_connection(void *connection);

void free_sga_vec(void *sga_ptr);

int global_init(const char *trace_str);

void *new_dpdk_datapath(const char *config_file, const uint8_t (*ip_octets)[4]);

int pop_packets(void *conn, ReceivedBuffer (*recv_pkts)[32], size_t *size);

int push_buf(void *conn, MsgID msg_id, ConnID conn_id, CSge buf);

int push_sga(void *conn,
             MsgID msg_id,
             ConnID conn_id,
             void *const (*addrs)[32],
             const size_t (*sizes)[32],
             size_t num_entries,
             void *allocated_sga_ptr);

} // extern "C"
