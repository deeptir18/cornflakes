#include <stdio.h>
#include "linux_datapath.h"
#include "echo_dynamic_sga.h"

#define BUFFER_SIZE 128

struct Opt {
    char *config_file;
    char *server_ip;
    int copying_threshold;
    int inline_mode;
    int message_type;
    int push_buf_type;
};

void main() {
    struct Opt opt;
    opt.config_file = "example_config.yaml";
    opt.server_ip = "127.0.0.1";
    opt.copying_threshold = 256;
    opt.inline_mode = 0;    // nothing
    opt.message_type = 0;   // single
    opt.push_buf_type = 0;  // sga

    // ds-echo/src/run_datapath.rs:run_server()
    void *conn = LinuxConnection_new(opt.config_file, opt.server_ip);

    LinuxConnection_set_copying_threshold(conn, opt.copying_threshold);
    LinuxConnection_set_inline_mode(conn, opt.inline_mode);

    // cornflakes-libos/src/state-machine/server.rs:init()
    int buf_size = 256;
    int max_size = 8192;
    int min_elts = 8192;
    while(1) {
        printf("Adding memory pool of size %d\n", buf_size);
        LinuxConnection_add_memory_pool(conn, buf_size, min_elts);
        buf_size *= 2;
        if (buf_size > max_size) {
            break;
        }
    }

    // cornflakes-libos/src/state-machine/server.rs:run_state_machine()
    int32_t msg_ids[BUFFER_SIZE];
    size_t conn_ids[BUFFER_SIZE];
    void *ordered_sgas[BUFFER_SIZE];
    while(1) {
        int i, n;
        struct ReceivedPkt **pkts = LinuxConnection_pop(conn, &n);
        if (n > BUFFER_SIZE) {
            printf("ERROR: Buffer size is too small");
            exit(-1);
        }
        // assume sga push_buf_type
        // assume single message_type
        // ds-echo/src/cornflakes_dynamic/mod.rs:process_requests_sga()
        for (i = 0; i < n; i++) {
            struct ReceivedPkt *pkt = pkts[i];
            printf("Incoming packet length: %ld\n", pkt->data_len);
            struct SingleBufferCF *single_deser = SingleBufferCF_new();
            struct SingleBufferCF *single_ser = SingleBufferCF_new();
            // cornflakes-codegen/src/utils/dynamic_sga_hdr.rs:deserialize()
            // ignore indexing pkt.seg(0)
            SingleBufferCF_deserialize(single_deser, pkt->data);
            // generated echo_dynamic_sga.rs
            SingleBufferCF_set_message(
                single_ser,
                // should CFBytes be a zero-overhead wrapper around the ptr?
                SingleBufferCF_get_message(single_deser)
            );
            // cornflake-libos/src/lib.rs:allocate()
            void *ordered_sga = OrderedSga_allocate(SingleBufferCF_num_scatter_gather_entries(single_ser));
            // cornflakes-codegen/src/utils/dynamic_sga_hdr.rs:serialize_into_sga()
            SingleBufferCF_serialize_into_sga(single_ser, ordered_sga, conn);
            msg_ids[i] = pkt->msg_id;
            conn_ids[i] = pkt->conn_id;
            ordered_sgas[i] = ordered_sga;
        }
        LinuxConnection_push_ordered_sgas(conn, n, msg_ids, conn_ids, ordered_sgas);
    }
}
