#!/bin/bash
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./build/netperf \
     -c 0xff -n 8 -a 0000:41:00.0,txq_inline_mpw=256,txqs_min_inline=0 --proc-type=auto -- \
     --mode=CLIENT \
     --server_mac=$SERVER_MAC \
     --server_ip=$SERVER_IP \
     --client_ip=$CLIENT_IP \
     --time=5 \
     --rate=1000000 \
     --array_size=32768000 \
     --segment_size=512 \
     --num_segments=2 
