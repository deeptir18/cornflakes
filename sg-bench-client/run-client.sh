#!/bin/bash
echo $CLIENT_IP
echo $SERVER_IP
echo $BASE
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH $CORNFLAKES/target/debug/dpdk_sg_client \
     --client_id 1 \
     --config_file $BASE/config/d6515x2.yaml \
     --distribution exponential \
     --num_clients 1 \
     --num_threads 2 \
     --our_ip $CLIENT_STATIC_IP \
     --random_seed 0 \
     --server_ip $SERVER_STATIC_IP \
     --time 5 \
     --debug_level debug \
     --array_size 32768000 \
     --send_packet_size 20 \
     --segment_size 512 \
     --num_segments 2
