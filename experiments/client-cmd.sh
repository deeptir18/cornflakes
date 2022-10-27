#!/bin/bash
sudo LD_LIBRARY_PATH=/proj/demeter-PG0/prthaker/cornflakes/cornflakes-libos/3rdparty/dpdk/install/lib/x86_64-linux-gnu /proj/demeter-PG0/prthaker/cornflakes/scatter-gather-bench/build/netperf \
     -c 0xff -n 4 -w 0000:41:00.0 --proc-type=auto -- \
     --mode=CLIENT \
     --server_mac=$SERVER_MAC \
     --server_ip=$SERVER_IP \
     --client_ip=$CLIENT_IP \
     --rate=2000000 \
     --segment_size=512 \
     --num_segments=2 \
     --time=10  \
     --random_seed=1655845760 \
     --num_machines=1 \
     --machine_id=0 \
     --array_size=32768000 \
     --client_threads=8 \
     --log=/proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_1024/mbufs_2/arraysize_32768000/recv_size_small/busycycles_0/1@50000/threads_2/zero_copy/server_cores_2/trial_7/client1.latency-t.log \
     --threads_log=/proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_1024/mbufs_2/arraysize_32768000/recv_size_small/busycycles_0/1@50000/threads_2/zero_copy/server_cores_2/trial_7/client1.threads.log \
     > /proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_1024/mbufs_2/arraysize_32768000/recv_size_small/busycycles_0/1@50000/threads_2/zero_copy/server_cores_2/trial_7/client1.log \
     2> /proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_1024/mbufs_2/arraysize_32768000/recv_size_small/busycycles_0/1@50000/threads_2/zero_copy/server_cores_2/trial_7/client1.err.log
