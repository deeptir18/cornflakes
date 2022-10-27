#!/bin/bash
sudo LD_LIBRARY_PATH=/proj/demeter-PG0/prthaker/cornflakes/cornflakes-libos/3rdparty/dpdk/install/lib/x86_64-linux-gnu\
  /proj/demeter-PG0/prthaker/cornflakes/scatter-gather-bench/build/netperf -c 0xff\
  -n 4 -w 0000:03:00.1 --proc-type=auto -- --mode=CLIENT --server_ip=$SERVER_IP\
  --client_ip=$CLIENT_IP --server_mac=$SERVER_MAC --rate=50000 --segment_size=512\
  --num_segments=2 --time=10 --log=/proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_512/mbufs_2/arraysize_3276800000/busycycles_0/1@50000/threads_4/zero_copy/trial_4/client1.latency-\
t.log\
  --array_size=3276800000 --client_threads=4 --threads_log=/proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_512/mbufs_2/arraysize_3276800000/busycycles_0/1@50000/threads_4/zero_copy/\
trial_4/client1.threads.log\
  --random_seed=1639037898 --num_machines=1 --machine_id=0 > /proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_512/mbufs_2/arraysize_3276800000/busycycles_0/1@50000/threads_4/zero_cop\
y/trial_4/client1.log\
  2> /proj/demeter-PG0/prthaker/cornflakes/experiments/scaling-results/segmentsize_512/mbufs_2/arraysize_3276800000/busycycles_0/1@50000/threads_4/zero_copy/trial_4/client1.err.log
