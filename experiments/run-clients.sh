#!/bin/bash
CORNFLAKES=/proj/demeter-PG0/prthaker/cornflakes
LD_LIBRARY_PATH=$LD_LIBRARY_PATH python3 mlx5-bench.py \
	-e individual \
	-f $CORNFLAKES/experiments/scaling-results/$1 \
	-c $CORNFLAKES/experiments/configs/multicore-config.yaml \
	-ec $CORNFLAKES/experiments/yamls/mlx5-netperf-multicore.yaml \
	--array_size=32768000 \
	-bc 0 \
	-nc 1 \
	-t 2 \
	--segment_size 1024 \
	--num_segments 2 \
	--rate 50000 \
	--num_cores 2 \
	--pprint
