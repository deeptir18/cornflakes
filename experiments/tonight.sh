#!/bin/bash

# recv_size
python3 mlx5-bench.py -e loop -f /h/deeptir/cornflakes/experiments/results/mlnx_sg_bench/dec6_recvsize -c
~/config/config.yaml -ec ~/cornflakes/experiments/yamls/mlx5-netperf.yaml --looping_variable recv_size --graph_only

# total size
python3 mlx5-bench.py -e loop -f /h/deeptir/cornflakes/experiments/results/mlnx_sg_bench/dec7_totalsize -c
~/config/config.yaml -ec ~/cornflakes/experiments/yamls/mlx5-netperf.yaml
--looping_variable total_size --graph_only

# num_segments
python3 mlx5-bench.py -e loop -f /h/deeptir/cornflakes/experiments/results/mlnx_sg_bench/dec7_numsegments -c
~/config/config.yaml -ec ~/cornflakes/experiments/yamls/mlx5-netperf.yaml --looping_variable num_segments --graph_only

# array size - graph only
python3 mlx5-bench.py -e loop -f /h/deeptir/cornflakes/experiments/results/mlnx_sg_bench/nov12_array_size_sweep -c ~/config/config.yaml -ec ~/cornflakes/experiments/yamls/mlx5-netperf.yaml --looping_variable array_total_size --graph_only
