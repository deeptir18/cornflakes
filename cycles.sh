#!/bin/bash

usage="USAGE: ./cycles.sh [cf|cf1c|fb|proto] [echo1|echo2|cfkv] [server|client]"
if [ $# -ne 3 ]; then
	echo $usage
	exit 1
fi

if [ $1 = "cf" ]; then
	inline_mode="objectheader"
	push_buf_type="object"
	serialization="cornflakes-dynamic"
elif [ $1 = "cf1c" ]; then
	inline_mode="nothing"
	push_buf_type="object"
	serialization="cornflakes1c-dynamic"
elif [ $1 = "fb" ]; then
	inline_mode="nothing"
	push_buf_type="singlebuf"
	serialization="flatbuffers"
elif [ $1 = "proto" ]; then
	inline_mode="nothing"
	push_buf_type="singlebuf"
	serialization="protobuf"
else
	echo $usage
	exit 1
fi

if [ $2 = "cfkv" ]; then
	rate=500000
	if [ $3 = "server" ]; then
		cmd="sudo env LD_LIBRARY_PATH=/mydata/ygina/cornflakes/dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu /mydata/ygina/cornflakes/target/release/ycsb_mlx5 --config_file $CORNFLAKES_CONFIG --mode server --server_ip 192.168.1.1 --debug_level info --trace $YCSB_LOAD_TRACE --num_values 2 --num_keys 1 --value_size UniformOverSizes-2048 --push_buf_type $push_buf_type --inline_mode $inline_mode --serialization $serialization"
	elif [ $3 = "client" ]; then
		cmd="sudo env LD_LIBRARY_PATH=/mydata/ygina/cornflakes/dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu /mydata/ygina/cornflakes/target/release/ycsb_dpdk --config_file $CORNFLAKES_CONFIG --server_ip 192.168.1.1 --our_ip 192.168.1.2 --debug_level info --mode client --queries $YCSB_ACCESS_TRACE --trace $YCSB_LOAD_TRACE --client_id 0 --num_clients 1 --num_threads 16 --time 25 --rate $rate --value_size UniformOverSizes-2048 --num_values 2 --num_keys 1 --push_buf_type $push_buf_type --inline_mode $inline_mode --serialization $serialization"
	else
		echo $usage
		exit 1
	fi
else
	rate=50000
	if [ $2 = "echo1" ]; then
		message="single"
	elif [ $2 = "echo2" ]; then
		message="list-2"
	else
		echo $usage
		exit 1
	fi

	if [ $3 = "server" ]; then
		cmd="sudo env LD_LIBRARY_PATH=/mydata/ygina/cornflakes/dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu nice -n -19 taskset -c 2 /mydata/ygina/cornflakes/target/release/ds_echo_mlx5 --config_file $CORNFLAKES_CONFIG --mode server --server_ip 192.168.1.1 --debug_level info --message $message --size 4096 --push_buf_type $push_buf_type --inline_mode $inline_mode --serialization $serialization"
	elif [ $3 = "client" ]; then
		cmd="sudo env RUST_BACKTRACE=1 LD_LIBRARY_PATH=/mydata/ygina/cornflakes/dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu /mydata/ygina/cornflakes/target/release/ds_echo_dpdk --mode client --config_file $CORNFLAKES_CONFIG --server_ip 192.168.1.1 --our_ip 192.168.1.2 --debug_level info --time 15 --rate $rate --num_threads 16 --message $message --size 4096 --push_buf_type $push_buf_type --inline_mode $inline_mode --serialization $serialization"
	else
		echo $usage
		exit 1
	fi
fi

echo $cmd
$cmd
# $cmd |& tee -a results.txt
