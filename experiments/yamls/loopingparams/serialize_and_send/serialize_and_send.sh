#!/bin/bash
DIR=$1
FINAL_DIR=$2
# google exp
python3 $DIR/cornflakes/experiments/google-bench.py -e loop -f $FINAL_DIR/googleproto_sga -c $DIR/config/cluster_config.yaml -ec $DIR/cornflakes/cf-kv/google.yaml -lc $DIR/cornflakes/experiments/yamls/loopingparams/serialize_and_send/google.yaml
# twitter exp
python3 $DIR/cornflakes/experiments/twitter-bench.py -e loop -f $FINAL_DIR/twittercf_sga -c $DIR/config/cluster_config.yaml -ec $DIR/cornflakes/cf-kv/twitter.yaml -lc $DIR/cornflakes/experiments/yamls/loopingparams/serialize_and_send/twitter.yaml
# ycsb exp
python3 $DIR/cornflakes/experiments/cf-kv-bench.py -e loop -f $FINAL_DIR/ycsbcf_sga -c $DIR/config/cluster_config.yaml -ec $DIR/cornflakes/cf-kv/ycsb.yaml -lt /proj/demeter-PG0/deeptir/ycsbc-traces/workloadc-1mil/workloadc-1mil-1-batched.load -qt /proj/demeter-PG0/deeptir/ycsbc-traces/workloadc-1mil/workloadc-1mil-1-batched.access -lc $DIR/cornflakes/experiments/yamls/loopingparams/serialize_and_send/ycsb4x1024.yaml
