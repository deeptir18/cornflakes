#!/bin/bash

python3 experiments/twitter-bench.py -e loop -f /mydata/deeptir/final_sosp_results/twittercf_april10 -c /mydata/deeptir/config/cluster_config.yaml -ec /mydata/deeptir/cornflakes/cf-kv/twitter.yaml -lc /mydata/deeptir/cornflakes/experiments/yamls/loopingparams/twitter_traces/cf-kv-twitter.yaml --trace /mydata/deeptir/twitter/cluster4.0_8192.log

python3 experiments/cf-kv-bench.py -e loop -f ../cfkv_mmtstudy/ -c /mydata/deeptir/config/cluster_config.yaml -ec cf-kv/ycsb.yaml -lt /proj/demeter-PG0/deeptir/ycsbc-traces/workloadc-1mil/workloadc-1mil-1-batched.load -qt /proj/demeter-PG0/deeptir/ycsbc-traces/workloadc-1mil/workloadc-1mil-1-batched.access -lc experiments/yamls/loopingparams/cf-kv-mmtstudy.yaml
