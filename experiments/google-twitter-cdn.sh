#!/bin/bash
python3 cdn-bench.py -e loop -f /mydata/deeptir/final_sosp_results/cdn_april6 -c /mydata/deeptir/config/cluster_config.yaml -ec ../cf-kv/cdn.yaml --trace_file /mydata/deeptir/cdn1mil/gen_sequence.txt -lc yamls/loopingparams/cf-kv-cdn.yaml

python3 google-bench.py -e loop -f /mydata/deeptir/final_sosp_results/googleproto_april6 -c /mydata/deeptir/config/cluster_config.yaml -ec /mydata/deeptir/cornflakes/cf-kv/google.yaml -lc /mydata/deeptir/cornflakes/experiments/yamls/loopingparams/cf-kv-google.yaml

python3 twitter-bench.py -e loop -f /mydata/deeptir/final_sosp_results/twittercf_april6 -c /mydata/deeptir/config/cluster_config.yaml -ec /mydata/deeptir/cornflakes/cf-kv/twitter.yaml -lc /mydata/deeptir/cornflakes/experiments/yamls/loopingparams/twitter_traces/cf-kv-twitter.yaml --trace /mydata/deeptir/twitter/cluster4.0_8192.log
