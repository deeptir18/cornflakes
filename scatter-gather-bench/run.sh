#!/bin/bash
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/proj/demeter-PG0/prthaker/cornflakes/build/netperf -c 0xff -n 8 -w 0000:41:00.0,txq_inline_mpw=256,txqs_min_inline=0 --proc-type=auto -- --mode=CLIENT --num_segments=1 --server_ip=128.110.218.251 --ip=128.110.218.243 --server_mac=1c:34:da:41:c6:fc --time=5 --rate=1 --array_size=3276800000 --segment_size=8192
