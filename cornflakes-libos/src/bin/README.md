This file contains two `netperf` binaries to run and test out DPDK in Rust.

## DPDK_ECHO


## FAST_ECHO
This binary is akin to the dpdk-netperf app I built in (C)[https://github.com/deeptir18/dpdk-netperf] and contains similar command line options.

Running the server:
```
sudo nice -n -19 taskset 0x1 env LD_LIBRARY_PATH=$LD_LIBRARY_PATH
target/release/fast_echo --config_file <PATH TO CONFIG> --debug_level
<debug,info,warn,error> --mode server --memory <EXTERNAL,DPDK>
--num_mbufs <1,2> --zero-copy
```

Running the client:
```
sudo nice -n -19 taskset 0x1 env LD_LIBRARY_PATH=$LD_LIBRARY_PATH
target/release/fast_echo --config_file <PATH TO CONFIG> --debug_level
<debug,info,warn,error> --mode client  --server_ip <SERVER_IP> --time 5 --rate 120000 --size 1024
```
