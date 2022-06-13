#!/bin/bash
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./test-hash \
     --server_mac=1c:34:da:41:c7:4c \
     --server_ip=128.110.218.241 \
     --client_ip=128.110.218.253 \
     --client_port=50000 \
     --server_port=50000
