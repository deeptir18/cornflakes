#!/bin/bash
MLX5_DRIVER=$1

pushd $MLX5_DRIVER
sudo ./mlnxofedinstall --upstream-libs --dpdk
sudo /etc/init.d/openibd restart
popd
