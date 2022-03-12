#!/bin/bash
PACKAGES=$1
MLX5_DRIVER=$2

pushd $MLX5_DRIVER
sudo ./mlnxofedinstall --upstream-libs --dpdk
sudo /etc/init.d/openibd restart
