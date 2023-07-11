#!/bin/bash
# this script builds DPDK using meson and ninja
# builds into DPDK/build
# installs into DPDK/install
# compatible with newest OFED drivers (5.6.xx and later, where compatibility
# with mlx4 drivers is disabled, so we must explicitly disable, so mlx4 doesn't
# show up in the package config path
DPDK=$1 # has to be a full path
pushd $DPDK
meson build 
meson configure -Ddisable_drivers=net/mlx4,common/mlx4,regex/cn9k build
meson configure -Dprefix=$DPDK/build build
ninja -C build
ninja -C build install
popd
