#!/bin/bash
# this script builds DPDK using meson and ninja
# builds into DPDK/build
# installs into DPDK/install
DPDK=$1 # has to be a full path
pushd $DPDK
INSTALL_DIR="$DPDK/install"
meson --prefix=$INSTALL_DIR build 
pushd build
ninja
ninja install
popd
popd
