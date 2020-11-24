#!/bin/bash
# this script builds DPDK with support for MLX5 drivers
# assumes x86
# works for versions 19.11
DIR=$1
pushd $DIR
echo "IN BUILD DPDK SCRIPT"
sed -i 's/CONFIG_RTE_LIBRTE_MLX5_PMD=n/CONFIG_RTE_LIBRTE_MLX5_PMD=y/g' config/common_base
sed -i 's/CONFIG_RTE_BUILD_SHARED_LIB=n/CONFIG_RTE_BUILD_SHARED_LIB=y/g' config/common_base
make config T=x86_64-native-linuxapp-gcc
make -j
popd
