#!/bin/bash
# installs protobuf, capnproto, flatbuffers (serialization baselines) into this
# location
PACKAGES=$1
MLX5_DRIVER=$2 # location where MLX5 driver has been unzipped
# number of hugepages per numa node
NR_HUGEPAGES=$3

./install-dependencies.sh
./install-rust.sh
./install-mlx5.sh $MLX5_DRIVER
./install-libraries.sh $PACKAGES
./install-hugepages.sh $NR_HUGEPAGES
