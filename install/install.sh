#!/bin/bash
# installs protobuf, capnproto, flatbuffers (serialization baselines) into this
# location
PACKAGES=$1
MLX5_DRIVER=$2 # location where MLX5 driver has been unzipped
# number of hugepages per numa node
NR_HUGEPAGES=$3

$CORNFLAKES/install/install-dependencies.sh
$CORNFLAKES/install/build-rust.sh
$CORNFLAKES/install/install-mlx5.sh $MLX5_DRIVER
$CORNFLAKES/install/install-libraries.sh $PACKAGES
$CORNFLAKES/install/install-hugepages.sh $NR_HUGEPAGES
