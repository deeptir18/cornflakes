#!/bin/bash
# installs protobuf, capnproto, flatbuffers (serialization baselines) into this
# location
PACKAGES=$1
# number of hugepages per numa node
NR_HUGEPAGES=$2

./install-dependencies.sh
./install-rust.sh
./install-mlx5.sh $PACKAGES
PRIMARY=$PRIMARY ./install-libraries.sh $PACKAGES
./install-hugepages.sh $NR_HUGEPAGES
