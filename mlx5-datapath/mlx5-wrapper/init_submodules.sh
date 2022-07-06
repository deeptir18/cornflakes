#!/bin/sh

set -e

CORES=`getconf _NPROCESSORS_ONLN`

echo building RDMA-CORE
cd rdma-core
EXTRA_CMAKE_FLAGS='-DENABLE_STATIC=1 -DCMAKE_C_FLAGS="-fPIC"' MAKEFLAGS=-j$CORES ./build.sh
cd ..
