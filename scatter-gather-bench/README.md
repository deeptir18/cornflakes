# Scatter-Gather Bench
This is a simple C-benchmark to discern scatter-gather parameters.
It has currently been tested on a Mellanox CX-5 NIC.

# Setup
The code here depends on an already compiled DPDK, and this has been tested with
DPDK 20.11.
If the higher level cornflakes repository has been compiled, DPDK will be built.
within `cornflakes-libos/3rdparty/dpdk`.
To compile, specify the pkg-config-path for DPDK as shown below:
```
PKG_CONFIG_PATH=$PATH_TO_CORNFLAKES/cornflakes-libos/3rdparty/dpdk/install/lib/x86_64-linux-gnu/pkgconfig
make
```
