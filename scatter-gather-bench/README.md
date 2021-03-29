# Scatter-Gather Bench
This is a simple C-benchmark to discern scatter-gather parameters.
It has currently been tested on a Mellanox CX-5 NIC.

# Setup
The code for this benchmark depends on an already-compiled DPDK, from the
`cornflakes-libos` sub-folder in this repository (which is a submodule).
The makefile manually links the `PKG_CONFIG_PATH` to the correct path, so do not
modify the location of this folder in relation to the other folders in this
repository.
