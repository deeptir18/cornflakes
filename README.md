# Cornflakes
Cornflakes is a new zero-copy serialization library and networking runtime, particularly
aimed at applications that run on kernel bypass networking stacks. This
repository contains an implementation of Cornflakes, which includes:
1. Code-generation modules for generating Rust serialization code with
   Cornflakes from protobuf schema files, and C-bindings to this serialization
   code.
2. A base serialization library (in Rust) with support for basic string/bytes
   types, and lists of strings, bytes, or complex objects.
3. A networking stack API that supports the Cornflakes serialization API as well
   as other baseline designs.
4. Datapath implementations of the networking interface for the Mellanox OFED interface
   and the Intel ICE interface for Intel and Mellanox NICs.
5. We have partial support for DPDK as well (not all features of Cornflakes are
   supported by this datapath).
6. Various applications written for cornflakes, including an:
- Echo server (built to compare Conflakes to protobuf, capnproto, and
  flatbuffers) 
- Key value store (built to compare Cornflakes to protobuf, capnproto and
  flatbuffers)
- Redis integration (linked in a submodule)


## System Dependencies
All instructions below assume:
- Ubuntu 20.04 (other versions have not been tested)
- For Mellanox NICs, we assume OFED driver version 5.6-2.0.9.0 (other versions
  have not been tested thoroughly)

## Hardware Requirements
Cornflakes provides improvements in low-latency microsecond-scale networks; we
have primarily tested on machines with Mellanox CX5, CX6 or Intel e810-series
100Gb or higher NICs (we have also tested on machines with Mellanox CX5 25Gb
NICs). Machines with these NICs are available on Cloudlab.
[This repository](https://github.com/deeptir18/cornflakes-scripts/) contains
instructions for getting started with our custom Cloudlab profile and
reproducing some of the results from the research paper.

## Building
### Software dependencies
For Intel NICs, ensure the proper drivers are installed (e.g., [ice drivers for
linux](https://www.intel.com/content/www/us/en/download/19630/intel-network-adapter-driver-for-e810-series-devices-under-linux.html)).
For Mellanox NICs, we have provided install scripts that setup the specific
version of the OFED drivers we use.

### Clone cornflakes
git clone https://github.com/deeptir18/cornflakes.git --recursive

### Software dependencies.
0. Choose a folder $PACKAGES to install the serialization library baselines and
   $MLX5_DRIVER for placing the unzipped drivers (these should be absolute
paths).

1. (Mellanox) Please download the [Mellanox OFED
   drivers](https://developer.nvidia.com/networking/mlnx-ofed-eula?mtag=linux_sw_drivers&mrequest=downloads&mtype=ofed&mver=MLNX_OFED-5.6-2.0.9.0&mname=MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.tgz)
and unzip the tarball into $MLX5_DRIVER. This link provides the link to check the user agreement and download version 5.6-2.0.9.0 for Ubuntu 20.04 systems; there are known bugs with earlier versions of the drivers.

2. Run the installation scripts. Set $PACKAGES to be the absolute path to
   download flatbuffers, capnproto and protobuf into; set $MLX5_DRIVER to be the
path where the drivers were unzipped; set $NR_HUGEPAGES to be the number of
huge pages per node. $CORNFLAKES is where cornflakes was cloned.
```
# Requires sudo and apt-debian package manager.
# clone cornflakes
git clone https://github.com/deeptir18/cornflakes.git --recursive
cd cornflakes
# install packages with apt-get (will run sudo commands)
./install/install-dependencies.sh
# install rust
./install/install-rust.sh
# install protobuf, flatbuffers and capnproto
PRIMARY=y ./install/install-libraries.sh $PACKAGES
# install version 5.6-2.0.9.0 of the Mellanox Drivers (Mellanox only)
./install/install-mlx5.sh $PACKAGES
# need to reboot machines after installing mellanox drivers to reload
sudo reboot
```

### Machine setup
After the dependencies are setup and the new drivers have been loaded, some
machine settings post reboot are required to run Cornflakes:
```
cd cornflakes
# setup huge pages (must be done everytime machine is rebooted)
./install/install-hugepages.sh $NR_HUGEPAGES
# Disable c-states and (optionally) try to disable CPU frequency changes
./install/set_freq.sh
```

For Intel NICs, you must use the DPDK devbind tool to unbind the driver from the
Kernel driver and instead bind it to the igb_uio driver. Follow something like
these instructions:
```
## get dpdk
git clone https://dpdk.org/git/dpdk
## get dpdk-kmods
git clone https://dpdk.org/git/dpdk-kmods
cd dpdk-kmods/linux/igb_uio
make
sudo modprobe uio
sudo insmod igb_uio.ko
## might need to unbind first
sudo $PATH_TO_DPDK/usertools/dpdk-devbind.py --bind igb_uio <pci addr>
```

### Build.
For Mellanox NICs, provide `CONFIG_MLX5`.
For Intel NICs, provide `CONFIG_ICE`.
For DPDK, provide `CONFIG_DPDK`.
For our evaluation, we usually used the DPDK version of the client apps and the
Mellanox or Intel versions of the server apps.
```
cd $CORNFLAKES
// builds rdma-core, dpdk, mellanox drivers datapath
make submodules [CONFIG_MLX5=y] DEBUG=n
// build mellanox netperf microbenchmark (mellanox only)
make mlx5-netperf CONFIG_MLX5=y
// build kv app
make kv [CONFIG_MLX5=y] [CONFIG_DPDK=y] [CONFIG_ICE=y]
// build ds-echo
make ds-echo [CONFIG_MLX5=y] [CONFIG_DPDK=y] [CONFIG_ICE=y]
// build redis (mellanox only)
make redis CONFIG_MLX5=y
```

## Running
All Cornflakes binaries built on DPDK, except the mlx5-netperf benchmark, require
DPDK. Some DPDK dependencies must be linked at runtime via LD_LIBRARY_PATH. As
such, use the following LD_LIBRARY_PATH when running any binaries:
```
export LD_LIBRARY_PATH=$CORNFLAKES/dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu
```

The [cornflakes-scripts repo](https://github.com/deeptir18/cornflakes-scripts)
contains some instructions for actually running Python scripts to run Cornflakes
and compare it to other libraries.


## Code structure / developing
If you would like to build a new application using Cornflakes, please take a
look at the `build.rs`, `Cargo.toml` and `src/lib.rs` files inside `ds-echo` or
`cf-kv` to see how we compile code from data structure schemas and use
Cornflakes generated code in applications.
