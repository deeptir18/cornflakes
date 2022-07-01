# Cornflakes
Cornflakes is a new zero-copy serialization library and runtime, particularly
aimed at applications that run on kernel bypass networking stacks.

## System Dependencies
All instructions below assume:
- Ubuntu 20.04 (other versions have not been tested)
- Mellanox OFED drivers 5.6-2.0.9.0 (other versions have not been tested
  thoroughly)
- Python version 3.8

## Building
### Clone cornflakes
git clone https://github.com/deeptir18/cornflakes.git --recursive

### Software dependencies.
0. Choose a folder $PACKAGES to install the serialization library baselines and
   $MLX5_DRIVER for placing the unzipped drivers (these should be absolute
paths).

1. Please download the [Mellanox OFED
   drivers](https://developer.nvidia.com/networking/mlnx-ofed-eula?mtag=linux_sw_drivers&mrequest=downloads&mtype=ofed&mver=MLNX_OFED-5.6-2.0.9.0&mname=MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.tgz)
and unzip the tarball into $MLX5_DRIVER. This link provides the link to check the user agreement and download version 5.6-2.0.9.0 for Ubuntu 20.04 systems; there are known bugs with earlier versions of the drivers.

2. Run the installation scripts. Set $PACKAGES to be the absolute path to
   download flatbuffers, capnproto and protobuf into; set $MLX5_DRIVER to be the
path where the drivers were unzipped; set $NR_HUGEPAGES to be the number of
huge pages per node. $CORNFLAKES is where cornflakes was cloned.
```
# Requires sudo and apt-debian package manager.
sudo $CORNFLAKES/install/install.sh $PACKAGES $MLX5_DRIVER $NR_HUGEPAGES
```

### Build.
```
cd $CORNFLAKES
// builds rdma-core, dpdk, mellanox drivers datapath
make submodules CONFIG_MLX5=y DEBUG=n
// build mellanox netperf microbenchmark
make mlx5-netperf CONFIG_MLX5=y
// build scatter-gather bench microbenchmark
make scatter-gather-bench
// builds cornflakes
make CONFIG_MLX5=y DEBUG=n
```

## Running
All Cornflakes related binaries, except the mlx5-netperf benchmark, require
DPDK. Some DPDK dependencies must be linked at runtime via LD_LIBRARY_PATH. As
such, use the following LD_LIBRARY_PATH when running any binaries:
```
export LD_LIBRARY_PATH=$CORNFLAKES/dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu
```
