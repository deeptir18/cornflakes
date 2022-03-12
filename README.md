# Cornflakes
Cornflakes is a new zero-copy serialization library and runtime, particularly
aimed at applications that run on kernel bypass networking stacks.

## Building
Follow these instructions to build and use Cornflakes.
We have only tested code on systems that run Ubuntu 18.04, and we currently only
have support for the Mellanox drivers.

### Clone cornflakes
git clone https://github.com/deeptir18/cornflakes.git --recursive

### Software dependencies.
0. Choose a folder $PACKAGES to install the serialization library baselines and
   $MLX5_DRIVER for placing the unzipped drivers (these should be absolute
paths).

1. Please download the [Mellanox OFED drivers](https://developer.nvidia.com/networking/mlnx-ofed-eula?mtag=linux_sw_drivers&mrequest=downloads&mtype=ofed&mver=MLNX_OFED-5.0-2.1.8.0&mname=MLNX_OFED_LINUX-5.0-2.1.8.0-ubuntu18.04-x86_64.tgz) and unzip the tarball into $MLX5_DRIVER. This link provides the link to check the user agreement and download version 5.0-2.1.8 for Ubuntu 18.04 systems; we currently have not tested support with other versions of the drivers.

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

make submodules CONFIG_MLX5=y DEBUG=n
export LD_LIBRARY_PATH=$CORNFLAKES/cornflakes-libos/3rdparty/dpdk/install/lib/x86_64-linux-gnu/
make CONFIG_MLX5=y DEBUG=n
```
