/bin/bash

# currently works on debian systems (with access to apt-get)
sudo apt-get update

# Required for DPDK
sudo apt-get install libbsd-dev libelf-dev libpcap-dev


sudo apt-get install -y quilt chrpath graphviz swig libnl-route-3-200 libnl-route-3-dev dpatch libnl-3-dev
sudo apt-get install -y autoconf automake libtool curl make g++ unzip cmake
sudo apt-get install -y python3 python3-pip python3-setuptools python3-wheel ninja-build clang
sudo apt-get install -y r-base # required for plotting
sudo apt-get install -y libnuma-dev valgrind
sudo apt-get install -y libhiredis-dev # for redis experiments
sudo apt-get install -y libsystemd-dev pandoc cython
sudo apt-get install -y build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc
sudo apt-get install -y libnuma-dev

# Required for RDMA core (for mellanox datapath)
sudo apt-get -y install build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc

# Via pip
sudo pip3 install meson # when I tried installing meson without sudo, I can't find it in the path
pip3 install colorama gitpython tqdm parse
pip3 install setuptools_rust # and then you need to install this
pip3 install fabric # and try this again
pip3 install pyelftools
pip3 install numpy pandas torch

