#!/bin/bash

sudo apt-get update

# Required for DPDK
sudo apt-get install -y libbsd-dev libelf-dev libpcap-dev

sudo apt-get install -y quilt chrpath graphviz swig libnl-route-3-200 libnl-route-3-dev dpatch libnl-3-dev
sudo apt-get install -y autoconf automake libtool curl make g++ unzip cmake
sudo apt-get install -y python3 python3-pip python3-setuptools python3-wheel ninja-build clang
sudo apt-get install -y libkrb5-dev
sudo apt-get install -y libnuma-dev valgrind
sudo apt-get install -y libhiredis-dev # for redis experiments
sudo apt-get install -y libsystemd-dev pandoc cython
sudo apt-get install -y build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc
sudo apt-get install -y libnuma-dev
sudo apt-get install -y libfreetype6-dev

# Required for RDMA core (for mellanox datapath)
sudo apt-get -y install build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc

# Via pip
## some bug with openSSL -- need to uninstall and install
sudo rm -rf /usr/lib/python3/dist-packages/OpenSSL
sudo pip3 install pyopenssl
sudo pip3 install pyopenssl --upgrade
sudo pip3 install python-gssapi
sudo pip3 install meson
pip3 install colorama gitpython tqdm parse
pip3 install setuptools_rust
pip3 install fabric==2.6.0 # and try this again
pip3 install pyelftools
pip3 install numpy pandas
pip3 install agenda toml
pip3 install result
