# Update
sudo apt-get update

# Required for DPDK
sudo apt-get install libbsd-dev libelf-dev libpcap-dev


sudo apt-get install quilt chrpath graphviz swig libnl-route-3-200 libnl-route-3-dev dpatch libnl-3-dev
sudo apt-get install autoconf automake libtool curl make g++ unzip cmake
sudo apt-get install python3 python3-pip python3-setuptools python3-wheel ninja-build clang
sudo apt install r-base # required for plotting
sudo apt-get install libnuma-dev valgrind
sudo apt-get install -y libhiredis-dev # for redis experiments
sudo apt-get install libsystemd-dev pandoc cython
sudo apt-get install build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc

# Required for RDMA core (for mellanox datapath)
sudo apt-get install build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc

# Via pip
sudo pip3 install meson # when I tried installing meson without sudo, I can't find it in the path
pip3 install colorama
pip3 install gitpython
pip3 install tqdm
pip3 install parse
pip3 install fabric # for some reason this fails
pip3 install setuptools_rust # and then you need to install this
pip3 install fabric # and try this again

# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup component add rustfmt


# Installing the mellanox drivers


