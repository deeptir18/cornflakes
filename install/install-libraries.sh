#!/bin/bash


# Required for protobuf
sudo apt install -y apt-transport-https curl gnupg
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
sudo mv bazel-archive-keyring.gpg /usr/share/keyrings
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
sudo apt update && sudo apt install -y bazel

# install and clone protobuf and flatbuffers and capnproto into this folder
PACKAGES=$1

pushd $PACKAGES
# clone protobuf
if [ "$PRIMARY" = "y" ]; then
    git clone https://github.com/protocolbuffers/protobuf.git --recursive
fi
cd protobuf
if [ "$PRIMARY" = "y" ]; then
    bazel build :protoc :protobuf
fi
sudo cp bazel-bin/protoc /usr/local/bin
cd ..

# clone flatbuffers
if [ "$PRIMARY" = "y" ]; then
    git clone https://github.com/google/flatbuffers.git
fi
cd flatbuffers
if [ "$PRIMARY" = "y" ]; then
    git checkout v22.9.24
    cmake -G "Unix Makefiles"
    make
fi
cd ..
sudo ln -s $PACKAGES/flatbuffers/flatc /usr/local/bin/flatc
chmod +x $PACKAGES/flatbuffers/flatc
flatc --version # sanity check that it works



# clone and build capnproto
if [ "$PRIMARY" = "y" ]; then
    curl -O https://capnproto.org/capnproto-c++-0.9.1.tar.gz
    tar zxf capnproto-c++-0.9.1.tar.gz
fi
cd capnproto-c++-0.9.1
if [ "$PRIMARY" = "y" ]; then
    ./configure
    make -j6 check
fi
sudo make install
popd
