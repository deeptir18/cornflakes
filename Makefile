mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))

all: build

# TODO: make it so that if mlx5 drivers are not present on this machine, it only
# tries to build the dpdk version of things
comma:= ,
empty:=
space:= $(empty) $(empty)

CARGOFLAGS ?=
CARGOFEATURES =
ifneq ($(DEBUG), y)
	CARGOFLAGS += --release
endif

ifeq ($(CONFIG_MLX5), y)
	CARGOFEATURES +=mlx5
endif


ifeq ($(PROFILER), y)
	CARGOFEATURES +=profiler
endif

CARGOFEATURES := $(subst $(space),$(comma),$(CARGOFEATURES))

redis: mlx5-datapath
	cargo b --package cf-kv $(CARGOFLAGS)
	cd $(PWD)/cf-kv/c/kv-sga-cornflakes-c && cargo b $(CARGOFLAGS)
	cd ../../..
	CORNFLAKES_PATH=$(PWD) make -C redis -j

build: mlx5-datapath
	cargo b $(CARGOFLAGS) --features $(CARGOFEATURES)

.PHONY: mlx5-datapath mlx5-netperf scatter-gather-bench ice-datapath

# scatter-gather bench microbenchmark
scatter-gather-bench:
	PKG_CONFIG_PATH=$(mkfile_dir)dpdk-datapath/3rdparty/dpdk/build/lib/x86_64-linux-gnu/pkgconfig make -C scatter-gather-bench CONFIG_MLX5=$(CONFIG_MLX5) DEBUG=$(DEBUG) GDB=$(GDB)

dpdk:
	# apply DPDK patch to dpdk datapath submodule
	git -C dpdk-datapath/3rdparty/dpdk apply ../dpdk-mlx.patch
	# build dpdk datapath submodule
	dpdk-datapath/3rdparty/build-dpdk.sh $(PWD)/dpdk-datapath/3rdparty/dpdk


ice-datapath:
	$(MAKE) -C ice-datapath/ice-wrapper DPDK_PATH=$(mkfile_dir)dpdk-datapath/3rdparty/dpdk

mlx5-datapath:
	$(MAKE) -C mlx5-datapath/mlx5-wrapper CONFIG_MLX5=$(CONFIG_MLX5) DEBUG=$(DEBUG) GDB=$(GDB)

# mlx5 netperf microbenchmark
mlx5-netperf:
	$(MAKE) -C mlx5-netperf CONFIG_MLX5=$(CONFIG_MLX5) DEBUG=$(DEBUG)

# clean up the system and components
clean:
	rm -rf mlx5-datapath/mlx5-wrapper/rdma-core/build
	$(MAKE) -C mlx5-datapath/mlx5-wrapper clean
	$(MAKE) -C mlx5-netperf clean
	$(MAKE) -C scatter-gather-bench clean
	$(MAKE) -C ice-datapath/ice-wrapper clean
	rm -rf dpdk-datapath/3rdparty/dpdk/build
	rm -rf dpdk-datapath/3rdparty/dpdk/install
	cargo clean

# initialize all of the submodules
submodules:
	# build rdma-core
	git submodule init
	git submodule update --init -f --recursive
	$(MAKE) submodules -C mlx5-datapath/mlx5-wrapper
	$(MAKE) submodules -C mlx5-netperf
	# apply DPDK patch to dpdk datapath submodule
	git -C dpdk-datapath/3rdparty/dpdk apply ../dpdk-mlx.patch
	# build dpdk datapath submodule
	dpdk-datapath/3rdparty/build-dpdk.sh $(PWD)/dpdk-datapath/3rdparty/dpdk

	
	



