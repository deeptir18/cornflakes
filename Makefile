mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))

all: build

# TODO: make it so that if mlx5 drivers are not present on this machine, it only
# tries to build the dpdk version of things
# TODO:
# 1. Separate out mlx5 and dpdk and ice feature flags so dpdk works
# independently.
# 2. Have targets for cf-kv that depend on mlx5 or ice (and compile the
# datapath accordingly)
comma:= ,
empty:=
space:= $(empty) $(empty)

CARGOFLAGS ?=
CARGOFEATURES =
datapath =
ifneq ($(DEBUG), y)
	CARGOFLAGS += --release
endif

ifeq ($(CONFIG_MLX5), y)
	CARGOFEATURES +=mlx5
	datapath += mlx5-datapath
endif

ifeq ($(CONFIG_DPDK), y)
	CARGOFEATURES +=dpdk
endif

ifeq ($(CONFIG_ICE), y)
	CARGOFEATURES +=ice
	datapath += ice-datapath
endif

ifeq ($(PROFILER), y)
	CARGOFEATURES +=profiler
endif

ifeq ($(TIMETRACE), y)
	CARGOFEATURES += timetrace
endif

CARGOFEATURES := $(subst $(space),$(comma),$(CARGOFEATURES))

tapir: mlx5-datapath
	cargo b --package mlx5-datapath-c $(CARGOFLAGS)
	cargo b --package tapir $(CARGOFLAGS)
	CORNFLAKES_PATH=$(PWD) make -C tapir/c/tapir-cf

redis: mlx5-datapath
	cargo b --package mlx5-datapath-c $(CARGOFLAGS)
	cargo b --package cf-kv $(CARGOFLAGS)
	cd $(PWD)/cf-kv/c/kv-redis-c && cargo b $(CARGOFLAGS)
	cd ../../..
	CORNFLAKES_PATH=$(PWD) make -C redis -j

kv:$(datapath)
	cargo b --package cf-kv $(CARGOFLAGS) --features $(CARGOFEATURES)

ds-echo: $(datapath)
	cargo b --package ds-echo $(CARGOFLAGS) --features $(CARGOFEATURES)

sg-bench: $(datapath)
	cargo b --package sg-bench-client $(CARGOFLAGS) --features $(CARGOFEATURES)
	
build: mlx5-datapath
	cargo b $(CARGOFLAGS) --features $(CARGOFEATURES)

.PHONY: mlx5-datapath mlx5-netperf scatter-gather-bench ice-datapath kv ds-echo sg-bench

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
ifeq ($(CONFIG_MLX5), y)
	$(MAKE) submodules -C mlx5-datapath/mlx5-wrapper
	$(MAKE) submodules -C mlx5-netperf
endif
	# apply DPDK patch to dpdk datapath submodule
	git -C dpdk-datapath/3rdparty/dpdk apply ../dpdk-mlx.patch
	# build dpdk datapath submodule
	dpdk-datapath/3rdparty/build-dpdk.sh $(PWD)/dpdk-datapath/3rdparty/dpdk
