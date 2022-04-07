# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2010-2014 Intel Corporation

#export PKG_CONFIG_PATH=/proj/demeter-PG0/deeptir/cornflakes/cornflakes-libos/3rdparty/dpdk/install/lib/x86_64-linux-gnu/pkgconfig

# binary name
APP = netperf
CXX = g++

# all source are stored in SRCS-y
SRCS-y := main.c rand_exp.cc thread_info.cc


# Build using pkg-config variables if possible
# ifneq ($(shell pkg-config --exists libdpdk && echo 0),0)
# $(error "no installation of DPDK found")
# endif

all: shared
.PHONY: shared static
shared: build/$(APP)-shared
	ln -sf $(APP)-shared build/$(APP)
static: export build/$(APP)-static
	ln -sf $(APP)-static build/$(APP)

PKGCONF ?= pkg-config

PC_FILE := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) $(PKGCONF) --path libdpdk 2>/dev/null)
CFLAGS += -O3 $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) $(PKGCONF) --cflags libdpdk)
ifeq ($(DEBUG), y)
	CFLAGS += -D__DEBUG__ -O0 -g -ggdb
endif

ifeq ($(TIMERS), y)
	CFLAGS += -D__TIMERS__
endif

LDFLAGS_SHARED = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) $(PKGCONF) --libs libdpdk) -lrte_net_mlx5 -lrte_bus_pci -lrte_bus_vdev -lrte_common_mlx5 -lpthread -lm -lstdc++
LDFLAGS_STATIC = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) $(PKGCONF) --static --libs libdpdk) -lrte_net_mlx5 -lrte_bus_pci -lrte_bus_vdev -lrte_common_mlx5 -lpthread -lm -lstdc++

CFLAGS += -DALLOW_EXPERIMENTAL_API -lm -lstdc++

build/$(APP)-shared: $(SRCS-y) Makefile $(PC_FILE) | build
	$(CC) $(CFLAGS) $(SRCS-y) -o $@ $(LDFLAGS) $(LDFLAGS_SHARED)
	#$(CXX) $(CFLAGS) $(SRC-cc) -o $@ $(LDFLAGS) $(LDFLAGS_SHARED)

build/$(APP)-static: $(SRCS-y) Makefile $(PC_FILE) | build
	$(CC) $(CFLAGS) $(SRCS-y) -o $@ $(LDFLAGS) $(LDFLAGS_STATIC)
	#$(CXX) $(CFLAGS) $(SRC-cc) -o $@ $(LDFLAGS) $(LDFLAGS_SHARED)

build:
	@mkdir -p $@

.PHONY: clean
clean:
	rm -f build/$(APP) build/$(APP)-static build/$(APP)-shared
	test -d build && rmdir -p build || true
