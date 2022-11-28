ROOT_PATH=.

# shared toolchain definitions
INC = -I$(ROOT_PATH)/inc -I$(DPDK_PATH)/build/include -I$(DPDK_PATH)/lib/ethdev -I$(DPDK_PATH)/lib/eal/include -I$(DPDK_PATH)/drivers/net/ice

CFLAGS  = -g -Wall -D_GNU_SOURCE $(INC) -lstdc++ -fsanitize=unreachable  -Wno-address-of-packed-member -mssse3
EXTRA_CFLAGS = -lm
LDFLAGS_SHARED =
LDFLAGS_STATIC =
LD      = gcc
CC      = gcc
LDXX	= g++
CXX	= g++
AR      = ar

ifeq ($(DEBUG), n)
ifeq ($(GDB), n)
	CFLAGS += -O3
endif
endif

ifeq ($(DEBUG), y)
	CFLAGS += -O0 -D__DEBUG__
endif

ifeq ($(TIMERS), y)
	CFLAGS += -D__TIMERS__
endif
ifeq ($(GDB), y)
	CFLAGS += -O0 -ggdb
endif

DPDK_LIBS= -L$(DPDK_PATH)/build/lib
DPDK_LIBS += -Wl,-whole-archive -Wl,-no-whole-archive
DPDK_LIBS += -lrte_mempool
DPDK_LIBS += -lrte_ring
DPDK_LIBS += -lrte_eal
DPDK_LIBS += -lrte_kvargs
DPDK_LIBS += -lrte_ethdev
DPDK_LIBS += -lrte_mbuf

LDFLAGS_STATIC += $(DPDK_LIBS)
LDFLAGS_SHARED += $(DPDK_LIBS)

LDFLAGS_STATIC += -lstdc++
LDFLAGS_SHARED += -ldstc++

# library name
LIB = libicewrapper

# source files
SRCS-y := $(wildcard src/*.c)

all: static
.PHONY: static

static: build $(LIB).a

$(LIB).a: ice_rxtx.o init.o mbuf.o mempool.o mem.o time.o
	ar rcs build/$@ build/ice_rxtx.o build/init.o build/mbuf.o build/mempool.o build/mem.o build/time.o

ice_rxtx.o:
	$(CC) $(CFLAGS) src/ice_rxtx.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

init.o:
	$(CC) $(CFLAGS) src/init.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

mbuf.o:
	$(CC) $(CFLAGS) src/mbuf.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

mempool.o:
	$(CC) $(CFLAGS) src/mempool.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

mem.o:
	$(CC) $(CFLAGS) src/mem.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

time.o:
	$(CC) $(CFLAGS) src/time.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

build:
	@mkdir -p $@

.PHONY: clean
clean:
	rm -rf build
