ROOT_PATH=.

# shared toolchain definitions
INC = -I$(ROOT_PATH)/inc
CFLAGS  = -g -Wall -D_GNU_SOURCE $(INC) -lstdc++ -fsanitize=unreachable 
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

MLX5_INC = -I$(ROOT_PATH)/rdma-core/build/include
MLX5_LIBS = -L$(ROOT_PATH)/rdma-core/build/lib/statics -L$(ROOT_PATH)/rdma-core/build/util -L$(ROOT_PATH)/rdma-core/build/ccan
MLX5_LIBS +=  -lmlx5 -libverbs -lrdma_util -lccan -lnl-3 -lnl-route-3  -lpthread -ldl -lnuma

ifeq ($(CONFIG_MLX5),y)
CFLAGS += -DMLX5
LDFLAGS_STATIC += $(MLX5_LIBS)
LDFLAGS_SHARED += $(MLX5_LIBS)
INC += $(MLX5_INC)
endif

LDFLAGS_STATIC += -lstdc++
LDFLAGS_SHARED += -ldstc++

# library name
LIB = libmlx5wrapper

# source files
SRCS-y := $(wildcard src/*.c)

all: static
.PHONY: static

static: build $(LIB).a

$(LIB).a: runtime.o init.o mbuf.o mempool.o mem.o time.o
	ar rcs build/$@ build/runtime.o build/init.o build/mbuf.o build/mempool.o build/mem.o build/time.o

runtime.o:
	$(CC) $(CFLAGS) src/runtime.c -c -o build/$@ $(EXTRA_CFLAGS) $(LDFLAGS) $(LDFLAGS_STATIC)

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

.PHONY: submodules
submodules:
	$(ROOT_PATH)/init_submodules.sh

.PHONY: clean
clean:
	rm -rf build
