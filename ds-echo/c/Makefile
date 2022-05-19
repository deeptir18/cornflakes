BINARY = ./server

CORNFLAKES_DIR = $(HOME)/cornflakes

INCLUDE_FLAGS = -I../../echo-dynamic-sga-c -I../../linux-datapath-c
LINKING_FLAGS = -L../../target/release -lecho_dynamic_sga_c  -llinux_datapath_c

# Build linux-datapath-c with --release
# Build echo-dynamic-sga-c with --release
build:
	$(CC) server.c $(INCLUDE_FLAGS) $(LINKING_FLAGS) -o $(BINARY)

run:
	LD_LIBRARY_PATH=$(CORNFLAKES_DIR)/target/release $(BINARY)
