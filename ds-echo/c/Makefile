BINARY = ./server

CURRENT_DIR = $(pwd)
CORNFLAKES_DIR = $(HOME)/cornflakes

INCLUDE_FLAGS = -I$(CORNFLAKES_DIR)/echo-dynamic-sga-c -I$(CORNFLAKES_DIR)/linux-datapath-c
LINKING_FLAGS = -L$(CORNFLAKES_DIR)/target/release -lecho_dynamic_sga_c  -llinux_datapath_c

build: deps
	$(CC) server.c $(INCLUDE_FLAGS) $(LINKING_FLAGS) -o $(BINARY)

deps:
	cd $(CORNFLAKES_DIR)/linux-datapath-c/ && cargo b --release
	cd $(CORNFLAKES_DIR)/echo-dynamic-sga-c/ && cargo b --release
	cd $(CURRENT_DIR)

run:
	LD_LIBRARY_PATH=$(CORNFLAKES_DIR)/target/release $(BINARY)
