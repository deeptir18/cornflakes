BINARY = ./server

CURRENT_DIR = $(pwd)
CORNFLAKES_DIR = $(HOME)/cornflakes

# To change CF_KV_OUT_DIR, also modify $(CORNFLAKES_DIR)/Cargo.toml so the
# package is in the current workspace.
CF_KV_OUT_DIR = $(CORNFLAKES_DIR)/cf-kv/c
# CF_KV_OUT_DIR = $(CORNFLAKES_DIR)

INCLUDE_FLAGS = -I$(CF_KV_OUT_DIR)/kv-redis-c -I$(CORNFLAKES_DIR)/linux-datapath-c
LINKING_FLAGS = -L$(CORNFLAKES_DIR)/target/release -llinux_datapath_c \
	-L$(CF_KV_OUT_DIR)/kv-redis-c/target/release -lkv_proto_c

build: deps
# 	$(CC) server.c $(INCLUDE_FLAGS) $(LINKING_FLAGS) -o $(BINARY)

deps:
# 	cd $(CORNFLAKES_DIR)/linux-datapath-c/ && cargo b --release
	cd $(CORNFLAKES_DIR)/cf-kv/ && cargo b --release
	cd $(CF_KV_OUT_DIR)/kv-redis-c/ && cargo b --release
	cd $(CURRENT_DIR)

run:
	LD_LIBRARY_PATH=$(CORNFLAKES_DIR)/target/release $(BINARY)

client:
	cd $(CORNFLAKES_DIR) && ./target/release/cf_kv_linux \
		--config_file example_config.yaml --debug_level info --mode client
