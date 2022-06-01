BINARY = ./server

CURRENT_DIR = $(pwd)
CORNFLAKES_DIR = $(HOME)/cornflakes

# To change DS_ECHO_OUT_DIR, also modify $(CORNFLAKES_DIR)/Cargo.toml so the
# package is in the current workspace.
DS_ECHO_OUT_DIR = $(CORNFLAKES_DIR)/target/release/build/ds-echo-bb1d115642fc3475/out
# DS_ECHO_OUT_DIR = $(CORNFLAKES_DIR)

INCLUDE_FLAGS = -I$(DS_ECHO_OUT_DIR)/echo-dynamic-sga-c -I$(CORNFLAKES_DIR)/linux-datapath-c
LINKING_FLAGS = -L$(CORNFLAKES_DIR)/target/release -lecho_dynamic_sga_c  -llinux_datapath_c

build: deps
	$(CC) server.c $(INCLUDE_FLAGS) $(LINKING_FLAGS) -o $(BINARY)

deps:
	cd $(CORNFLAKES_DIR)/linux-datapath-c/ && cargo b --release
	cd $(CORNFLAKES_DIR)/ds-echo/ && cargo b --release
	cd $(DS_ECHO_OUT_DIR)/echo-dynamic-sga-c/ && cargo b --release
	cd $(CURRENT_DIR)

run:
	LD_LIBRARY_PATH=$(CORNFLAKES_DIR)/target/release $(BINARY)

client:
	cd $(CORNFLAKES_DIR) && ./target/release/ds_echo_linux \
		--config_file example_config.yaml --debug_level info --mode client
