use bindgen::Builder;
use std::env;
use std::fs::canonicalize;
use std::path::Path;
use std::process::Command;

fn main() {
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    println!(
        "cargo:rerun-if-changed={:?}",
        Path::new(&cargo_dir)
            .join("src")
            .join("dpdk_bindings")
            .join("inlined.c")
    );
    let header_path = Path::new(&cargo_dir)
        .join("src")
        .join("dpdk_bindings")
        .join("dpdk-headers.h");
    let dpdk_path = canonicalize(cargo_dir.clone().join("3rdparty").join("dpdk")).unwrap();
    let dpdk_dir = dpdk_path.as_path();

    let dpdk_install = dpdk_dir.clone().join("build");
    let pkg_config_path = dpdk_install.join("lib/x86_64-linux-gnu/pkgconfig");

    let cflags_bytes = Command::new("pkg-config")
        .env("PKG_CONFIG_PATH", &pkg_config_path)
        .args(&["--cflags", "libdpdk"])
        .output()
        .unwrap_or_else(|e| panic!("Failed pkg-config cflags: {:?}", e))
        .stdout;
    let cflags = String::from_utf8(cflags_bytes).unwrap();

    let mut header_locations = vec![];

    for flag in cflags.split(' ') {
        if flag.starts_with("-I") {
            let header_location = &flag[2..];
            header_locations.push(header_location.trim());
        }
    }

    let dpdk_bindings_folder = Path::new(&cargo_dir).join("src").join("dpdk_bindings");
    header_locations.push(dpdk_bindings_folder.to_str().unwrap().trim());
    let ldflags_bytes = Command::new("pkg-config")
        .env("PKG_CONFIG_PATH", &pkg_config_path)
        .args(&["--libs", "libdpdk"])
        .output()
        .unwrap_or_else(|e| panic!("Failed pkg-config ldflags: {:?}", e))
        .stdout;
    let ldflags = String::from_utf8(ldflags_bytes).unwrap();

    let mut library_location = None;
    let mut lib_names = vec![];

    for flag in ldflags.split(' ') {
        if flag.starts_with("-L") {
            library_location = Some(&flag[2..]);
        } else if flag.starts_with("-l") {
            lib_names.push(&flag[2..]);
        }
    }

    // Link in `librte_net_mlx5` and its dependencies if desired.
    #[cfg(feature = "mlx5")]
    {
        lib_names.extend(&[
            "rte_net_mlx5",
            "rte_bus_pci",
            "rte_bus_vdev",
            "rte_common_mlx5",
            "rte_bus_auxiliary",
        ]);
    }

    // Step 1: Now that we've compiled and installed DPDK, point cargo to the libraries.
    println!(
        "cargo:rustc-link-search=native={}",
        library_location.unwrap()
    );
    for lib_name in &lib_names {
        println!("cargo:rustc-link-lib={}", lib_name);
    }

    let mut builder = Builder::default();
    for header_location in &header_locations {
        println!("Adding header location {}", header_location);
        builder = builder.clang_arg(&format!("-I{}", header_location.trim()));
    }
    println!("Builder args: {:?}", builder.command_line_flags());
    let bindings = builder
        .header(header_path.to_str().unwrap())
        .allowlist_recursively(true)
        .allowlist_type("rte_mbuf")
        .allowlist_type("rte_mempool")
        .allowlist_function("rte_mempool_obj_iter")
        .allowlist_function("rte_mempool_mem_iter")
        .allowlist_function("rte_mempool_free")
        .allowlist_function("rte_eth_tx_burst")
        .allowlist_function("rte_eth_rx_burst")
        .allowlist_function("rte_eal_init")
        .allowlist_type("rte_eth_txconf")
        .allowlist_type("rte_eth_rxconf")
        .allowlist_function("rte_eth_dev_socket_id")
        .allowlist_function("rte_eth_dev_socket_id")
        .allowlist_function("rte_eth_rx_queue_setup")
        .allowlist_function("rte_eth_tx_queue_setup")
        .allowlist_type("rte_eth_fc_conf")
        .allowlist_function("rte_eth_dev_start")
        .allowlist_function("rte_eth_dev_flow_ctrl_get")
        .allowlist_function("rte_strerror")
        .allowlist_function("rte_eth_dev_count_avail")
        .allowlist_function("rte_eth_dev_is_valid_port")
        .allowlist_function("rte_eth_dev_flow_ctrl_set")
        .allowlist_var("RTE_PKTMBUF_HEADROOM")
        .allowlist_function("rte_mempool_avail_count")
        .allowlist_function("rte_mempool_in_use_count")
        .allowlist_var("RTE_ETHER_MAX_JUMBO_FRAME")
        .allowlist_type("rte_eth_link")
        .allowlist_function("rte_eth_link_get_nowait")
        .allowlist_var("RTE_ETH_LINK_UP")
        .allowlist_var("RTE_ETH_LINK_FULL_DUPLEX")
        .allowlist_function("rte_delay_us_block")
        .allowlist_function("rte_socket_id")
        .allowlist_function("rte_pktmbuf_pool_create")
        .allowlist_type("rte_pktmbuf_pool_private")
        .allowlist_function("rte_mempool_create_empty")
        .allowlist_function("rte_pktmbuf_pool_init")
        .allowlist_function("rte_mempool_populate_default")
        .allowlist_function("rte_pktmbuf_init")
        .allowlist_function("rte_mempool_avail_count")
        .allowlist_function("rte_mempool_in_use_count")
        .allowlist_type("rte_ether_addr")
        .allowlist_var("RTE_ETHER_MAX_JUMBO_FRAME_LEN")
        .allowlist_var("RTE_ETH_DEV_NO_OWNER")
        .allowlist_function("rte_eth_find_next_owned_by")
        .allowlist_var("RTE_MAX_ETHPORTS")
        .allowlist_function("rte_eth_dev_info_get")
        .allowlist_function("rte_eth_macaddr_get")
        .allowlist_var("RTE_ETH_RX_OFFLOAD_IPV4_CKSUM")
        .allowlist_var("RTE_ETH_RX_OFFLOAD_UDP_CKSUM")
        .allowlist_function("rte_auxiliarry_register")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .unwrap_or_else(|e| panic!("Failed to generate bindings: {:?}", e));
    let out_dir = env::var("OUT_DIR").unwrap();
    println!("Out dir: {:?}", out_dir);
    let dpdk_bindings = Path::new(&out_dir).join("dpdk_bindings.rs");
    bindings
        .write_to_file(dpdk_bindings)
        .expect("Could not write bindings");

    // Compile stubs for inlined functions
    let mut compiler = cc::Build::new();
    compiler.opt_level(3);
    compiler.pic(true);
    compiler.flag("-march=native");
    compiler.flag("-Wno-unused-parameter");
    compiler.flag("-Wno-deprecated-declarations");
    let inlined_file = Path::new(&cargo_dir)
        .join("src")
        .join("dpdk_bindings")
        .join("inlined.c");
    compiler.file(inlined_file.to_str().unwrap());
    // extra header for memory constants in bindings folder
    for header_location in &header_locations {
        compiler.include(header_location.trim());
    }
    compiler.compile("dpdkinlined");
}
