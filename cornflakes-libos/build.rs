use bindgen::Builder;
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    // BUILD DPDK
    println!("cargo:rerun-if-changed=.git/modules/dpdk/HEAD");

    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    // config MLX5 drivers
    println!("cargo:warning=Building DPDK...");
    let dpdk_dir = cargo_dir.clone().join("3rdparty").join("dpdk");
    println!("DPDK PATH: {:?}", dpdk_dir);
    Command::new("./build-dpdk.sh")
        .args(&[dpdk_dir.to_str().unwrap()])
        .status()
        .unwrap_or_else(|e| panic!("Failed to build DPDK: {:?}", e));

    let dpdk_build = dpdk_dir.clone().join("build");
    let dpdk_libs = dpdk_build.clone().join("lib");

    println!("DPDK lib path: {:?}", dpdk_libs.to_str().unwrap());
    // change to static lib
    /*Command::new("ar")
    .args(&["crus", "libdpdk.a", "dpdk.o"])
    .current_dir(&dpdk_libs)
    .status()
    .unwrap();*/
    // use DPDK directory as -L
    println!(
        "cargo:rustc-env=LD_LIBRARY_PATH={}",
        dpdk_libs.to_str().unwrap()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        dpdk_libs.to_str().unwrap()
    );

    if dpdk_libs.join("libdpdk.so").exists() {
        println!("cargo:rustc-link-lib=dpdk");
        println!("cargo:rustc-link-lib=rte_eal");
        println!("cargo:rustc-link-lib=rte_kvargs");
    }
    let header_path = Path::new(&cargo_dir)
        .join("src")
        .join("native_include")
        .join("dpdk-headers.h");
    let dpdk_include_path = dpdk_build.clone().join("include");
    println!("Header path {:?}", header_path.to_str());
    let bindings = Builder::default()
        .header(header_path.to_str().unwrap())
        .blacklist_type("rte_arp_ipv4")
        .blacklist_type("rte_arp_hdr")
        .clang_args(vec!["-I", dpdk_include_path.to_str().unwrap()].iter())
        .blacklist_type("max_align_t") // https://github.com/servo/rust-bindgen/issues/550
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate DPDK bindings");
    let out_dir = env::var("OUT_DIR").unwrap();
    println!("Out dir: {:?}", out_dir);
    let dpdk_bindings = Path::new(&out_dir).join("dpdk_bindings.rs");
    bindings
        .write_to_file(dpdk_bindings)
        .expect("Could not write bindings");
    let dynamic_libs = &[
        "ibverbs",
        "mlx4",
        "mlx5",
        "nl-3",
        "nl-route-3",
        "numa",
        "pcap",
    ];
    for dynamic_lib in dynamic_libs {
        println!("cargo:rustc-link-lib=dylib={}", dynamic_lib);
    }
}
