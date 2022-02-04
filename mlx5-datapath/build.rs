use bindgen::Builder;
use std::{
    env,
    fs::canonicalize,
    path::{Path, PathBuf},
};

// TODO: Have a top level makefile instead of a top level cargo build.rs
// This way, we can directly build DPDK and the mellanox datapath from the makefile,
// rather than having to rely on just cargo.
fn main() {
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let out_dir = env::var("OUT_DIR").unwrap();
    println!(
        "cargo:rerun-if-changed={:?}",
        Path::new(&cargo_dir)
            .join("src")
            .join("mlx5_bindings")
            .join("inlined.c")
    );

    let header_path = Path::new(&cargo_dir)
        .join("src")
        .join("mlx5_bindings")
        .join("mlx5-headers.h");

    let mlx5_bindings = Path::new(&out_dir).join("mlx5_bindings.rs");
    // statically link in the mlx5-datapath
    let mlx5_wrapper_dir = Path::new(&cargo_dir)
        .join("..")
        .join("mlx5-datapath")
        .join("mlx5-wrapper");
    let mut library_locations: Vec<PathBuf> = Vec::default();
    library_locations.push(canonicalize(mlx5_wrapper_dir.clone().join("build")).unwrap());
    library_locations.push(
        canonicalize(
            mlx5_wrapper_dir
                .clone()
                .join("rdma-core")
                .join("build")
                .join("lib")
                .join("statics"),
        )
        .unwrap(),
    );
    library_locations.push(
        canonicalize(
            mlx5_wrapper_dir
                .clone()
                .join("rdma-core")
                .join("build")
                .join("util"),
        )
        .unwrap(),
    );
    library_locations.push(
        canonicalize(
            mlx5_wrapper_dir
                .clone()
                .join("rdma-core")
                .join("build")
                .join("ccan"),
        )
        .unwrap(),
    );

    let lib_names: Vec<String> = vec![
        "mlx5wrapper".to_string(),
        "mlx5".to_string(),
        "ibverbs".to_string(),
        "ccan".to_string(),
        "nl-3".to_string(),
        "nl-route-3".to_string(),
        "pthread".to_string(),
        "dl".to_string(),
        "numa".to_string(),
    ];

    let mut header_paths: Vec<PathBuf> = Vec::default();
    header_paths.push(canonicalize(mlx5_wrapper_dir.clone().join("inc")).unwrap());
    header_paths.push(
        canonicalize(
            mlx5_wrapper_dir
                .clone()
                .join("rdma-core")
                .join("build")
                .join("include"),
        )
        .unwrap(),
    );

    for lib_loc in library_locations.iter() {
        println!(
            "cargo:rustc-link-search=native={}",
            lib_loc.to_str().unwrap()
        );
    }

    for lib_name in lib_names.iter() {
        println!("cargo:rustc-link-lib={}", lib_name.as_str());
    }

    let mut builder = Builder::default();
    for path in header_paths.iter() {
        println!("cargo:include={}", path.to_str().unwrap());
        builder = builder.clang_arg(&format!("-I{}", path.to_str().unwrap()));
    }

    let bindings = builder
        .header(header_path.to_str().unwrap())
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .unwrap_or_else(|e| panic!("Failed to generate bindings: {:?}", e));
    bindings
        .write_to_file(mlx5_bindings)
        .expect("Could not write bindings");

    // compile stubs for inlined functions
    let mut compiler = cc::Build::new();
    compiler.opt_level(3);
    compiler.pic(true);
    compiler.flag("-march=native");
    compiler.flag("-Wno-unused-parameter");
    let inlined_file = Path::new(&cargo_dir)
        .join("src")
        .join("mlx5_bindings")
        .join("inlined.c");
    compiler.file(inlined_file.to_str().unwrap());
    for path in header_paths.iter() {
        compiler.include(path.to_str().unwrap());
    }
    compiler.compile("inlined");
}
