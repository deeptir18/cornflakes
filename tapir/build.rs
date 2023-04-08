extern crate cbindgen;

use cbindgen::{Config, Language as BindLanguage};
use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
use std::{
    env,
    fs::{canonicalize},
    path::{Path, PathBuf},
};

fn main() {
    // generate the serialization functions needed.
    println!("cargo:rerun-if-changed=src/cf-dynamic/tapir_proto.proto");
    // store all compiled proto files in out_dir
    //let out_dir = env::var("OUT_DIR").unwrap();
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let tapir_src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();

    let out_dir = env::var("OUT_DIR").unwrap();

    let input_cf_path = tapir_src_path.clone().join("cf_dynamic");
    let input_cf_file = input_cf_path.clone().join("tapir_proto.proto");
    // with ref counting
    println!("{:?}", out_dir);
    match compile(
        input_cf_file.as_path().to_str().unwrap(),
        &out_dir,
        CompileOptions::new_with_datapath_param(HeaderType::HybridArenaObject, Language::Rust),
    ) {
        Ok(_) => {}
        Err(e) => {
            panic!("Cornflakes dynamic rc sga failed: {:?}", e);
        }
    }

    // generate the cpp header
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
   
    let cpp_header_file = PathBuf::from(&cargo_manifest_dir)
        .join("tapir_serialized_cpp.h")
        .display()
        .to_string();

    let config = Config {
        language: BindLanguage::Cxx,
        ..Default::default()
    };

    cbindgen::generate_with_config(&cargo_manifest_dir, config)
        .unwrap()
        .write_to_file(&cpp_header_file);
   
}
