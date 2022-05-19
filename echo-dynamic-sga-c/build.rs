use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
use std::{
    env,
    fs::canonicalize,
    path::Path,
};

fn main() {
    // rerun-if-changed
    println!("cargo:rerun-if-changed=src/cornflakes_dynamic/echo_dynamic_sga.proto");

    // store all compiled proto files in out_dir
    let out_dir = env::var("OUT_DIR").unwrap();
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();

    // compile cornflakes dynamic
    let input_cf_path = src_path.clone();
    let input_cf_file_sga = input_cf_path.clone().join("echo_dynamic_sga.proto");
    // with ref counting
    match compile(
        input_cf_file_sga.as_path().to_str().unwrap(),
        &out_dir,
        CompileOptions::new(HeaderType::Sga, Language::Rust),
    ) {
        Ok(_) => {}
        Err(e) => {
            panic!("Cornflakes dynamic sga failed: {:?}", e);
        }
    }
}
