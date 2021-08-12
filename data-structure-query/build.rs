use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
use std::{env, fs::canonicalize, path::Path};

fn main() {
    // store all compiled proto files in out_dir
    let out_dir = env::var("OUT_DIR").unwrap();
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let ds_query_src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();

    // compile cornflakes dynamic
    let input_cf_path = ds_query_src_path.clone().join("cornflakes_dynamic");
    let input_cf_file = input_cf_path.clone().join("ds_query_cf_dynamic.proto");
    match compile(
        input_cf_file.as_path().to_str().unwrap(),
        &out_dir,
        CompileOptions::new(HeaderType::LinearDeserialization, Language::Rust),
    ) {
        Ok(_) => {}
        Err(e) => {
            panic!("Cornflakes codegen failed: {:?}", e);
        }
    }
}
