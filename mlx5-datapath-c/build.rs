extern crate cbindgen;

use cbindgen::{Config, Language};
use std::env;
use std::path::PathBuf;

fn main() {
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let output_file = PathBuf::from(&cargo_manifest_dir)
        .join("mlx5_datapath.h")
        .display()
        .to_string();

    let config = Config {
        language: Language::Cxx,
        ..Default::default()
    };

    cbindgen::generate_with_config(&cargo_manifest_dir, config)
        .unwrap()
        .write_to_file(&output_file);
}
