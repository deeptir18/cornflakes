use capnpc;
//use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
//use cxx_build;
//use protoc_rust;
use std::{
    env,
    fs::canonicalize,
    //fs::{canonicalize, read_to_string, File},
    //io::{BufWriter, Write},
    path::Path,
    process::Command,
};

fn main() {
    // rerun-if-changed
    // cereal cc bridge files
    //println!("cargo:rerun-if-changed=src/cereal/cereal_classes.cc");
    //println!("cargo:rerun-if-changed=src/cereal/include/cereal_headers.hh");
    // protobuf, cornflakes, flatbuffers, capnproto schema files
    //println!("cargo:rerun-if-changed=src/protobuf/echo_proto.proto");
    //println!("cargo:rerun-if-changed=src/cornflakes_dynamic/kv_cf_dynamic.proto");
    println!("cargo:rerun-if-changed=src/capnproto/kv.capnp");
    //println!("cargo:rerun-if-changed=src/flatbuffers/echo_fb.fbs");

    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let kv_src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir_path = Path::new(&out_dir);

    // store all compiled proto files in out_dir
    /* let _include_path = canonicalize(cargo_dir.parent().unwrap())
        .unwrap()
        .join("include");

    // compile cornflakes dynamic
    let input_cf_path = kv_src_path.clone().join("cornflakes_dynamic");
    let input_cf_file = input_cf_path.clone().join("kv_cf_dynamic.proto");
    match compile(
        input_cf_file.as_path().to_str().unwrap(),
        &out_dir,
        CompileOptions::new(HeaderType::LinearDeserialization, Language::Rust),
    ) {
        Ok(_) => {}
        Err(e) => {
            panic!("Cornflakes codegen failed: {:?}", e);
        }
    }*/

    // compile capnproto echo
    let input_capnp_path = kv_src_path.clone().join("capnproto");
    let input_capnp_file = input_capnp_path.clone().join("kv.capnp");
    capnpc::CompilerCommand::new()
        .output_path(out_dir_path)
        .src_prefix(input_capnp_path.as_path().to_str().unwrap())
        .file(input_capnp_file.as_path().to_str().unwrap())
        .run()
        .expect(&format!(
            "Capnp compilation failed on {:?}.",
            input_capnp_file
        ));

    // compile flatbuffers
    let input_fb_path = kv_src_path.clone().join("flatbuffers");
    let input_fb_file = input_fb_path.clone().join("kv_fb.fbs");

    // requires flatbuffers is installed as flatc in path
    Command::new("flatc")
        .args(&[
            "--rust",
            "-o",
            out_dir_path.to_str().unwrap(),
            input_fb_file.as_path().to_str().unwrap(),
        ])
        .output()
        .expect(&format!(
            "Failed to run flatc compiler on {:?}.",
            input_fb_file
        ));
}
