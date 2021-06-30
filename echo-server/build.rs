use capnpc;
use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
use cxx_build;
use protoc_rust;
use std::{
    env,
    fs::{canonicalize, read_to_string, File},
    io::{BufWriter, Write},
    path::Path,
    process::Command,
};

fn main() {
    // store all compiled proto files in out_dir
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir_path = Path::new(&out_dir);
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let echo_src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();
    let include_path = canonicalize(cargo_dir.parent().unwrap())
        .unwrap()
        .join("include");

    // compile c++ bridge to cereal
    cxx_build::bridge("src/cereal/mod.rs")
        .file("src/cereal/cereal_classes.cc")
        .flag_if_supported("-std=c++14")
        .includes(vec![include_path])
        .compile("cxxbridge-cereal-api");

    // compile protobuf echo
    let input_proto_path = echo_src_path.clone().join("protobuf");
    let input_proto_file = input_proto_path.clone().join("echo_proto.proto");
    protoc_rust::Codegen::new()
        .out_dir(&out_dir)
        .inputs(&[input_proto_file.as_path()])
        .includes(&[input_proto_path.as_path()])
        .customize(protoc_rust::Customize {
            gen_mod_rs: Some(true),
            ..Default::default()
        })
        .run()
        .expect(&format!(
            "Protoc compilation failed on {:?}.",
            input_proto_file
        ));

    // issue with includes and ![allow()]s in generated code; remove them:
    // Resolve the path to the generated file.
    let proto_outpath = Path::new(&out_dir).join("echo_proto.rs");
    // Read the generated code to a string.
    let code = read_to_string(&proto_outpath).expect("Failed to read generated file");
    // Write filtered lines to the same file.
    let mut writer = BufWriter::new(File::create(proto_outpath).unwrap());
    for line in code.lines() {
        if !line.starts_with("//!") && !line.starts_with("#!") {
            writer.write_all(line.as_bytes()).unwrap();
            writer.write_all(&[b'\n']).unwrap();
        }
    }
    // compile capnproto echo
    let input_capnp_path = echo_src_path.clone().join("capnproto");
    let input_capnp_file = input_capnp_path.clone().join("echo.capnp");
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
    let input_fb_path = echo_src_path.clone().join("flatbuffers");
    let input_fb_file = input_fb_path.clone().join("echo_fb.fbs");
    // requires flatbuffers is installed as flatc
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

    // compile cornflakes dynamic
    let input_cf_path = echo_src_path.clone().join("cornflakes_dynamic");
    let input_cf_file = input_cf_path.clone().join("echo_cf_dynamic.proto");
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

    // compile cornflakes fixed
    let input_cf_path = echo_src_path.clone().join("cornflakes_fixed");
    let input_cf_file = input_cf_path.clone().join("echo_cf_fixed.proto");
    match compile(
        input_cf_file.as_path().to_str().unwrap(),
        &out_dir,
        CompileOptions::new(HeaderType::ConstantDeserialization, Language::Rust),
    ) {
        Ok(_) => {}
        Err(e) => {
            panic!("Cornflakes codegen failed: {:?}", e);
        }
    }
}
