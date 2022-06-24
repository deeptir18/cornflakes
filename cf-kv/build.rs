use capnpc;
use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
use std::{
    env,
    fs::{canonicalize, read_to_string, File},
    io::{BufWriter, Write},
    path::Path,
    process::Command,
};

fn main() {
    println!("cargo:rerun-if-changed=src/cornflakes_dynamic/kv.proto");
    println!("cargo:rerun-if-changed=src/flatbuffers/cf_kv_fb.fbs");
    println!("cargo:rerun-if-changed=src/capnproto/cf_kv.capnp");
    println!("cargo:rerun-if-changed=src/protobuf/kv.proto");
    // store all compiled proto files in out_dir
    //let out_dir = env::var("OUT_DIR").unwrap();
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_dir = Path::new(&cargo_manifest_dir);
    let kv_src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();

    //let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    //let cargo_dir = Path::new(&cargo_manifest_dir);
    //let kv_src_path = canonicalize(cargo_dir.clone().join("src")).unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir_path = Path::new(&out_dir);
    //let include_path = canonicalize(cargo_dir.parent().unwrap())
    //    .unwrap()
    //    .join("include");

    let input_cf_path = kv_src_path.clone().join("cornflakes_dynamic");
    let input_cf_file_sga = input_cf_path.clone().join("kv.proto");
    // with ref counting
    match compile(
        input_cf_file_sga.as_path().to_str().unwrap(),
        &out_dir,
        CompileOptions::new_with_datapath_param(HeaderType::RcSga, Language::Rust),
    ) {
        Ok(_) => {}
        Err(e) => {
            panic!("Cornflakes dynamic rc sga failed: {:?}", e);
        }
    }

    let input_cf_file_sga = input_cf_path.clone().join("kv_nonrefcounted.proto");
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

    // compile flatbuffers
    let input_fb_path = kv_src_path.clone().join("flatbuffers");
    let input_fb_file = input_fb_path.clone().join("cf_kv_fb.fbs");

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

    // compile capnproto
    let input_capnp_path = kv_src_path.clone().join("capnproto");
    let input_capnp_file = input_capnp_path.clone().join("cf_kv.capnp");
    capnpc::CompilerCommand::new()
        .output_path(out_dir_path)
        .src_prefix(input_capnp_path.as_path().to_str().unwrap())
        .file(input_capnp_file.as_path().to_str().unwrap())
        .run()
        .expect(&format!(
            "Capnp compilation failed on {:?}.",
            input_capnp_file
        ));

    // compile protobuf baseline
    let input_proto_path = kv_src_path.clone().join("protobuf");
    let input_proto_file = input_proto_path.clone().join("kv_protobuf.proto");
    let mut customize = protobuf_codegen::Customize::default();
    customize = customize.gen_mod_rs(true);
    protobuf_codegen::Codegen::new()
        .out_dir(&out_dir)
        .inputs(&[input_proto_file.as_path()])
        .includes(&[input_proto_path.as_path()])
        .customize(customize)
        .run()
        .expect(&format!(
            "Protoc compilation failed on {:?}.",
            input_proto_file
        ));

    // issue with includes and ![allow()]s in generated code; remove them:
    // Resolve the path to the generated file.
    let proto_outpath = Path::new(&out_dir).join("kv_protobuf.rs");
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
}
