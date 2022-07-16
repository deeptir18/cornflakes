use super::{
    Language,
    header_utils::ProtoReprInfo, CompileOptions, HeaderType,
    rust_codegen::{self, Context, FunctionContext, SerializationCompiler},
};
use color_eyre::eyre::{Result, WrapErr};
use std::{str, fs, path::Path};

mod common;
mod rcsga;
mod sga;

/// package-name-c/
///     src/
///         package_name.rs
///         lib.rs
///     build.rs
///     Cargo.toml
pub fn compile(repr: &ProtoReprInfo, output_folder: &str, options: CompileOptions) -> Result<()> {
    let package_folder = repr.get_c_package_name(output_folder);
    let src_folder = package_folder.join("src");
    fs::create_dir_all(&src_folder)?;

    gen_build_rs(repr, &package_folder)?;
    gen_cargo_toml(repr, &package_folder)?;
    gen_rust_code(repr, &src_folder, &options)?;
    gen_c_code(repr, &src_folder, options)?;
    Ok(())
}

fn gen_build_rs(repr: &ProtoReprInfo, package_folder: &Path) -> Result<()> {
    let mut compiler = SerializationCompiler::new();
    compiler.add_extern_crate("cbindgen")?;
    compiler.add_newline()?;
    compiler.add_dependency("std::{env, path::PathBuf}")?;
    compiler.add_dependency("cbindgen::Config")?;
    compiler.add_newline()?;

    let func_context = FunctionContext::new("main", false, vec![], "");
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_def_with_let(false, None, "cargo_manifest_dir",
        "env::var(\"CARGO_MANIFEST_DIR\").unwrap()",
    )?;
    compiler.add_def_with_let(false, None, "output_file", &format!(
        "PathBuf::from(&cargo_manifest_dir).join(\"{}.h\").display().to_string()",
        repr.get_repr().package,
    ))?;
    compiler.add_def_with_let(false, None, "config",
        "Config { language: cbindgen::Language::C, ..Default::default() }"
    )?;
    compiler.add_line(
        "cbindgen::generate_with_config(&cargo_manifest_dir, config) \
        .unwrap() \
        .write_to_file(&output_file);"
    )?;
    compiler.pop_context()?;
    compiler.flush(&package_folder.join("build.rs"))?;
    Ok(())
}

fn gen_cargo_toml(repr: &ProtoReprInfo, package_folder: &Path) -> Result<()> {
    let package_name = repr.get_repr().package;
    let package_name_c = format!("{}-c", str::replace(&package_name, "_", "-"));
    let package_name_rust = format!("{}_c", &package_name);

    let mut compiler = SerializationCompiler::new();
    compiler.add_line("[package]")?;
    compiler.add_line(&format!("name = \"{}\"", package_name_c))?;
    compiler.add_line("version = \"0.1.0\"")?;
    compiler.add_line("edition = \"2021\"")?;
    compiler.add_newline()?;

    compiler.add_line("[lib]")?;
    compiler.add_line(&format!("name = \"{}\"", package_name_rust))?;
    compiler.add_line("path = \"src/lib.rs\"")?;
    compiler.add_line("crate-type = [\"cdylib\"]")?;
    compiler.add_newline()?;

    // Dependencies
    // TODO: path to local dependencies depends on OUT_DIR
    compiler.add_line("[dependencies]")?;
    if repr.has_int_field() {
        compiler.add_line("byteorder = \"1.3.4\"")?;
    }
    compiler.add_line("bitmaps = \"3.2.0\"")?;
    compiler.add_line("color-eyre = \"0.5\"")?;
    compiler.add_line("cornflakes-libos = { path = \"../../../cornflakes-libos\" }")?;
    compiler.add_line("cornflakes-codegen = { path = \"../../../cornflakes-codegen\" }")?;
    compiler.add_line("mlx5-datapath = { path = \"../../../mlx5-datapath\" }")?;
    // TODO: rcsga only
    compiler.add_line("bumpalo = { git = \"https://github.com/deeptir18/bumpalo\", features = [\"collections\"] }")?;
    compiler.add_newline()?;

    // Build dependencies
    compiler.add_line("[build-dependencies]")?;
    compiler.add_line("cbindgen = \"0.23.0\"")?;
    compiler.add_newline()?;

    // Exclude the generated package from the workspace
    compiler.add_line("[workspace]")?;

    compiler.flush(&package_folder.join("Cargo.toml"))?;
    Ok(())
}

fn gen_rust_code(repr: &ProtoReprInfo, src_folder: &Path, options: &CompileOptions) -> Result<()> {
    rust_codegen::compile(repr, src_folder.to_str().unwrap(), CompileOptions {
        needs_datapath_param: false,
        header_type: options.header_type,
        language: Language::Rust,
    })?;
    Ok(())
}

fn gen_c_code(repr: &ProtoReprInfo, src_folder: &Path, options: CompileOptions) -> Result<()> {
    // Import generated Rust functions
    let package_name = repr.get_repr().package;
    let mut compiler = SerializationCompiler::new();
    compiler.add_mod_declaration(&package_name)?;
    compiler.add_dependency(&format!("{}::*", package_name))?;

    // Generate extern "C" code
    match options.header_type {
        HeaderType::Sga => {
            sga::compile(repr, &mut compiler).wrap_err("Sga codegen failed to generate code.")?;
        }
        HeaderType::RcSga => {
            rcsga::compile(repr, &mut compiler).wrap_err("RcSga codegen failed to generate code.")?;
        }
        ty => unimplemented!("unimplemented header type: {:?}", ty)
    }
    compiler.flush(&src_folder.join("lib.rs"))?;
    Ok(())
}
