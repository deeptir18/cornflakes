use super::{
    Language, rust_codegen,
    header_utils::ProtoReprInfo, CompileOptions, HeaderType,
};
use ffiber::CDylibCompiler;
use color_eyre::eyre::{Result, WrapErr};
use std::{str, path::Path};

mod common;
mod rcsga;
mod mlx5_datapath;

/// package-name-c/
///     src/
///         package_name.rs
///         lib.rs
///     build.rs
///     Cargo.toml
pub fn compile(repr: &ProtoReprInfo, output_folder: &str, options: CompileOptions) -> Result<()> {
    let mut compiler = CDylibCompiler::new("kv-sga-cornflakes", &output_folder);
    let src_folder = Path::new(&output_folder)
        .join("kv-sga-cornflakes-c")
        .join("src");
    gen_cargo_toml(&mut compiler, repr)?;
    gen_c_code(&mut compiler, repr, &options)?;
    compiler.flush()?;

    rust_codegen::compile(repr, src_folder.to_str().unwrap(), CompileOptions {
        needs_datapath_param: false,
        header_type: options.header_type,
        language: Language::Rust,
    })?;
    mlx5_datapath::compile(output_folder)?;
    Ok(())
}

fn gen_cargo_toml(
    compiler: &mut CDylibCompiler,
    repr: &ProtoReprInfo,
) -> Result<()> {
    if repr.has_int_field() {
        compiler.add_crate_version("byteorder", "1.3.4");
    }
    compiler.add_crate_version("bitmaps", "3.2.0");
    compiler.add_crate_version("color-eyre", "0.5");
    compiler.add_crate("cornflakes-libos",
        vec![("path", "\"../../../cornflakes-libos\"")])?;
    compiler.add_crate("cornflakes-codegen",
        vec![("path", "\"../../../cornflakes-codegen\"")])?;
    compiler.add_crate("mlx5-datapath",
        vec![("path", "\"../../../mlx5-datapath\"")])?;
    compiler.add_crate("bumpalo",
        vec![
            ("git", "\"https://github.com/deeptir18/bumpalo\""),
            ("features", "[\"collections\"]"),
        ])?;
    Ok(())
}

fn gen_c_code(
    compiler: &mut CDylibCompiler,
    repr: &ProtoReprInfo,
    options: &CompileOptions,
) -> Result<()> {
    // Import generated Rust functions
    let package_name = repr.get_repr().package;
    compiler.inner.add_mod_declaration(&package_name)?;
    compiler.add_dependency(&format!("{}::*", package_name))?;

    // Generate extern "C" code
    match options.header_type {
        HeaderType::RcSga => {
            rcsga::compile(repr, compiler).wrap_err("RcSga codegen failed to generate code.")?;
        }
        ty => unimplemented!("unimplemented header type: {:?}", ty)
    }
    Ok(())
}
