use super::{
    header_utils::ProtoReprInfo, CompileOptions, HeaderType,
    rust_codegen::SerializationCompiler,
};
use color_eyre::eyre::{Result, WrapErr};
use std::str;

mod sga;

pub fn compile(repr: &ProtoReprInfo, output_folder: &str, options: CompileOptions) -> Result<()> {
    let mut compiler = SerializationCompiler::new();
    match options.header_type {
        HeaderType::Sga => {
            sga::compile(repr, &mut compiler).wrap_err("Sga codegen failed to generate code.")?;
        }
        ty => unimplemented!("unimplemented header type: {:?}", ty)
    }
    compiler.flush(&repr.get_output_file(output_folder).as_path())?;
    Ok(())
}
