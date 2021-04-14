use super::SerializationCompiler;
use color_eyre::eyre::Result;
use protobuf_parser::FileDescriptor;

pub fn compile(repr: FileDescriptor, compiler: &mut SerializationCompiler) -> Result<()> {
    tracing::debug!("File contents: {:?}", repr);
    compiler.add_newline()?;
    Ok(())
}
