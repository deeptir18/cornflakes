mod header_utils;
pub mod c_codegen;
pub mod rust_codegen;
pub mod utils;
use color_eyre::eyre::{bail, Result, WrapErr};
use header_utils::ProtoReprInfo;
use protobuf_parser::{FileDescriptor, Syntax};
use std::{
    fs::File,
    io::{prelude::*, BufReader},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderType {
    ConstantDeserialization,
    LinearDeserialization,
    LinearDeserializationRefCnt,
    Sga,
    RcSga,
}

impl std::str::FromStr for HeaderType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<HeaderType> {
        Ok(match s {
            "fixed" => HeaderType::ConstantDeserialization,
            "dynamic" => HeaderType::LinearDeserialization,
            "dynamic-rc" => HeaderType::LinearDeserializationRefCnt,
            "sga" => HeaderType::Sga,
            "rcsga" => HeaderType::RcSga,
            x => bail!("{} header type unknown.", x),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Rust,
    C,
}

impl std::str::FromStr for Language {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Language> {
        Ok(match s {
            "rust" => Language::Rust,
            "c" => Language::C,
            x => bail!("{} language type unknown.", x),
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Copy)]
pub struct CompileOptions {
    pub header_type: HeaderType,
    pub language: Language,
}

impl CompileOptions {
    pub fn new(header_type: HeaderType, language: Language) -> Self {
        CompileOptions {
            header_type: header_type,
            language: language,
        }
    }
}

/// Generate protobuf struct
/// Input: file to path to generate protobuf struct for.
/// Output: representation of protobuf structs in the file.
fn generate_proto_representation(input_file: &str) -> Result<ProtoReprInfo> {
    let file = File::open(input_file)?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;
    let fd = match FileDescriptor::parse(contents) {
        Ok(p) => {
            match p.syntax {
                Syntax::Proto3 => {}
                _ => {
                    bail!("Syntax {:?} not supported.", p.syntax);
                }
            }
            if p.enums.len() > 0 {
                bail!(
                    "Currently, the compiler does not support if the protobuf file contains enums."
                );
            }
            if p.extensions.len() > 0 {
                bail!("Compiler does not support extensions.");
            }
            if p.import_paths.len() > 0 {
                bail!("Compiler does not support external import paths.");
            }
            p
        }
        Err(e) => {
            bail!("Failed to parse protobuf file: {:?}", e);
        }
    };

    Ok(ProtoReprInfo::new(fd))
}

/// Write out generated Rust serialization code from schema to given output file.
pub fn compile(input_file: &str, output_folder: &str, options: CompileOptions) -> Result<()> {
    let mut repr = generate_proto_representation(input_file)?;
    if options.header_type == HeaderType::LinearDeserializationRefCnt
        || options.header_type == HeaderType::RcSga
    {
        repr.set_ref_counted();
    }
    if options.header_type == HeaderType::Sga || options.header_type == HeaderType::RcSga {
        repr.set_lifetime_name("obj");
    }
    match options.language {
        Language::C => {
            c_codegen::compile(&repr, output_folder, options).wrap_err(format!(
                "Failed to run C Code gen module on input_file: {} with options: {:?}.",
                input_file, options
            ))?;
        }
        Language::Rust => {
            rust_codegen::compile(&repr, output_folder, options).wrap_err(format!(
                "Failed to run Rust Code gen module on input_file: {} with options: {:?}.",
                input_file, options
            ))?;
        }
    }

    Ok(())
}
