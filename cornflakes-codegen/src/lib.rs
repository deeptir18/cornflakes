pub mod rust_codegen;
pub mod utils;

use color_eyre::eyre::{bail, Result, WrapErr};
use protobuf_parser::FileDescriptor;
use std::{
    fs::File,
    io::{prelude::*, BufReader},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderType {
    ConstantDeserialization,
    LinearDeserialization,
}

impl std::str::FromStr for HeaderType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<HeaderType> {
        Ok(match s {
            "fixed_size" => HeaderType::ConstantDeserialization,
            "dynamic_size" => HeaderType::LinearDeserialization,
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
fn generate_proto_representation(input_file: &str) -> Result<FileDescriptor> {
    let file = File::open(input_file)?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;
    match FileDescriptor::parse(contents) {
        Ok(p) => Ok(p),
        Err(e) => {
            bail!("Failed to parse protobuf file: {:?}", e);
        }
    }
}

/// Write out generated Rust serialization code from schema to given output file.
pub fn compile(input_file: &str, output_file: &str, options: CompileOptions) -> Result<()> {
    let repr = generate_proto_representation(input_file)?;

    match options.language {
        Language::C => {
            bail!("Code generation module for C not implemented yet.");
        }
        Language::Rust => {
            rust_codegen::compile(repr, output_file, options).wrap_err(format!(
                "Failed to run Rust Code gen module on input_file: {} with options: {:?}.",
                input_file, options
            ))?;
        }
    }

    Ok(())
}
