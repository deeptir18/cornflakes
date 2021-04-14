use super::{CompileOptions, HeaderType};
use color_eyre::eyre::{bail, Result, WrapErr};
use protobuf_parser::FileDescriptor;
use std::{fs::File, io::Write, process::Command, str};
use which::which;

mod constant_codegen;
mod linear_codegen;

pub fn compile(repr: FileDescriptor, output_file: &str, options: CompileOptions) -> Result<()> {
    let mut compiler = SerializationCompiler::new();
    match options.header_type {
        HeaderType::ConstantDeserialization => {
            constant_codegen::compile(repr, &mut compiler)
                .wrap_err("Constant codegen failed to generate code.")?;
        }
        HeaderType::LinearDeserialization => {
            linear_codegen::compile(repr, &mut compiler)
                .wrap_err("Linear codegen failed to generate code.")?;
        }
    }
    compiler.flush(output_file)?;
    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArgInfo {
    is_ref: bool,
    is_mut: bool,
    lifetime: Option<String>,
    arg_name: String,
}

impl ArgInfo {
    pub fn ref_arg(name: &str, lifetime: Option<String>) -> ArgInfo {
        ArgInfo {
            is_ref: true,
            is_mut: false,
            lifetime: lifetime,
            arg_name: String::from(name),
        }
    }

    pub fn ref_mut_arg(name: &str, lifetime: Option<String>) -> ArgInfo {
        ArgInfo {
            is_ref: true,
            is_mut: true,
            lifetime: lifetime,
            arg_name: String::from(name),
        }
    }

    pub fn owned(name: &str) -> ArgInfo {
        ArgInfo {
            is_ref: false,
            is_mut: false,
            lifetime: None,
            arg_name: String::from(name),
        }
    }

    pub fn get_type_string(&self) -> String {
        if self.is_ref && !self.is_mut {
            match &self.lifetime {
                Some(lifetime) => {
                    format!("&'{} {}", lifetime, self.arg_name)
                }
                None => {
                    format!("& {}", self.arg_name)
                }
            }
        } else if self.is_ref && self.is_mut {
            match &self.lifetime {
                Some(lifetime) => {
                    format!("&'{} mut {}", lifetime, self.arg_name)
                }
                None => {
                    format!("&mut {}", self.arg_name)
                }
            }
        } else {
            format!("{}", self.arg_name)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FunctionArg {
    SelfArg,
    MutSelfArg,
    Arg(String, ArgInfo),
}

impl FunctionArg {
    pub fn get_string(&self) -> String {
        match self {
            FunctionArg::SelfArg => "&self".to_string(),
            FunctionArg::MutSelfArg => "&mut self".to_string(),
            FunctionArg::Arg(name, info) => format!("{}: {}", name, info.get_type_string()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FunctionContext {
    pub name: String,
    pub is_pub: bool,
    pub args: Vec<FunctionArg>,
}

impl FunctionContext {
    pub fn new(name: &str, is_pub: bool, args: Vec<FunctionArg>) -> Self {
        FunctionContext {
            name: name.to_string(),
            is_pub: is_pub,
            args: args,
        }
    }

    pub fn push(&self) -> String {
        let is_pub_str = match self.is_pub {
            true => "pub ".to_string(),
            false => "".to_string(),
        };
        let args: Vec<String> = self.args.iter().map(|arg| arg.get_string()).collect();
        let args_string = args.join(", ");
        format!("{}fn {}({}) {{", is_pub_str, self.name, args_string)
    }

    pub fn pop(&self) -> String {
        "}".to_string()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructContext {
    name: String,
    derives_copy: bool,
    lifetime: String,
}

impl StructContext {
    pub fn new(name: &str, derives_copy: bool, lifetime: &str) -> Self {
        StructContext {
            name: name.to_string(),
            derives_copy: derives_copy,
            lifetime: lifetime.to_string(),
        }
    }

    pub fn push(&self) -> String {
        let derives = match self.derives_copy {
            true => "Debug, Clone, PartialEq, Eq, Copy",
            false => "Debug, Clone, PartialEq, Eq",
        };
        format!(
            "#[derive({})]\npub struct {}<'{}> {{",
            derives, self.name, self.lifetime
        )
    }

    pub fn pop(&self) -> String {
        "}".to_string()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImplContext {
    pub struct_name: String,
    pub trait_name: Option<String>,
    pub struct_lifetime: Option<String>,
}

impl ImplContext {
    pub fn new(name: &str, trait_name: Option<String>, lifetime: &str) -> Self {
        let struct_lifetime: Option<String> = match lifetime {
            "" => None,
            x => Some(x.to_string()),
        };
        ImplContext {
            struct_name: name.to_string(),
            trait_name: trait_name,
            struct_lifetime: struct_lifetime,
        }
    }

    pub fn push(&self) -> String {
        let lifetime_str = match &self.struct_lifetime {
            Some(x) => format!("<'{}>", x),
            None => "".to_string(),
        };
        match &self.trait_name {
            Some(t) => {
                format!(
                    "impl{} {} for {}{}",
                    lifetime_str, t, self.struct_name, lifetime_str
                )
            }
            None => {
                format!("impl{} {}{}", lifetime_str, self.struct_name, lifetime_str)
            }
        }
    }

    pub fn pop(&self) -> String {
        "}".to_string()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LoopBranch {
    If(String),
    ElseIf(String),
    Else,
    Finished,
}

impl LoopBranch {
    pub fn ifbranch(cond: &str) -> Self {
        LoopBranch::If(cond.to_string())
    }

    pub fn elseif(cond: &str) -> Self {
        LoopBranch::ElseIf(cond.to_string())
    }

    pub fn elsebranch() -> Self {
        LoopBranch::Else
    }

    pub fn fin() -> Self {
        LoopBranch::Finished
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LoopContext {
    branches: Vec<LoopBranch>,
    current_idx: usize,
}

impl LoopContext {
    pub fn new(mut branches: Vec<LoopBranch>) -> Self {
        branches.push(LoopBranch::fin());
        LoopContext {
            branches: branches,
            current_idx: 0,
        }
    }

    pub fn pop(&mut self) -> (String, bool) {
        let last_branch = &self.branches[self.current_idx];
        self.current_idx += 1;
        match last_branch {
            LoopBranch::If(cond) => (format!("if {} {{", cond), true),
            LoopBranch::ElseIf(cond) => (format!("}} else if {} {{", cond), true),
            LoopBranch::Else => ("}} else {{".to_string(), true),
            LoopBranch::Finished => ("}".to_string(), false),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Context {
    Function(FunctionContext),
    Struct(StructContext),
    Impl(ImplContext),
    Loop(LoopContext),
}

pub struct SerializationCompiler {
    current_string: String,
    current_context: Vec<Context>,
}

impl SerializationCompiler {
    pub fn new() -> Self {
        SerializationCompiler {
            current_string: "".to_string(),
            current_context: Vec::default(),
        }
    }

    pub fn add_line(&mut self, line: &str) -> Result<()> {
        self.current_string.push_str("\n");
        self.current_string
            .push_str(&("\t".repeat(self.current_indent_level())));
        self.current_string.push_str(line);
        Ok(())
    }

    pub fn current_indent_level(&self) -> usize {
        self.current_context.len()
    }

    pub fn add_context(&mut self, ctx: Context) {
        self.current_context.push(ctx);
    }

    pub fn pop_context(&mut self) -> Result<()> {
        let mut last_ctx = match self.current_context.pop() {
            Some(ctx) => ctx,
            None => {
                bail!("No context to pop.");
            }
        };

        match last_ctx {
            Context::Function(func_context) => {
                self.add_line(&func_context.pop())?;
            }
            Context::Struct(struct_context) => {
                self.add_line(&struct_context.pop())?;
            }
            Context::Impl(impl_context) => {
                self.add_line(&impl_context.pop())?;
            }
            Context::Loop(ref mut loop_context) => {
                let (line, cont) = loop_context.pop();
                self.add_line(&line)?;
                if cont {
                    self.current_context
                        .push(Context::Loop(loop_context.clone()));
                }
            }
        }
        Ok(())
    }

    pub fn add_newline(&mut self) -> Result<()> {
        self.current_string.push_str("\n");
        Ok(())
    }

    pub fn flush(&self, output_file: &str) -> Result<()> {
        let mut of = File::create(output_file)
            .wrap_err(format!("Failed to create output file at {}", output_file))?;
        let mut pos: usize = 0;
        while pos < self.current_string.as_str().len() {
            pos += of.write(&self.current_string.as_str().as_bytes()[pos..])?;
        }
        of.flush().wrap_err("Failed to flush file.")?;
        run_rustfmt(output_file).wrap_err("Failure from run_rustfmt.")?;
        Ok(())
    }
}

/// Programmatically runs rustfmt on a file.
pub fn run_rustfmt(output_file: &str) -> Result<()> {
    let rustfmt = which("rustfmt").wrap_err("Failed to find rustfmt.")?;
    Command::new(rustfmt)
        .arg(output_file)
        .output()
        .wrap_err("failed to execute Rustfmt process")?;
    Ok(())
}
