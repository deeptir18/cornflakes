use color_eyre::eyre::{Result, WrapErr};
use cornflakes_codegen::{compile, CompileOptions, HeaderType, Language};
use cornflakes_utils::{global_debug_init, TraceLevel};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Cornflakes Compiler.",
    about = "Cornflakes code generation module."
)]

struct Opt {
    #[structopt(
        short = "f",
        long = "file",
        help = "Input file to generate serialization code for."
    )]
    input_file: String,
    #[structopt(
        short = "o",
        long = "output_folder",
        help = "Output file to write generated code.",
        default_value = "."
    )]
    output_folder: String,
    #[structopt(
        short = "debug",
        long = "debug_level",
        help = "Configure tracing settings.",
        default_value = "warn"
    )]
    trace_level: TraceLevel,
    #[structopt(
        short = "l",
        long = "language",
        help = "Output code generation language.",
        default_value = "rust"
    )]
    language: Language,
    #[structopt(
        short = "h",
        long = "header_type",
        help = "Generated header type.",
        default_value = "fixed"
    )]
    header_type: HeaderType,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    compile(
        &opt.input_file,
        &opt.output_folder,
        CompileOptions::new(opt.header_type, opt.language),
    )
    .wrap_err("Compile failed.")?;
    Ok(())
}
