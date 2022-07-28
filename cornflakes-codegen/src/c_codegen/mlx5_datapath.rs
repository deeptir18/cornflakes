use ffiber::{CDylibCompiler, types::*};
use color_eyre::eyre::Result;
use std::path::Path;

pub fn compile(output_folder: &str) -> Result<()> {
    let mut compiler = CDylibCompiler::new("mlx5-datapath", &output_folder);
    let src_folder = Path::new(&output_folder)
        .join("mlx5-datapath-c")
        .join("src");
    gen_cargo_toml(&mut compiler)?;
    gen_dependencies(&mut compiler)?;
    gen_constructor(&mut compiler)?;
    gen_configuration(&mut compiler)?;
    gen_pop(&mut compiler)?;
    gen_push(&mut compiler)?;
    compiler.flush()?;
    Ok(())
}

fn gen_cargo_toml(compiler: &mut CDylibCompiler) -> Result<()> {
    compiler.add_crate("cornflakes-libos",
        vec![("path", "\"../../../cornflakes-libos\"")])?;
    compiler.add_crate("cornflakes-utils",
        vec![("path", "\"../../../cornflakes-utils\"")])?;
    compiler.add_crate("mlx5-datapath",
        vec![("path", "\"../../../mlx5-datapath\"")])?;
    Ok(())
}

fn gen_dependencies(compiler: &mut CDylibCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_libos::{ \
        datapath::{Datapath, InlineMode}, \
        {ArenaOrderedRcSga, OrderedSga}, \
    }")?;
    compiler.add_dependency("cornflakes_utils::AppMode")?;
    compiler.add_dependency("mlx5_datapath::datapath::connection::Mlx5Connection")?;
    compiler.add_dependency("std::{ffi::CStr, net::Ipv4Addr, str::FromStr}")?;
    Ok(())
}

fn gen_constructor(compiler: &mut CDylibCompiler) -> Result<()> {
    // Mlx5Connection_new
    Ok(())
}

fn gen_configuration(compiler: &mut CDylibCompiler) -> Result<()> {
    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_set_copying_threshold
    compiler.add_extern_c_function(
        ArgType::new_struct("Mlx5Connection"),
        "set_copying_threshold",
        Some(SelfArgType::Mut),
        vec![
            ("copying_threshold", ArgType::Primitive("usize".to_string())),
        ],
        None,
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_set_inline_mode
    // TODO: enums

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_add_memory_pool
    compiler.add_extern_c_function(
        ArgType::new_struct("Mlx5Connection"),
        "add_memory_pool",
        Some(SelfArgType::Mut),
        vec![
            ("buf_size", ArgType::Primitive("usize".to_string())),
            ("min_elts", ArgType::Primitive("usize".to_string())),
        ],
        None,
        true,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_add_tx_mempool
    compiler.add_extern_c_function(
        ArgType::new_struct("Mlx5Connection"),
        "add_tx_mempool",
        Some(SelfArgType::Mut),
        vec![
            ("size", ArgType::Primitive("usize".to_string())),
            ("min_elts", ArgType::Primitive("usize".to_string())),
        ],
        None,
        true,
    )?;
    Ok(())
}

fn gen_pop(compiler: &mut CDylibCompiler) -> Result<()> {
    // Mlx5Connection_pop
    Ok(())
}

fn gen_push(compiler: &mut CDylibCompiler) -> Result<()> {
    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_push_ordered_sgas

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_queue_arena_ordered_sga

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_queue_single_buffer_with_copy
    Ok(())
}
