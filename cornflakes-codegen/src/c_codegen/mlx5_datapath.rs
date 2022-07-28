use ffiber::{CDylibCompiler, types::*, compiler::*};
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
    compiler.add_crate_version("color-eyre", "0.5");
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
    compiler.add_dependency("color_eyre::Result")?;
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
        SelfArgType::Mut,
        "set_copying_threshold",
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
        SelfArgType::Mut,
        "add_memory_pool",
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
        SelfArgType::Mut,
        "add_tx_mempool",
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
    // TODO: buffers of different types

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_queue_arena_ordered_sga
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("conn", "&mut Mlx5Connection")),
        FunctionArg::CArg(CArgInfo::arg("msg_id", "u32")),
        FunctionArg::CArg(CArgInfo::arg("conn_id", "usize")),
        FunctionArg::CArg(CArgInfo::arg("rcsga", "ArenaOrderedRcSga<Mlx5Connection>")),
        FunctionArg::CArg(CArgInfo::arg("end_batch", "bool")),
    ];
    compiler.inner.add_context(Context::Function(FunctionContext::new(
        "Mlx5Connection_queue_arena_ordered_rcsga_inner",
        false, args, "Result<()>",
    )))?;
    compiler.inner.add_line("conn.queue_arena_ordered_rcsga((msg_id, conn_id, rcsga), end_batch)")?;
    compiler.inner.pop_context()?; // end of function
    compiler.inner.add_newline()?;
    compiler.add_extern_c_function_standalone(
        "Mlx5Connection_queue_arena_ordered_rcsga",
        "Mlx5Connection_queue_arena_ordered_rcsga_inner",
        vec![
            ("conn", ArgType::new_ref_mut("Mlx5Connection")),
            ("msg_id", ArgType::Primitive("u32".to_string())),
            ("conn_id", ArgType::Primitive("usize".to_string())),
            ("rcsga", ArgType::Struct {
                name: "ArenaOrderedRcSga".to_string(),
                params: vec![Box::new(ArgType::new_struct("Mlx5Connection"))],
            }),
            ("end_batch", ArgType::Primitive("bool".to_string())),
        ],
        None,
        true,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // Mlx5Connection_queue_single_buffer_with_copy
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("conn", "&mut Mlx5Connection")),
        FunctionArg::CArg(CArgInfo::arg("msg_id", "u32")),
        FunctionArg::CArg(CArgInfo::arg("conn_id", "usize")),
        FunctionArg::CArg(CArgInfo::arg("buffer", "&[u8]")),
        FunctionArg::CArg(CArgInfo::arg("end_batch", "bool")),
    ];
    compiler.inner.add_context(Context::Function(FunctionContext::new(
        "Mlx5Connection_queue_single_buffer_with_copy_inner",
        false, args, "Result<()>",
    )))?;
    compiler.inner.add_line("conn.queue_single_buffer_with_copy((msg_id, conn_id, buffer), end_batch)")?;
    compiler.inner.pop_context()?; // end of function
    compiler.inner.add_newline()?;
    compiler.add_extern_c_function_standalone(
        "Mlx5Connection_queue_single_buffer_with_copy",
        "Mlx5Connection_queue_single_buffer_with_copy_inner",
        vec![
            ("conn", ArgType::new_ref_mut("Mlx5Connection")),
            ("msg_id", ArgType::Primitive("u32".to_string())),
            ("conn_id", ArgType::Primitive("usize".to_string())),
            ("buffer", ArgType::Buffer),
            ("end_batch", ArgType::Primitive("bool".to_string())),
        ],
        None,
        true,
    )?;
    Ok(())
}
