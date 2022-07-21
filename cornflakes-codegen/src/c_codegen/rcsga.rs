use super::{
    super::header_utils::{MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
    },
    common::*,
};
use color_eyre::eyre::Result;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    let datapath = "Mlx5Connection";  // hardcoded datapath
    add_rcsga_dependencies(fd, compiler)?;
    compiler.add_newline()?;
    add_bumpalo_functions(compiler)?;
    compiler.add_newline()?;
    add_arena_allocate(compiler, datapath)?;
    compiler.add_newline()?;
    add_cornflakes_structs(fd, compiler, Some(datapath))?;

    // For each message type: add basic constructors, getters and setters, and
    // header trait functions.
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        add_default_impl(compiler, &msg_info, Some(datapath))?;
        compiler.add_newline()?;
        add_impl(fd, compiler, &msg_info, Some(datapath))?;
        compiler.add_newline()?;
        add_rcsga_header_repr(compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        add_shared_rcsga_header_repr(compiler, &msg_info, datapath)?;
    }
    Ok(())
}

fn add_rcsga_dependencies(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("bumpalo")?;
    compiler.add_dependency("cornflakes_libos::{ArenaOrderedSga, ArenaOrderedRcSga}")?;
    compiler.add_dependency("cornflakes_libos::dynamic_rcsga_hdr::*")?;
    compiler.add_dependency("mlx5_datapath::datapath::connection::Mlx5Connection")?;

    // For VariableList_<param_ty>_index
    for message in fd.get_repr().messages.iter() {
        let msg_info = MessageInfo(message.clone());
        if !has_variable_list(fd, &msg_info, None)?.is_empty() {
            compiler.add_dependency("std::ops::Index")?;
            break;
        }
    }
    Ok(())
}

fn add_bumpalo_functions(compiler: &mut SerializationCompiler) -> Result<()> {
    // add bump intialization function (manually-generated)
    add_bump_initialization_function(compiler)?;

    // add arena reset function
    add_extern_c_wrapper_function(
        compiler,
        "Bump_reset",
        "bumpalo::Bump",
        "reset",
        Some(true),
        vec![],
        None,
        false,
    )?;
    Ok(())
}

/// cornflakes-libos/src/state_machine/server.rs
fn add_bump_initialization_function(
    compiler: &mut SerializationCompiler,
) -> Result<()> {
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("batch_size", "usize")),
        FunctionArg::CArg(CArgInfo::arg("max_packet_size", "usize")),
        FunctionArg::CArg(CArgInfo::arg("max_entries", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg("*mut ::std::os::raw::c_void")),
    ];
    let func_context = FunctionContext::new_extern_c(
        "Bump_with_capacity", true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_func_call_with_let(
        "capacity",
        None,
        None,
        "ArenaOrderedSga::arena_size",
        vec![
            "batch_size".to_string(),
            "max_packet_size".to_string(),
            "max_entries".to_string(),
        ],
        false,
    )?;
    compiler.add_func_call_with_let(
        "arena",
        None,
        None,
        "bumpalo::Bump::with_capacity",
        vec!["capacity * 100".to_string()],
        false,
    )?;
    compiler.add_def_with_let(false, None, "arena",
        "Box::into_raw(Box::new(arena))")?;
    compiler.add_unsafe_set("return_ptr", "arena as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

/// cornflakes-libos/src/lib.rs
fn add_arena_allocate(
    compiler: &mut SerializationCompiler,
    datapath: &str,
) -> Result<()> {
    // add allocate function
    add_extern_c_wrapper_function(
        compiler,
        "ArenaOrderedRcSga_allocate",
        &format!("ArenaOrderedRcSga::<{}>", datapath),
        "allocate",
        None,
        vec![
            ("num_entries", ArgType::Rust { string: "usize".to_string() }),
            ("arena", ArgType::Ref { inner_ty: "bumpalo::Bump".to_string() }),
        ],
        Some(ArgType::VoidPtr {
            inner_ty: format!("ArenaOrderedRcSga<{}>", datapath),
        }),
        false,
    )?;
    Ok(())
}

fn add_rcsga_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    // add num scatter_gather_entries function
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_num_scatter_gather_entries", msg_info.get_name()),
        &format!("{}<{}>", msg_info.get_name(), datapath),
        "num_scatter_gather_entries",
        Some(false),
        vec![],
        Some(ArgType::Rust { string: "usize".to_string() }),
        false,
    )?;
    Ok(())
}

// These aren't generated functions so we generate the wrappers manually.
// See: cornflakes-codegen/src/utils/dynamic_rcsga_hdr.rs
fn add_shared_rcsga_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let struct_name = format!("{}<{}>", &msg_info.get_name(), datapath);

    // add deserialize_from_buf function
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_deserialize_from_buf", &msg_info.get_name()),
        &struct_name,
        "deserialize_from_buf",
        Some(true),
        vec![("buffer", ArgType::Buffer)],
        None,
        true,
    )?;

    // add serialize_into_arena_sga function
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_serialize_into_arena_sga", &msg_info.get_name()),
        &struct_name,
        "serialize_into_arena_sga",
        Some(false),
        vec![
            ("ordered_sga", ArgType::RefMut {
                inner_ty: format!("ArenaOrderedRcSga<{}>", datapath),
            }),
            ("arena", ArgType::Ref {
                inner_ty: "bumpalo::Bump".to_string(),
            }),
            ("datapath", ArgType::Ref { inner_ty: datapath.to_string() }),
            ("with_copy", ArgType::Rust { string: "bool".to_string() }),
        ],
        None,
        true,
    )?;
    Ok(())
}
