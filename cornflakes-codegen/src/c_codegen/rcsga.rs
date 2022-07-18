use super::{
    super::header_utils::{MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
        MatchContext,
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
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("num_entries", "usize")),
        FunctionArg::CArg(CArgInfo::arg("arena", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("*mut ::std::os::raw::c_void")),
    ];
    let func_context = FunctionContext::new_extern_c(
        "ArenaOrderedRcSga_allocate", true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_def_with_let(false, None, "arg0", "num_entries")?;
    compiler.add_unsafe_def_with_let(false, None, "arg1",
        "Box::from_raw(arena as *mut bumpalo::Bump)")?;
    compiler.add_func_call_with_let(
        "value",
        Some(format!("ArenaOrderedRcSga<{}>", datapath)),
        None,
        "ArenaOrderedRcSga::allocate",
        vec![
            "arg0".to_string(),
            "&arg1".to_string(),
        ],
        false,
    )?;
    compiler.add_def_with_let(false, None, "value",
        "Box::into_raw(Box::new(value))")?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["arg1".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
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
    add_deserialize_from_buf_function(compiler, msg_info, &struct_name)?;
    add_serialize_into_arena_sga_function(compiler, msg_info, &struct_name, datapath)?;
    Ok(())
}

fn add_deserialize_from_buf_function(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    struct_name: &str,
) -> Result<()> {
    let args = {
        let mut args = vec![];
        args.push(FunctionArg::CSelfArg);
        args.push(FunctionArg::CArg(CArgInfo::arg("buffer", "*const ::std::os::raw::c_uchar")));
        args.push(FunctionArg::CArg(CArgInfo::len_arg("buffer")));
        args
    };

    let func_context = FunctionContext::new_extern_c(
        &format!("{}_{}", &msg_info.get_name(), "deserialize_from_buf"),
        true, args, true,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(true, None, "self_", &format!(
        "Box::from_raw(self_ as *mut {})", struct_name))?;
    compiler.add_unsafe_def_with_let(false, None, "arg0", "std::slice::from_raw_parts(buffer, buffer_len)")?;
    compiler.add_func_call_with_let("value", None, Some("self_".to_string()),
        "deserialize_from_buf", vec!["arg0".to_string()], false)?;
    compiler.add_func_call(None, "Box::into_raw", vec!["self_".to_string()], false)?;

    let match_context = MatchContext::new_with_def("value",
        vec!["Ok (value)".to_string(), "Err(_)".to_string()], "value");
    compiler.add_context(Context::Match(match_context))?;
    compiler.add_return_val("value", false)?;
    compiler.pop_context()?;
    compiler.add_return_val("1", true)?;
    compiler.pop_context()?;
    compiler.add_line("0")?;

    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

fn add_serialize_into_arena_sga_function(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    struct_name: &str,
    datapath: &str,
) -> Result<()> {
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_serialize_into_arena_sga", &msg_info.get_name()),
        struct_name,
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
