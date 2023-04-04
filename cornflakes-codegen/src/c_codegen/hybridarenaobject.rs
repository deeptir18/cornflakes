use super::{
    super::header_utils::{MessageInfo, ProtoReprInfo},
    super::rust_codegen::{CArgInfo, Context, FunctionArg, FunctionContext, SerializationCompiler},
    common::{self, add_extern_c_wrapper_function, ArgType, SelfArgType},
    hybridrcsga::{
        add_db_load_functions, add_received_pkt_functions, has_cf_bytes, has_cf_string,
        has_variable_list,
    },
};
use color_eyre::eyre::Result;
use std::collections::HashSet;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    let datapath = "Mlx5Connection"; // hardcoded datapath
    add_hybrid_arena_object_dependencies(fd, compiler)?;
    compiler.add_newline()?;
    add_bumpalo_functions(compiler)?;
    compiler.add_newline()?;
    add_cornflakes_structs(fd, compiler, datapath)?;
    compiler.add_newline()?;

    add_received_pkt_functions(compiler, datapath)?;
    compiler.add_newline()?;
    add_db_load_functions(compiler, datapath)?;

    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        add_new_in(compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        common::add_impl(fd, compiler, &msg_info, Some(datapath), true)?;
        add_dynamic_rcsga_hybrid_header_repr(compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        add_queue_cornflakes_obj_function(compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        add_free(compiler, &msg_info, datapath)?;
    }

    Ok(())
}

fn add_hybrid_arena_object_dependencies(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
) -> Result<()> {
    compiler.add_dependency("bumpalo")?;
    compiler.add_dependency("cornflakes_libos::datapath::{Datapath, ReceivedPkt}")?;
    compiler.add_dependency("cornflakes_libos::dynamic_object_arena_hdr::*")?;
    compiler.add_dependency("cornflakes_libos::ArenaOrderedSga")?;
    compiler.add_dependency("cf_kv::{MsgType, ServerLoadGenerator}")?;
    compiler.add_dependency("mlx5_datapath::datapath::connection::Mlx5Connection")?;
    compiler.add_dependency("std::str::FromStr")?;

    // For VariableList_<param_ty>_index
    for message in fd.get_repr().messages.iter() {
        let msg_info = MessageInfo(message.clone());
        if !has_variable_list(fd, &msg_info, "")?.is_empty() {
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
        Some(SelfArgType::Mut),
        vec![],
        None,
        false,
    )?;
    Ok(())
}

/// cornflakes-libos/src/state_machine/server.rs
fn add_bump_initialization_function(compiler: &mut SerializationCompiler) -> Result<()> {
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("batch_size", "usize")),
        FunctionArg::CArg(CArgInfo::arg("max_packet_size", "usize")),
        FunctionArg::CArg(CArgInfo::arg("max_entries", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg("*mut ::std::os::raw::c_void")),
    ];
    let func_context = FunctionContext::new_extern_c("Bump_with_capacity", true, args, false);
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
        vec!["capacity".to_string()],
        false,
    )?;
    compiler.add_def_with_let(false, None, "arena", "Box::into_raw(Box::new(arena))")?;
    compiler.add_unsafe_set("return_ptr", "arena as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

/// Determine whether we need to add wrapper functions for CFString, CFBytes,
/// or VariableList<T> parameterized by some type T. Then add them.
fn add_cornflakes_structs(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    datapath: &str,
) -> Result<()> {
    let mut added_cf_string = false;
    let mut added_cf_bytes = false;
    let mut added_variable_lists = HashSet::new();
    for message in fd.get_repr().messages.iter() {
        let msg_info = MessageInfo(message.clone());
        if !added_cf_string && has_cf_string(&msg_info) {
            add_cf_string_or_bytes(compiler, datapath, "CFString")?;
            added_cf_string = true;
        }
        if !added_cf_bytes && has_cf_bytes(&msg_info) {
            add_cf_string_or_bytes(compiler, datapath, "CFBytes")?;
            added_cf_bytes = true;
        }
        for param_ty in has_variable_list(fd, &msg_info, datapath)? {
            if added_variable_lists.insert(param_ty.clone()) {
                add_variable_list(compiler, param_ty, datapath)?;
            }
        }
    }
    Ok(())
}
/// ty == "CFString" or "CFBytes"
fn add_cf_string_or_bytes(
    compiler: &mut SerializationCompiler,
    datapath: &str,
    ty: &str,
) -> Result<()> {
    let struct_ty = format!("{}<{}>", ty, datapath);
    let struct_name = format!("{}::<{}>", ty, datapath);

    ////////////////////////////////////////////////////////////////////////////
    // <ty>_new_in
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_new_in", ty),
        &struct_name,
        "new_in",
        None,
        vec![(
            "arena",
            ArgType::Ref {
                inner_ty: "bumpalo::Bump".to_string(),
            },
        )],
        Some(ArgType::VoidPtr {
            inner_ty: struct_ty.clone(),
        }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // <ty>_new
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_new", ty),
        &struct_name,
        "new",
        None,
        vec![
            ("ptr", ArgType::Buffer),
            (
                "datapath",
                ArgType::RefMut {
                    inner_ty: datapath.to_string(),
                },
            ),
            (
                "arena",
                ArgType::Ref {
                    inner_ty: "bumpalo::Bump".to_string(),
                },
            ),
        ],
        Some(ArgType::VoidPtr {
            inner_ty: struct_ty.clone(),
        }),
        true,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // <ty>_unpack
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("self_", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_uchar")),
        FunctionArg::CArg(CArgInfo::ret_len_arg()),
    ];
    let func_context = FunctionContext::new_extern_c(&format!("{}_unpack", ty), true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw = format!("Box::from_raw(self_ as *mut {}<{}>)", ty, datapath);
    compiler.add_unsafe_def_with_let(false, None, "self_", &box_from_raw)?;
    compiler.add_def_with_let(false, None, "ptr", "(*self_).as_ref()")?;
    compiler.add_unsafe_set("return_ptr", "ptr.as_ptr()")?;
    compiler.add_unsafe_set("return_len_ptr", "self_.len()")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["self_".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

/// Note that the value passed around as a VariableList is actually a reference
/// &VariableList, so parsing the self_ argument would be a
/// *mut *const VariableList instead of just a *mut VariableList. We
/// additionally need to deref the Box<*const VariableList> as a
/// Box<&VariableList>. But careful using VariableList_init which returns an
/// actual VariableList... I think the C code could just pass a reference.
fn add_variable_list(
    compiler: &mut SerializationCompiler,
    param_ty: ArgType,
    datapath: &str,
) -> Result<()> {
    let struct_name = match param_ty {
        ArgType::Rust { ref string } => format!("VariableList_{}", string),
        ArgType::Bytes { .. } => "VariableList_CFBytes".to_string(),
        ArgType::String { .. } => "VariableList_CFString".to_string(),
        _ => unimplemented!("unimplemented VariableList type"),
    };
    let struct_ty = ArgType::List {
        datapath: Some(datapath.to_string()),
        param_ty: Box::new(param_ty.clone()),
    }
    .to_cf_string();

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_init
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_init", &struct_name),
        &format!("VariableList::{}", &struct_ty["VariableList".len()..]),
        "init",
        None,
        vec![
            (
                "num",
                ArgType::Rust {
                    string: "usize".to_string(),
                },
            ),
            (
                "arena",
                ArgType::Ref {
                    inner_ty: "bumpalo::Bump".to_string(),
                },
            ),
        ],
        Some(ArgType::VoidPtr {
            inner_ty: struct_ty.clone(),
        }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_append
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_append", &struct_name),
        &struct_ty,
        "append",
        Some(SelfArgType::Mut),
        vec![(
            "val",
            ArgType::VoidPtr {
                inner_ty: param_ty.to_cf_string(),
            },
        )],
        None,
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_len
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_len", &struct_name),
        &struct_ty,
        "len",
        Some(SelfArgType::Value),
        vec![],
        Some(ArgType::Rust {
            string: "usize".to_string(),
        }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_index
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_index", &struct_name),
        &struct_ty,
        "index",
        Some(SelfArgType::Value),
        vec![(
            "idx",
            ArgType::Rust {
                string: "usize".to_string(),
            },
        )],
        Some(ArgType::Ref {
            inner_ty: param_ty.to_cf_string(),
        }),
        false,
    )?;

    Ok(())
}

fn add_new_in(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let lifetimes = msg_info.get_function_params_hybrid(true)?.join(",");
    let type_annotations = msg_info
        .get_type_params_hybrid_object_ffi(true, datapath)?
        .join(",");
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_new_in<{}>", msg_info.get_name(), lifetimes,),
        &format!("{}::<{}>", &msg_info.get_name(), type_annotations,),
        "new_in",
        None,
        vec![(
            "arena",
            ArgType::Ref {
                inner_ty: "bumpalo::Bump".to_string(),
            },
        )],
        Some(ArgType::VoidPtr {
            inner_ty: msg_info.get_name(),
        }),
        false,
    )?;
    Ok(())
}

fn add_dynamic_rcsga_hybrid_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let lifetimes = msg_info.get_function_params_hybrid(true)?.join(",");
    let type_annotations = msg_info
        .get_type_params_hybrid_object_ffi(true, datapath)?
        .join(",");
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_deserialize<{}>", msg_info.get_name(), lifetimes),
        &format!("{}::<{}>", msg_info.get_name(), type_annotations),
        "deserialize",
        Some(SelfArgType::Mut),
        vec![
            (
                "pkt",
                ArgType::Ref {
                    inner_ty: format!("ReceivedPkt<{}>", datapath),
                },
            ),
            (
                "offset",
                ArgType::Rust {
                    string: "usize".to_string(),
                },
            ),
            (
                "arena",
                ArgType::Ref {
                    inner_ty: "bumpalo::Bump".to_string(),
                },
            ),
        ],
        None,
        true,
    )?;
    Ok(())
}

fn add_queue_cornflakes_obj_function(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let mut lifetime_annotations = msg_info.get_function_params_hybrid(true)?;
    let type_annotations = msg_info
        .get_type_params_hybrid_object_ffi(true, datapath)?
        .join(",");
    if !(lifetime_annotations.contains(&"'arena".to_string())) {
        // arena needed for copy context
        lifetime_annotations.push("'arena".to_string());
    }
    let lifetimes = lifetime_annotations.join(",");
    add_extern_c_wrapper_function(
        compiler,
        &format!(
            "{}_{}_queue_cornflakes_arena_object<{}>",
            datapath,
            msg_info.get_name(),
            lifetimes,
        ),
        datapath,
        "queue_cornflakes_arena_object",
        Some(SelfArgType::Mut),
        vec![
            (
                "msg_id",
                ArgType::Rust {
                    string: "u32".to_string(),
                },
            ),
            (
                "conn_id",
                ArgType::Rust {
                    string: "usize".to_string(),
                },
            ),
            (
                "cornflakes_obj",
                ArgType::VoidPtr {
                    inner_ty: format!("{}<{}>", msg_info.get_name(), type_annotations),
                },
            ),
            (
                "end_batch",
                ArgType::Rust {
                    string: "bool".to_string(),
                },
            ),
        ],
        None,
        true,
    )?;
    Ok(())
}

fn add_free(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let lifetimes = msg_info.get_function_params_hybrid(true)?.join(",");
    let type_annotations = msg_info
        .get_type_params_hybrid_object_ffi(true, datapath)?
        .join(",");
    common::add_free_function(
        compiler,
        &format!("{}", msg_info.get_name()),
        &format!("<{}>", type_annotations),
        &lifetimes,
    )?;
    Ok(())
}
