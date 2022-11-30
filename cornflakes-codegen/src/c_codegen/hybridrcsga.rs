use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    super::rust_codegen::{CArgInfo, Context, FunctionArg, FunctionContext, SerializationCompiler},
    common::{self, add_extern_c_wrapper_function, ArgType, SelfArgType},
};
use color_eyre::eyre::Result;
use protobuf_parser::FieldType;
use std::collections::HashSet;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    let datapath = "Mlx5Connection"; // hardcoded datapath
    add_hybrid_rcsga_dependencies(fd, compiler)?;
    compiler.add_newline()?;
    add_bumpalo_functions(compiler)?;
    compiler.add_newline()?;
    add_arena_allocate(compiler, datapath)?;
    compiler.add_newline()?;
    add_cornflakes_structs(fd, compiler, datapath)?;
    compiler.add_newline()?;
    add_copy_context_functions(compiler, datapath)?;
    compiler.add_newline()?;
    // TODO: these seem application specific. is there a better place to put them?
    add_received_pkt_functions(compiler, datapath)?;

    add_db_load_functions(compiler, datapath)?;

    // For each message type: add basic constructors, getters and setters, and
    // header trait functions.
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        compiler.add_newline()?;
        add_new_in(fd, compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        common::add_impl(fd, compiler, &msg_info, Some(datapath), true)?;
        compiler.add_newline()?;
        add_dynamic_rcsga_hybrid_header_repr(fd, compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        add_queue_cornflakes_obj_function(fd, compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        add_free(fd, compiler, &msg_info, datapath)?;
        // compiler.add_newline()?;
        // add_rcsga_header_repr(compiler, &msg_info, datapath)?;
        // compiler.add_newline()?;
        // add_shared_rcsga_header_repr(compiler, &msg_info, datapath)?;
    }

    Ok(())
}

fn add_hybrid_rcsga_dependencies(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
) -> Result<()> {
    compiler.add_dependency("bumpalo")?;
    compiler.add_dependency("cornflakes_libos::datapath::{Datapath, ReceivedPkt}")?;
    compiler
        .add_dependency("cornflakes_libos::{ArenaOrderedSga, ArenaOrderedRcSga, CopyContext}")?;
    compiler.add_dependency("cornflakes_libos::dynamic_rcsga_hybrid_hdr::*")?;
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
        vec!["capacity * 100".to_string()],
        false,
    )?;
    compiler.add_def_with_let(false, None, "arena", "Box::into_raw(Box::new(arena))")?;
    compiler.add_unsafe_set("return_ptr", "arena as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

/// cornflakes-libos/src/lib.rs
fn add_arena_allocate(compiler: &mut SerializationCompiler, datapath: &str) -> Result<()> {
    // add allocate function
    add_extern_c_wrapper_function(
        compiler,
        "ArenaOrderedRcSga_allocate",
        &format!("ArenaOrderedRcSga::<{}>", datapath),
        "allocate",
        None,
        vec![
            (
                "num_entries",
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
            inner_ty: format!("ArenaOrderedRcSga<{}>", datapath),
        }),
        false,
    )?;
    Ok(())
}

fn add_new_in(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let lifetimes = msg_info.get_function_params(fd)?.join(",");
    let type_annotations = msg_info
        .get_type_params_with_lifetime_ffi(true, fd, datapath)?
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
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let lifetimes = msg_info.get_function_params(fd)?.join(",");
    let type_annotations = msg_info
        .get_type_params_with_lifetime_ffi(true, fd, datapath)?
        .join(",");

    ////////////////////////////////////////////////////////////////////////////
    // MsgInfoName_deserialize
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

    ////////////////////////////////////////////////////////////////////////////
    // MsgInfoName_reset_bitmap
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_reset_bitmap<{}>", msg_info.get_name(), lifetimes),
        &format!("{}::<{}>", msg_info.get_name(), type_annotations),
        "reset_bitmap",
        Some(SelfArgType::Mut),
        vec![],
        None,
        false,
    )?;
    Ok(())
}

fn add_queue_cornflakes_obj_function(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let mut lifetime_annotations = msg_info.get_function_params(fd)?;
    let type_annotations = msg_info
        .get_type_params_with_lifetime_ffi(true, fd, datapath)?
        .join(",");
    if !(lifetime_annotations.contains(&"'arena".to_string())) {
        // arena needed for copy context
        lifetime_annotations.push("'arena".to_string());
    }
    let lifetimes = lifetime_annotations.join(",");
    add_extern_c_wrapper_function(
        compiler,
        &format!(
            "{}_{}_queue_cornflakes_obj<{}>",
            datapath,
            msg_info.get_name(),
            lifetimes,
        ),
        datapath,
        "queue_cornflakes_obj",
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
                "copy_context",
                ArgType::RefMut {
                    inner_ty: format!("CopyContext<'arena, {}>", datapath),
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
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    let lifetimes = msg_info.get_function_params(fd)?.join(",");
    let type_annotations = msg_info
        .get_type_params_with_lifetime_ffi(true, fd, datapath)?
        .join(",");
    common::add_free_function(
        compiler,
        &format!("{}", msg_info.get_name()),
        &format!("<{}>", type_annotations),
        &lifetimes,
    )?;
    Ok(())
}
// fn add_rcsga_header_repr(
//     compiler: &mut SerializationCompiler,
//     msg_info: &MessageInfo,
//     datapath: &str,
// ) -> Result<()> {
//     // add num scatter_gather_entries function
//     add_extern_c_wrapper_function(
//         compiler,
//         &format!("{}_num_scatter_gather_entries", msg_info.get_name()),
//         &format!("{}<{}>", msg_info.get_name(), datapath),
//         "num_scatter_gather_entries",
//         Some(SelfArgType::Value),
//         vec![],
//         Some(ArgType::Rust { string: "usize".to_string() }),
//         false,
//     )?;
//     Ok(())
// }

// // These aren't generated functions so we generate the wrappers manually.
// // See: cornflakes-codegen/src/utils/dynamic_rcsga_hdr.rs
// fn add_shared_rcsga_header_repr(
//     compiler: &mut SerializationCompiler,
//     msg_info: &MessageInfo,
//     datapath: &str,
// ) -> Result<()> {
//     let struct_name = format!("{}<{}>", &msg_info.get_name(), datapath);

//     // add deserialize_from_buf function
//     add_extern_c_wrapper_function(
//         compiler,
//         &format!("{}_deserialize_from_buf", &msg_info.get_name()),
//         &struct_name,
//         "deserialize_from_buf",
//         Some(SelfArgType::Mut),
//         vec![("buffer", ArgType::Buffer)],
//         None,
//         true,
//     )?;

//     // add serialize_into_arena_sga function
//     add_extern_c_wrapper_function(
//         compiler,
//         &format!("{}_serialize_into_arena_sga", &msg_info.get_name()),
//         &struct_name,
//         "serialize_into_arena_sga",
//         Some(SelfArgType::Value),
//         vec![
//             ("ordered_sga", ArgType::RefMut {
//                 inner_ty: format!("ArenaOrderedRcSga<{}>", datapath),
//             }),
//             ("arena", ArgType::Ref {
//                 inner_ty: "bumpalo::Bump".to_string(),
//             }),
//             ("datapath", ArgType::Ref { inner_ty: datapath.to_string() }),
//             ("with_copy", ArgType::Rust { string: "bool".to_string() }),
//         ],
//         None,
//         true,
//     )?;
//     Ok(())
// }

fn add_copy_context_functions(compiler: &mut SerializationCompiler, datapath: &str) -> Result<()> {
    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_new
    add_extern_c_wrapper_function(
        compiler,
        "CopyContext_new",
        "CopyContext",
        "new",
        None,
        vec![
            (
                "arena",
                ArgType::Ref {
                    inner_ty: "bumpalo::Bump".to_string(),
                },
            ),
            (
                "datapath",
                ArgType::RefMut {
                    inner_ty: datapath.to_string(),
                },
            ),
        ],
        Some(ArgType::VoidPtr {
            inner_ty: "CopyContext".to_string(),
        }),
        true,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_data_len
    add_extern_c_wrapper_function(
        compiler,
        "CopyContext_data_len",
        &format!("CopyContext<{}>", datapath),
        "data_len",
        Some(SelfArgType::Value),
        vec![],
        Some(ArgType::Rust { string: "usize".to_string() }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_reset
    add_extern_c_wrapper_function(
        compiler,
        "CopyContext_reset",
        &format!("CopyContext<{}>", datapath),
        "reset",
        Some(SelfArgType::Mut),
        vec![],
        None,
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_free
    common::add_free_function(compiler, "CopyContext", &format!("<{}>", datapath), "")?;

    Ok(())
}

fn add_db_load_functions(compiler: &mut SerializationCompiler, datapath: &str) -> Result<()> {
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("conn", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("trace_file", "*const ::std::os::raw::c_char")),
        FunctionArg::CArg(CArgInfo::arg("db_ptr", "*mut *mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg(
            "list_db_ptr",
            "*mut *mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("num_keys", "usize")),
        FunctionArg::CArg(CArgInfo::arg("num_values", "usize")),
        FunctionArg::CArg(CArgInfo::arg("value_size", "*const ::std::os::raw::c_char")),
    ];
    let func_context =
        FunctionContext::new_extern_c(&format!("{}_load_ycsb_db", datapath), true, args, true);
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(
        true,
        None,
        "conn_box",
        &format!("Box::from_raw(conn as *mut {})", datapath),
    )?;
    // initialize ycsb load generator
    compiler.add_block("
    let file_str = unsafe { std::ffi::CStr::from_ptr(trace_file).to_str().unwrap() };
    let value_size_str = unsafe {std::ffi::CStr::from_ptr(value_size).to_str().unwrap()};
    let value_size_generator = cf_kv::ycsb::YCSBValueSizeGenerator::from_str(value_size_str).unwrap();
    let load_generator = cf_kv::ycsb::YCSBServerLoader::new(value_size_generator, num_values, num_keys, false);
    let (kv, list_kv, _, _) = load_generator.new_kv_state(file_str, conn_box.as_mut(), false).unwrap();
    let boxed_kv = Box::new(kv);
    let boxed_list_kv = Box::new(list_kv);
    unsafe {
        *db_ptr = Box::into_raw(boxed_kv) as _;
        *list_db_ptr = Box::into_raw(boxed_list_kv) as _;
    }
        ")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["conn_box".to_string()], false)?;
    compiler.add_return_val("0", false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    let db_iterator_args = vec![
        FunctionArg::CArg(CArgInfo::arg("db", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg(
            "db_keys_vec",
            "*mut *mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("db_keys_len", "*mut usize")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_get_db_keys_vec", datapath),
        true,
        db_iterator_args,
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "db_box",
        &format!("Box::from_raw(db as *mut cf_kv::KVServer<{}>)", datapath),
    )?;

    compiler.add_block(
        "
    unsafe {
        let keys = db_box.keys();
        *db_keys_len = keys.len();
        let boxed_keys = Box::new(keys);
        *db_keys_vec = Box::into_raw(boxed_keys) as _;
    }
        ",
    )?;

    compiler.add_func_call(None, "Box::into_raw", vec!["db_box".to_string()], false)?;

    compiler.pop_context()?;

    let list_db_iterator_args = vec![
        FunctionArg::CArg(CArgInfo::arg("list_db", "*mut *mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg(
            "list_db_keys_vec",
            "*mut *mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("list_db_keys_len", "*mut usize")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_get_list_db_keys_vec", datapath),
        true,
        list_db_iterator_args,
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "list_db_box",
        &format!(
            "Box::from_raw(list_db as *mut cf_kv::ListKVServer<{}>)",
            datapath
        ),
    )?;

    compiler.add_block(
        "
    unsafe {
        let keys = list_db_box.keys();
        *list_db_keys_len = keys.len();
        let boxed_keys = Box::new(keys);
        *list_db_keys_vec = Box::into_raw(boxed_keys) as _;
    }
        ",
    )?;
    compiler.add_func_call(
        None,
        "Box::into_raw",
        vec!["list_db_box".to_string()],
        false,
    )?;

    compiler.pop_context()?;

    let db_get_args = vec![
        FunctionArg::CArg(CArgInfo::arg("db", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("db_keys_vec", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("key_idx", "usize")),
        FunctionArg::CArg(CArgInfo::arg("key_ptr", "*mut *mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("key_len", "*mut usize")),
        FunctionArg::CArg(CArgInfo::arg(
            "value_ptr",
            "*mut *mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("value_len", "*mut usize")),
    ];

    let func_context = FunctionContext::new_extern_c(
        &format!("{}_get_db_value_at", datapath),
        true,
        db_get_args,
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "db_box",
        &format!("Box::from_raw(db as *mut cf_kv::KVServer<{}>)", datapath),
    )?;

    compiler.add_unsafe_def_with_let(
        false,
        None,
        "db_keys_vec_box",
        "Box::from_raw(db_keys_vec as *mut Vec<String>)",
    )?;

    compiler.add_block(
        "
    unsafe {
        let key = db_keys_vec_box.get(key_idx).unwrap();
        let value = db_box.get(&key).unwrap();
        *value_len = value.as_ref().len();
        *value_ptr = value.as_ref().as_ptr() as _;
        *key_len = key.len();
        *key_ptr = key.as_str().as_ptr() as _;
    }
        ",
    )?;

    compiler.add_func_call(
        None,
        "Box::into_raw",
        vec!["db_keys_vec_box".to_string()],
        false,
    )?;
    compiler.add_func_call(None, "Box::into_raw", vec!["db_box".to_string()], false)?;

    compiler.pop_context()?;

    let list_db_get_list_size_args = vec![
        FunctionArg::CArg(CArgInfo::arg("list_db", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg(
            "list_db_keys_vec",
            "*mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("key_idx", "usize")),
        FunctionArg::CArg(CArgInfo::arg("key_ptr", "*mut *mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("key_len", "*mut usize")),
        FunctionArg::CArg(CArgInfo::arg("list_size", "*mut usize")),
    ];

    let func_context = FunctionContext::new_extern_c(
        &format!("{}_list_db_get_list_size", datapath),
        true,
        list_db_get_list_size_args,
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "list_db_box",
        &format!(
            "Box::from_raw(list_db as *mut cf_kv::ListKVServer<{}>)",
            datapath
        ),
    )?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "list_db_keys_vec_box",
        "Box::from_raw(list_db_keys_vec as *mut Vec<String>)",
    )?;

    compiler.add_block(
        "unsafe {
        let key = list_db_keys_vec_box.get(key_idx).unwrap();
        let value_list = list_db_box.get(&key).unwrap();
        *list_size = value_list.len();
        *key_len = key.len();
        *key_ptr = key.as_str().as_ptr() as _;
    }",
    )?;
    compiler.add_func_call(
        None,
        "Box::into_raw",
        vec!["list_db_keys_vec_box".to_string()],
        false,
    )?;
    compiler.add_func_call(
        None,
        "Box::into_raw",
        vec!["list_db_box".to_string()],
        false,
    )?;

    compiler.pop_context()?;
    let list_db_get_value_at_idx_args = vec![
        FunctionArg::CArg(CArgInfo::arg("list_db", "*mut ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg(
            "list_db_keys_vec",
            "*mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("key_idx", "usize")),
        FunctionArg::CArg(CArgInfo::arg("list_idx", "usize")),
        FunctionArg::CArg(CArgInfo::arg(
            "value_ptr",
            "*mut *mut ::std::os::raw::c_void",
        )),
        FunctionArg::CArg(CArgInfo::arg("value_len", "*mut usize")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_list_db_get_value_at_idx", datapath),
        true,
        list_db_get_value_at_idx_args,
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "list_db_box",
        &format!(
            "Box::from_raw(list_db as *mut cf_kv::ListKVServer<{}>)",
            datapath
        ),
    )?;

    compiler.add_unsafe_def_with_let(
        false,
        None,
        "list_db_keys_vec_box",
        "Box::from_raw(list_db_keys_vec as *mut Vec<String>)",
    )?;

    compiler.add_block(
        "
    unsafe {
        let key = list_db_keys_vec_box.get(key_idx).unwrap();
        let value_list = list_db_box.get(&key).unwrap();
        let value = &value_list[list_idx];
        *value_len = value.as_ref().len();
        *value_ptr = value.as_ref().as_ptr() as _;
    }
        ",
    )?;

    compiler.add_func_call(
        None,
        "Box::into_raw",
        vec!["list_db_keys_vec_box".to_string()],
        false,
    )?;
    compiler.add_func_call(
        None,
        "Box::into_raw",
        vec!["list_db_box".to_string()],
        false,
    )?;

    compiler.pop_context()?;

    Ok(())
}

fn add_received_pkt_functions(compiler: &mut SerializationCompiler, datapath: &str) -> Result<()> {
    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_msg_type
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("pkt", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("size_ptr", "*mut u16")),
        FunctionArg::CArg(CArgInfo::ret_arg("u16")),
    ];
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_msg_type", true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw = format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
    compiler.add_unsafe_def_with_let(false, None, "pkt", &box_from_raw)?;
    compiler.add_def_with_let(
        false,
        None,
        "msg_type",
        "MsgType::from_packet(&pkt).unwrap()",
    )?;
    compiler.add_block(
        "match msg_type {
        MsgType::Get => unsafe {
            *return_ptr = 0;
            *size_ptr = 1;
        },
        MsgType::Put => {
            unimplemented!();
        }
        MsgType::GetM(s) => {
        unsafe {
            *return_ptr = 2;
            *size_ptr = s;
            }
        }
        MsgType::PutM(_s) => {
            unimplemented!();
        }
        MsgType::GetList(s) => {
        unsafe {
            *return_ptr = 4;
            *size_ptr = s;
            }
        }
        MsgType::PutList(_s) => {
            unimplemented!();
        }
        MsgType::AppendToList(_s) => {
            unimplemented!();
        }
        MsgType::AddUser => {
            unimplemented!();
        }
        MsgType::FollowUnfollow => {
            unimplemented!();
        }
        MsgType::PostTweet => {
            unimplemented!();
        }
        MsgType::GetTimeline(_s) => {
            unimplemented!();
        }
    }",
    )?;
    compiler.add_func_call(None, "Box::into_raw", vec!["pkt".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_size
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("pkt", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("u16")),
    ];
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_size", true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw = format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
    compiler.add_unsafe_def_with_let(false, None, "pkt", &box_from_raw)?;
    compiler.add_def_with_let(false, None, "seg", "pkt.seg(0).as_ref()")?;
    compiler.add_def_with_let(
        false,
        None,
        "size",
        "(seg[3] as u16) | ((seg[2] as u16) << 8)",
    )?;
    compiler.add_unsafe_set("return_ptr", "size")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["pkt".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_data
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("pkt", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_uchar")),
    ];
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_data", true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw = format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
    compiler.add_unsafe_def_with_let(false, None, "pkt", &box_from_raw)?;
    compiler.add_def_with_let(false, None, "value", "pkt.seg(0).as_ref().as_ptr()")?;
    compiler.add_unsafe_set("return_ptr", "value")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["pkt".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_data_len
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("pkt", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("usize")),
    ];
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_data_len", true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw = format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
    compiler.add_unsafe_def_with_let(false, None, "pkt", &box_from_raw)?;
    compiler.add_def_with_let(false, None, "value", "pkt.seg(0).as_ref().len()")?;
    compiler.add_unsafe_set("return_ptr", "value")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["pkt".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_msg_id
    add_extern_c_wrapper_function(
        compiler,
        "ReceivedPkt_msg_id",
        &format!("ReceivedPkt<{}>", datapath),
        "msg_id",
        Some(SelfArgType::Value),
        vec![],
        Some(ArgType::Rust {
            string: "u32".to_string(),
        }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_conn_id
    add_extern_c_wrapper_function(
        compiler,
        "ReceivedPkt_conn_id",
        &format!("ReceivedPkt<{}>", datapath),
        "conn_id",
        Some(SelfArgType::Value),
        vec![],
        Some(ArgType::Rust {
            string: "usize".to_string(),
        }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_free
    common::add_free_function(compiler, "ReceivedPkt", &format!("<{}>", datapath), "")?;

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

/// Returns whether the message has a field of type CFString.
fn has_cf_string(msg_info: &MessageInfo) -> bool {
    for field in msg_info.get_fields().iter() {
        match &field.typ {
            FieldType::String | FieldType::RefCountedString => {
                return true;
            }
            _ => {}
        };
    }
    false
}

/// Returns whether the message has a field of type CFBytes.
fn has_cf_bytes(msg_info: &MessageInfo) -> bool {
    for field in msg_info.get_fields().iter() {
        match &field.typ {
            FieldType::Bytes | FieldType::RefCountedBytes => {
                return true;
            }
            _ => {}
        };
    }
    false
}

/// If the message has field(s) of type VariableList<T>, returns the type(s)
/// that parameterize the VariableList.
fn has_variable_list(
    fd: &ProtoReprInfo,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<Vec<ArgType>> {
    let mut param_tys = vec![];
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        let field_ty = ArgType::new(fd, &field_info, Some(datapath))?;
        match field_ty {
            ArgType::List { param_ty, .. } => param_tys.push(*param_ty),
            _ => {}
        }
    }
    Ok(param_tys)
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
                "copy_context",
                ArgType::RefMut {
                    inner_ty: format!("CopyContext<{}>", datapath),
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
    let box_from_raw = format!("&*(self_ as *mut {}<{}>)", ty, datapath);
    compiler.add_unsafe_def_with_let(false, None, "self_", &box_from_raw)?;
    compiler.add_unsafe_set("return_ptr", "self_.as_ref().as_ptr()")?;
    compiler.add_unsafe_set("return_len_ptr", "self_.len()")?;
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
