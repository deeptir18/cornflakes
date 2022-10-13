use super::{
    super::header_utils::{MessageInfo, ProtoReprInfo, FieldInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
    },
    common::{
        self, add_extern_c_wrapper_function,
        SelfArgType, ArgType,
    },
};
use std::collections::HashSet;
use protobuf_parser::FieldType;
use color_eyre::eyre::Result;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    let datapath = "Mlx5Connection";  // hardcoded datapath
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
    add_received_pkt_functions(compiler, datapath)?;

    // For each message type: add basic constructors, getters and setters, and
    // header trait functions.
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        compiler.add_newline()?;
        add_new_in(compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        common::add_impl(fd, compiler, &msg_info, Some(datapath), true)?;
        compiler.add_newline()?;
        add_dynamic_rcsga_hybrid_header_repr(compiler, &msg_info, datapath)?;
        compiler.add_newline()?;
        add_queue_cornflakes_obj_function(compiler, &msg_info, datapath)?;
        // compiler.add_newline()?;
        // add_rcsga_header_repr(compiler, &msg_info, datapath)?;
        // compiler.add_newline()?;
        // add_shared_rcsga_header_repr(compiler, &msg_info, datapath)?;
    }

    Ok(())
}

fn add_hybrid_rcsga_dependencies(fd: &ProtoReprInfo, compiler: &mut
        SerializationCompiler) -> Result<()> {
    compiler.add_dependency("bumpalo")?;
    compiler.add_dependency("cornflakes_libos::datapath::{Datapath, ReceivedPkt}")?;
    compiler.add_dependency("cornflakes_libos::{ArenaOrderedSga, ArenaOrderedRcSga, CopyContext}")?;
    compiler.add_dependency("cornflakes_libos::dynamic_rcsga_hybrid_hdr::*")?;
    compiler.add_dependency("mlx5_datapath::datapath::connection::Mlx5Connection")?;

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

fn add_new_in(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_new_in<'arena, 'registered: 'arena>", msg_info.get_name()),
        &format!("{}::<'arena, 'registered, {}>", &msg_info.get_name(), datapath),
        "new_in",
        None,
        vec![("arena", ArgType::Ref { inner_ty: "bumpalo::Bump".to_string() })],
        Some(ArgType::VoidPtr { inner_ty: msg_info.get_name() }),
        false,
    )?;
    Ok(())
}

fn add_dynamic_rcsga_hybrid_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: &str,
) -> Result<()> {
    // add deserialize function
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_deserialize", msg_info.get_name()),
        &format!("{}<{}>", msg_info.get_name(), datapath),
        "deserialize",
        Some(SelfArgType::Mut),
        vec![
            ("pkt", ArgType::Ref {
                inner_ty: format!("ReceivedPkt<{}>", datapath),
            }),
            ("offset", ArgType::Rust { string: "usize".to_string() }),
            ("arena", ArgType::Ref { inner_ty: "bumpalo::Bump".to_string() }),
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
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_{}_queue_cornflakes_obj<'arena>", datapath,
            msg_info.get_name()),
        datapath,
        "queue_cornflakes_obj",
        Some(SelfArgType::Mut),
        vec![
            ("msg_id", ArgType::Rust { string: "u32".to_string() }),
            ("conn_id", ArgType::Rust { string: "usize".to_string() }),
            ("copy_context", ArgType::RefMut {
                inner_ty: format!("CopyContext<'arena, {}>", datapath),
            }),
            ("cornflakes_obj", ArgType::VoidPtr {
                inner_ty: format!("{}<{}>", msg_info.get_name(), datapath),
            }),
            ("end_batch", ArgType::Rust { string: "bool".to_string() }),
        ],
        None,
        true,
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

fn add_copy_context_functions(
    compiler: &mut SerializationCompiler,
    datapath: &str,
) -> Result<()> {

    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_new
    add_extern_c_wrapper_function(
        compiler,
        "CopyContext_new",
        "CopyContext",
        "new",
        None,
        vec![
            ("arena", ArgType::Ref { inner_ty: "bumpalo::Bump".to_string() }),
            ("datapath", ArgType::RefMut { inner_ty: datapath.to_string() }),
        ],
        Some(ArgType::VoidPtr { inner_ty: "CopyContext".to_string() }),
        true,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_reset
    add_extern_c_wrapper_function(
        compiler,
        "CopyContext_reset",
        &format!("CopyContext<{}>", datapath),
        "reset",
        Some(SelfArgType::Mut),
        vec![("datapath", ArgType::RefMut { inner_ty: datapath.to_string() })],
        None,
        true,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // CopyContext_free
    common::add_free_function(
        compiler,
        "CopyContext",
        &format!("<{}>", datapath),
    )?;

    Ok(())
}

fn add_received_pkt_functions(
    compiler: &mut SerializationCompiler,
    datapath: &str,
) -> Result<()> {

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_msg_type
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("pkt", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("u16")),
    ];
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_msg_type",
        true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw =
        format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
    compiler.add_unsafe_def_with_let(false, None, "pkt", &box_from_raw)?;
    compiler.add_def_with_let(false, None, "seg", "pkt.seg(0).as_ref()")?;
    compiler.add_def_with_let(false, None, "msg_type",
        "(seg[1] as u16) | ((seg[0] as u16) << 8)")?;
    compiler.add_unsafe_set("return_ptr", "msg_type")?;
    compiler.add_func_call(None, "Box::into_raw", vec!["pkt".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_size
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("pkt", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("u16")),
    ];
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_size",
        true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw =
        format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
    compiler.add_unsafe_def_with_let(false, None, "pkt", &box_from_raw)?;
    compiler.add_def_with_let(false, None, "seg", "pkt.seg(0).as_ref()")?;
    compiler.add_def_with_let(false, None, "size",
        "(seg[3] as u16) | ((seg[2] as u16) << 8)")?;
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
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_data",
        true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw =
        format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
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
    let func_context = FunctionContext::new_extern_c("ReceivedPkt_data_len",
        true, args, false);
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw =
        format!("Box::from_raw(pkt as *mut ReceivedPkt<{}>)", datapath);
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
        Some(ArgType::Rust { string: "u32".to_string() }),
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
        Some(ArgType::Rust { string: "usize".to_string() }),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // ReceivedPkt_free
    common::add_free_function(
        compiler,
        "ReceivedPkt",
        &format!("<{}>", datapath),
    )?;

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
        vec![
            ("arena", ArgType::Ref { inner_ty: "bumpalo::Bump".to_string() }),
        ],
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
            ("datapath", ArgType::RefMut { inner_ty: datapath.to_string()}),
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
    let box_from_raw =
        format!("Box::from_raw(self_ as *mut {}<{}>)", ty, datapath);
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
            ("num", ArgType::Rust { string: "usize".to_string() }),
            ("arena", ArgType::Ref { inner_ty: "bumpalo::Bump".to_string() }),
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
