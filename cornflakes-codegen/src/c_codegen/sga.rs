use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
        MatchContext,
    },
};
use std::collections::HashSet;
use color_eyre::eyre::Result;
use protobuf_parser::FieldType;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    add_dependencies(fd, compiler)?;
    compiler.add_newline()?;

    // Determine whether we need to add wrapper functions for CFString, CFBytes,
    // or VariableList<T> parameterized by some type T. Then add them.
    {
        let mut added_cf_string = false;
        let mut added_cf_bytes = false;
        let mut added_variable_lists = HashSet::new();
        for message in fd.get_repr().messages.iter() {
            let msg_info = MessageInfo(message.clone());
            if !added_cf_string && has_cf_string(&msg_info) {
                add_cf_string(compiler)?;
                added_cf_string = true;
            }
            if !added_cf_bytes && has_cf_bytes(&msg_info) {
                add_cf_bytes(compiler)?;
                added_cf_bytes = true;
            }
            for param_ty in has_variable_list(fd, &msg_info)? {
                if added_variable_lists.insert(param_ty.clone()) {
                    add_variable_list(compiler, param_ty)?;
                }
            }
        }
    }

    // For each message type: add basic constructors, getters and setters, and
    // header trait functions.
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        add_default_impl(compiler, &msg_info)?;
        compiler.add_newline()?;
        add_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_header_repr(compiler, &msg_info)?;
        compiler.add_newline()?;
        add_shared_header_repr(compiler, &msg_info)?;
        break;
    }
    Ok(())
}

fn add_dependencies(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_libos::OrderedSga")?;
    compiler.add_dependency("cornflakes_libos::dynamic_sga_hdr::*")?;
    compiler.add_dependency("linux_datapath::datapath::connection::LinuxConnection")?;

    // For VariableList_<param_ty>_index
    for message in fd.get_repr().messages.iter() {
        let msg_info = MessageInfo(message.clone());
        if !has_variable_list(fd, &msg_info)?.is_empty() {
            compiler.add_dependency("std::ops::Index")?;
            break;
        }
    }
    Ok(())
}

/// Returns whether the message has a field of type CFString.
fn has_cf_string(msg_info: &MessageInfo) -> bool {
    for field in msg_info.get_fields().iter() {
        match &field.typ {
            FieldType::String | FieldType::RefCountedString => { return true; }
            _ => {}
        };
    }
    false
}

/// Returns whether the message has a field of type CFBytes.
fn has_cf_bytes(msg_info: &MessageInfo) -> bool {
    for field in msg_info.get_fields().iter() {
        match &field.typ {
            FieldType::Bytes | FieldType::RefCountedBytes => { return true; }
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
) -> Result<Vec<ArgType>> {
    let mut param_tys = vec![];
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        let field_ty = ArgType::new(fd, &field_info)?;
        match field_ty {
            ArgType::List(param_ty) => { param_tys.push(*param_ty) }
            _ => {}
        }
    }
    Ok(param_tys)
}

fn add_cf_string(compiler: &mut SerializationCompiler) -> Result<()> {
    ////////////////////////////////////////////////////////////////////////////
    // CFString_new
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("buffer", "*const ::std::os::raw::c_uchar")),
        FunctionArg::CArg(CArgInfo::arg("buffer_len", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_void")),
    ];
    let func_context = FunctionContext::new_extern_c(
        "CFString_new", true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "value",
        "std::slice::from_raw_parts(buffer, buffer_len)")?;
    compiler.add_def_with_let(false, None, "value",
        "Box::into_raw(Box::new(CFString::new_from_bytes(value)))")?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // CFString_unpack
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("value", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_uchar")),
        FunctionArg::CArg(CArgInfo::ret_len_arg()),
    ];
    let func_context = FunctionContext::new_extern_c(
        "CFString_unpack", true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "value",
        "Box::from_raw(value as *mut CFString)")?;
    compiler.add_unsafe_set("return_ptr", "value.bytes().as_ptr()")?;
    compiler.add_unsafe_set("return_len_ptr", "value.len()")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

fn add_cf_bytes(compiler: &mut SerializationCompiler) -> Result<()> {
    // todo!()
    Ok(())
}

fn add_variable_list(
    compiler: &mut SerializationCompiler,
    param_ty: ArgType,
) -> Result<()> {
    let struct_name = format!("VariableList_{}", param_ty.to_cf_string());
    let struct_ty = format!("VariableList<{}>", param_ty.to_cf_string());

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_init
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("num", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_void")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_init", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_def_with_let(false, Some(struct_ty.clone()), "list",
        "VariableList::init(num)")?;
    compiler.add_def_with_let(false, None, "list",
        "Box::into_raw(Box::new(list))")?;
    compiler.add_unsafe_set("return_ptr", "list as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_append
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("list", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("value", param_ty.to_string())),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_append", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(true, None, "list",
        &format!("Box::from_raw(list as *mut {})", &struct_ty))?;
    compiler.add_unsafe_def_with_let(false, None, "value",
        &format!("Box::from_raw(value as *mut {})", param_ty.to_cf_string()))?;
    compiler.add_func_call(Some("list".to_string()), "append",
        vec!["*value".to_string()], false)?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["list".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_len
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("list", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("usize")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_len", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(true, None, "list",
        &format!("Box::from_raw(list as *mut {})", &struct_ty))?;
    compiler.add_def_with_let(false, None, "value", "list.len()")?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["list".to_string()], false)?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_index
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("list", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("idx", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg(param_ty.to_string())),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_index", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(true, None, "list",
        &format!("Box::from_raw(list as *mut {})", &struct_ty))?;
    compiler.add_def_with_let(
        false, Some(format!("*const {}", &param_ty.to_cf_string())),
        "value", "list.index(idx)")?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["list".to_string()], false)?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum ArgType {
    Rust(String),
    Bytes,
    String,
    VoidPtr(String),
    List(Box<ArgType>),
}

impl ArgType {
    fn new(fd: &ProtoReprInfo, field: &FieldInfo) -> Result<Self> {
        let field_type = match &field.0.typ {
            FieldType::Bytes | FieldType::RefCountedBytes => ArgType::Bytes,
            FieldType::String | FieldType::RefCountedString => ArgType::String,
            _ => ArgType::Rust(fd.get_c_type(field.clone())?),
        };
        if field.is_list() {
            Ok(ArgType::List(Box::new(field_type)))
        } else {
            Ok(field_type)
        }
    }

    fn to_string(&self) -> &str {
        match self {
            ArgType::Rust(string) => string,
            ArgType::Bytes => "*const ::std::os::raw::c_void",
            ArgType::String => "*const ::std::os::raw::c_void",
            ArgType::VoidPtr(_) => "*mut ::std::os::raw::c_void",
            ArgType::List(_) => "*const ::std::os::raw::c_void",
        }
    }

    fn to_cf_string(&self) -> String {
        match self {
            ArgType::Rust(string) => string.to_string(),
            ArgType::Bytes => "CFBytes".to_string(),
            ArgType::String => "CFString".to_string(),
            ArgType::List(param_ty) => format!(
                "VariableList<{}>",
                match &**param_ty {
                    ArgType::Rust(string) => string,
                    ArgType::Bytes => "CFBytes",
                    ArgType::String => "CFString",
                    _ => unimplemented!("unhandled VariableList type"),
                },
            ),
            ArgType::VoidPtr(_) => unimplemented!("unknown struct probably"),
        }
    }
}

fn add_extern_c_wrapper_function(
    compiler: &mut SerializationCompiler,
    struct_name: &str,
    func_name: &str,
    is_mut_self: Option<bool>,
    raw_args: Vec<(&str, ArgType)>,
    raw_ret: Option<ArgType>,
    use_error_code: bool,
) -> Result<()> {
    let args = {
        let mut args = vec![];
        if is_mut_self.is_some() {
            args.push(FunctionArg::CSelfArg);
        }
        for (arg_name, arg_ty) in &raw_args {
            args.push(FunctionArg::CArg(CArgInfo::arg(arg_name, arg_ty.to_string())));
        }
        if let Some(ret_ty) = &raw_ret {
            args.push(FunctionArg::CArg(CArgInfo::ret_arg(ret_ty.to_string())));
        }
        args
    };

    let func_context = FunctionContext::new_extern_c(
        &format!("{}_{}", struct_name, func_name),
        true, args, use_error_code,
    );
    compiler.add_context(Context::Function(func_context))?;

    if let Some(is_mut) = is_mut_self {
        compiler.add_unsafe_def_with_let(is_mut, None, "self_", &format!(
            "Box::from_raw(self_ as *mut {})", struct_name,
        ))?;
    }

    // Format arguments
    for (i, (arg_name, arg_ty)) in raw_args.iter().enumerate() {
        let left = format!("arg{}", i);
        let right = match arg_ty {
            ArgType::Rust(_) => arg_name.to_string(),
            ArgType::Bytes | ArgType::String | ArgType::List(_) => format!(
                "unsafe {{ *Box::from_raw({} as *mut {}) }}",
                arg_name, arg_ty.to_cf_string(),
            ),
            ArgType::VoidPtr(inner_ty) => format!(
                "unsafe {{ Box::from_raw({} as *mut {}) }}",
                arg_name, inner_ty,
            ),
        };
        compiler.add_def_with_let(false, None, &left, &right)?;
    }

    // Generate function arguments and return type
    let args = (0..raw_args.len()).map(|i| format!("arg{}", i)).collect::<Vec<_>>();
    let ret_ty = if let Some(ref ret_ty) = raw_ret {
        match ret_ty {
            ArgType::List(_) => {
                // Need to cast VariableList to *const pointer because they are
                // returned as a reference.
                Some(format!("*const {}", ret_ty.to_cf_string()))
            }
            _ => None,
        }
    } else {
        None
    };

    // Call function wrapper
    if is_mut_self.is_some() {
        compiler.add_func_call_with_let("value", ret_ty,
            Some("self_".to_string()), func_name, args, false)?;
    } else {
        compiler.add_func_call_with_let("value", ret_ty, None,
            &format!("{}::{}", struct_name, func_name), args, false)?;
    }

    // Unformat arguments
    if is_mut_self.is_some() {
        compiler.add_func_call(None, "Box::into_raw", vec!["self_".to_string()], false)?;
    }
    for (i, (_, arg_ty)) in raw_args.iter().enumerate() {
        match arg_ty {
            ArgType::Rust(_) => { continue; },
            ArgType::Bytes => { continue; },
            ArgType::String => { continue; },
            ArgType::VoidPtr(_) => {
                compiler.add_func_call(None, "Box::into_raw", vec![format!("arg{}", i)], false)?;
            },
            ArgType::List(_) => { continue; },
        };
    }

    // Unwrap result if uses an error code
    if use_error_code {
        let match_context = MatchContext::new_with_def(
            "value", vec!["Ok(value)".to_string(), "Err(_)".to_string()], "value",
        );
        compiler.add_context(Context::Match(match_context))?;
        compiler.add_return_val("value", false)?;
        compiler.pop_context()?;
        compiler.add_return_val("1", true)?;
        compiler.pop_context()?;
    }

    // Marshall return value into C type
    if let Some(ret_ty) = &raw_ret {
        match ret_ty {
            ArgType::Rust(_) => {
                compiler.add_unsafe_set("return_ptr", "value")?;
            }
            ArgType::VoidPtr(_) | ArgType::Bytes | ArgType::String
                    | ArgType::List(_) => {
                compiler.add_func_call_with_let("value", None, None,
                   "Box::into_raw", vec!["Box::new(value)".to_string()],
                   false)?;
                compiler.add_unsafe_set("return_ptr", "value as _")?;
            }
        }
    }

    if use_error_code {
        compiler.add_line("0")?;
    }

    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

fn add_default_impl(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "default",
        None,
        vec![],
        Some(ArgType::VoidPtr(msg_info.get_name())),
        false,
    )?;
    Ok(())
}

fn add_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    compiler.add_newline()?;
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "new",
        None,
        vec![],
        Some(ArgType::VoidPtr(msg_info.get_name())),
        false,
    )?;

    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        add_field_methods(fd, compiler, msg_info, &field_info)?;
    }
    Ok(())
}

fn add_field_methods(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    // add has_x, get_x, set_x
    compiler.add_newline()?;
    add_has(compiler, msg_info, field)?;
    compiler.add_newline()?;
    add_get(fd, compiler, msg_info, field)?;
    compiler.add_newline()?;
    add_set(fd, compiler, msg_info, field)?;
    compiler.add_newline()?;

    // if field is a list or a nested struct, add get_mut_x
    if field.is_list() || field.is_nested_msg() {
        add_get_mut(fd, compiler, msg_info, field)?;
    }

    // if field is list, add init_x
    if field.is_list() {
        add_list_init(compiler, msg_info, field)?;
    }
    Ok(())
}

fn add_has(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        &format!("has_{}", field.get_name()),
        Some(false),
        vec![],
        Some(ArgType::Rust("bool".to_string())),
        false,
    )?;
    Ok(())
}

fn add_get(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let return_type = ArgType::new(fd, field)?;
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        &format!("get_{}", field.get_name()),
        Some(false),
        vec![],
        Some(return_type),
        false,
    )?;
    Ok(())
}

fn add_get_mut(
    _fd: &ProtoReprInfo,
    _compiler: &mut SerializationCompiler,
    _msg_info: &MessageInfo,
    _field: &FieldInfo,
) -> Result<()> {
    // let field_name = field.get_name();
    // let rust_type = fd.get_rust_type(field.clone())?;
    // let func_context = FunctionContext::new_extern_c(
    //     &format!("{}_get_mut_{}", msg_info.get_name(), &field_name),
    //     true,
    //     vec![FunctionArg::MutSelfArg],
    //     // &format!("&mut {}", rust_type)
    //     false,
    // );
    // compiler.add_context(Context::Function(func_context))?;
    // compiler.add_return_val(&format!("&mut self.{}", &field_name), false)?;
    // compiler.pop_context()?;
    Ok(())
}

fn add_set(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let field_name = field.get_name();
    let field_type = ArgType::new(fd, field)?;
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        &format!("set_{}", field.get_name()),
        Some(true),
        vec![(&field_name, field_type)],
        None,
        false,
    )?;
    Ok(())
}

fn add_list_init(
    _compiler: &mut SerializationCompiler,
    _msg_info: &MessageInfo,
    _field: &FieldInfo,
) -> Result<()> {
    // let func_context = FunctionContext::new_extern_c(
    //     &format!("{}_init_{}", msg_info.get_name(), field.get_name()),
    //     true,
    //     vec![
    //         FunctionArg::MutSelfArg,
    //         FunctionArg::new_arg("num", ArgInfo::owned("usize")),
    //     ],
    //     false,
    // );
    // compiler.add_context(Context::Function(func_context))?;
    // match &field.0.typ {
    //     FieldType::Int32
    //     | FieldType::Int64
    //     | FieldType::Uint32
    //     | FieldType::Uint64
    //     | FieldType::Float => {
    //         compiler.add_statement(
    //             &format!("self.{}", field.get_name()),
    //             &format!("List::init(num)"),
    //         )?;
    //     }
    //     FieldType::String
    //     | FieldType::Bytes
    //     | FieldType::RefCountedString
    //     | FieldType::RefCountedBytes => {
    //         compiler.add_statement(
    //             &format!("self.{}", field.get_name()),
    //             &format!("VariableList::init(num)"),
    //         )?;
    //     }
    //     FieldType::MessageOrEnum(_) => {
    //         compiler.add_statement(
    //             &format!("self.{}", field.get_name()),
    //             &format!("VariableList::init(num)"),
    //         )?;
    //     }
    //     x => {
    //         bail!("Field type not supported: {:?}", x);
    //     }
    // }
    // compiler.add_func_call(
    //     Some("self".to_string()),
    //     "set_bitmap_field",
    //     vec![format!("{}", field.get_bitmap_idx_str(true))],
    //     false,
    // )?;
    // compiler.pop_context()?;
    Ok(())
}

fn add_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // add dynamic header size function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "dynamic_header_size",
        Some(false),
        vec![],
        Some(ArgType::Rust("usize".to_string())),
        false,
    )?;

    // add dynamic header start function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "dynamic_header_start",
        Some(false),
        vec![],
        Some(ArgType::Rust("usize".to_string())),
        false,
    )?;

    // add num scatter_gather_entries function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "num_scatter_gather_entries",
        Some(false),
        vec![],
        Some(ArgType::Rust("usize".to_string())),
        false,
    )?;

    Ok(())
}

// These aren't generated functions so we generate the wrappers manually.
// See: cornflakes-codegen/src/utils/dynamic_sga_hdr.rs
fn add_shared_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    add_deserialize_function(compiler, &msg_info.get_name())?;
    add_serialize_into_sga_function(compiler, &msg_info.get_name())?;
    Ok(())
}

fn add_deserialize_function(
    compiler: &mut SerializationCompiler,
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
        &format!("{}_{}", struct_name, "deserialize"),
        true, args, true,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(true, None, "self_", &format!(
        "Box::from_raw(self_ as *mut {})", struct_name))?;
    compiler.add_unsafe_def_with_let(false, None, "arg0", "std::slice::from_raw_parts(buffer, buffer_len)")?;
    compiler.add_func_call_with_let("value", None, Some("self_".to_string()),
        "deserialize", vec!["arg0".to_string()], false)?;
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

fn add_serialize_into_sga_function(
    compiler: &mut SerializationCompiler,
    struct_name: &str,
) -> Result<()> {
    let args = {
        let mut args = vec![];
        args.push(FunctionArg::CSelfArg);
        args.push(FunctionArg::CArg(CArgInfo::arg("ordered_sga", "*mut
            ::std::os::raw::c_void")));
        args.push(FunctionArg::CArg(CArgInfo::arg("datapath", "*mut
            ::std::os::raw::c_void")));
        args
    };

    let func_context = FunctionContext::new_extern_c(
        &format!("{}_{}", struct_name, "serialize_into_sga"),
        true, args, true,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "self_", &format!(
        "Box::from_raw(self_ as *mut {})", struct_name))?;
    compiler.add_def_with_let(true, None, "arg0", "unsafe { Box::from_raw
        (ordered_sga as *mut OrderedSga) }")?;
    compiler.add_def_with_let(false, None, "arg1", "unsafe { Box::from_raw
        (datapath as *mut LinuxConnection) }")?;
    compiler.add_func_call_with_let("value", None, Some("self_".to_string()),
        "serialize_into_sga", vec!["&mut arg0".to_string(),
        "arg1.as_ref()".to_string()], false)?;
    compiler.add_func_call(None, "Box::into_raw", vec!["self_".to_string()],
        false)?;
    compiler.add_func_call(None, "Box::into_raw", vec!["arg0".to_string()],
        false)?;
    compiler.add_func_call(None, "Box::into_raw", vec!["arg1".to_string
        ()], false)?;

    let match_context = MatchContext::new_with_def(
        "value", vec!["Ok(value)".to_string(), "Err(_)".to_string()], "value",
    );
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
