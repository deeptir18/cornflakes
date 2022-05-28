use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
        MatchContext,
    },
};
use color_eyre::eyre::Result;
use protobuf_parser::FieldType;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    add_dependencies(fd, compiler)?;
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

fn add_dependencies(_repr: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_libos::OrderedSga")?;
    compiler.add_dependency("cornflakes_codegen::utils::dynamic_sga_hdr::*")?;
    compiler.add_dependency("linux_datapath::datapath::connection::LinuxConnection")?;
    Ok(())
}

fn is_array(field: &FieldInfo) -> bool {
    match &field.0.typ {
        FieldType::Bytes | FieldType::RefCountedBytes => true,
        _ => false,
    }
}

enum ArgType {
    Rust(String),
    Bytes,
    VoidPtr(String),
}

impl ArgType {
    fn is_array(&self) -> bool {
        match self {
            ArgType::Bytes => true,
            _ => false,
        }
    }

    fn to_string(&self) -> &str {
        match self {
            ArgType::Rust(string) => string,
            ArgType::Bytes => "*const ::std::os::raw::c_uchar",
            ArgType::VoidPtr(_) => "*mut ::std::os::raw::c_void",
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
            if arg_ty.is_array() {
                args.push(FunctionArg::CArg(CArgInfo::len_arg(arg_name)));
            }
        }
        if let Some(ret_ty) = &raw_ret {
            args.push(FunctionArg::CArg(CArgInfo::ret_arg(ret_ty.to_string())));
            if ret_ty.is_array() {
                args.push(FunctionArg::CArg(CArgInfo::ret_len_arg()));
            }
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
            ArgType::Bytes => format!(
                "CFBytes::new(unsafe {{ std::slice::from_raw_parts({}, {}_len) }})",
                arg_name, arg_name,
            ),
            ArgType::VoidPtr(inner_ty) => format!(
                "unsafe {{ Box::from_raw({} as *mut {}) }}",
                arg_name, inner_ty,
            ),
        };
        compiler.add_def_with_let(false, None, &left, &right)?;
    }

    // Call function wrapper
    let args = (0..raw_args.len()).map(|i| format!("arg{}", i)).collect::<Vec<_>>();
    if is_mut_self.is_some() {
        compiler.add_func_call_with_let("value", Some("self_".to_string()), func_name, args, false)?;
    } else {
        compiler.add_func_call_with_let("value", None, &format!("{}::{}", struct_name, func_name), args, false)?;
    }

    // Unformat arguments
    if is_mut_self.is_some() {
        compiler.add_func_call(None, "Box::into_raw", vec!["self_".to_string()], false)?;
    }
    for (i, (_, arg_ty)) in raw_args.iter().enumerate() {
        match arg_ty {
            ArgType::Rust(_) => { continue; },
            ArgType::Bytes => { continue; },
            ArgType::VoidPtr(_) => {
                compiler.add_func_call(None, "Box::into_raw", vec![format!("arg{}", i)], false)?;
            },
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
            ArgType::Bytes => {
                compiler.add_unsafe_set("return_ptr", "value.get_ptr().as_ptr()")?;
                compiler.add_unsafe_set("return_len_ptr", "value.len()")?;
            }
            ArgType::VoidPtr(_) => {
                compiler.add_func_call_with_let("value", None, "Box::into_raw",
                   vec!["Box::new(value)".to_string()], false)?;
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
    let return_type = if is_array(field) {
        ArgType::Bytes
    } else {
        ArgType::Rust(fd.get_c_type(field.clone())?)
    };
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
    unimplemented!()
}

fn add_set(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let field_name = field.get_name();
    let field_type = if is_array(field) {
        ArgType::Bytes
    } else {
        ArgType::Rust(fd.get_c_type(field.clone())?)
    };
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
    unimplemented!()
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
    compiler.add_func_call_with_let("value", Some("self_".to_string()),
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
    compiler.add_func_call_with_let("value", Some("self_".to_string()),
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
