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

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ArgType {
    Rust { string: String},
    Bytes { datapath: Option<String> },
    String { datapath: Option<String> },
    VoidPtr { inner_ty: String },
    Ref { inner_ty: String },
    RefMut { inner_ty: String },
    List { datapath: Option<String>, param_ty: Box<ArgType> },
}

impl ArgType {
    pub fn new(
        fd: &ProtoReprInfo,
        field: &FieldInfo,
        datapath: Option<&str>,
    ) -> Result<Self> {
        let field_type = match &field.0.typ {
            FieldType::Bytes | FieldType::RefCountedBytes =>
                ArgType::Bytes { datapath: datapath.map(|d| d.to_string()) },
            FieldType::String | FieldType::RefCountedString =>
                ArgType::String { datapath: datapath.map(|d| d.to_string()) },
            _ => ArgType::Rust { string: fd.get_c_type(field.clone())? },
        };
        if field.is_list() {
            Ok(ArgType::List {
                datapath: datapath.map(|d| d.to_string()),
                param_ty: Box::new(field_type)
            })
        } else {
            Ok(field_type)
        }
    }

    pub fn to_string(&self) -> &str {
        match self {
            ArgType::Rust { string } => string,
            ArgType::Bytes{..} => "*const ::std::os::raw::c_void",
            ArgType::String{..} => "*const ::std::os::raw::c_void",
            ArgType::VoidPtr{..} => "*mut ::std::os::raw::c_void",
            ArgType::List{..} => "*const ::std::os::raw::c_void",
            ArgType::Ref{..} => "*mut ::std::os::raw::c_void",
            ArgType::RefMut{..} => "*mut ::std::os::raw::c_void",
        }
    }

    pub fn to_cf_string(&self) -> String {
        match self {
            ArgType::Rust { string } => string.to_string(),
            ArgType::Bytes { datapath } => match datapath {
                Some(datapath) => format!("CFBytes<{}>", datapath),
                None => "CFBytes".to_string(),
            },
            ArgType::String { datapath } => match datapath {
                Some(datapath) => format!("CFString<{}>", datapath),
                None => "CFString".to_string(),
            },
            ArgType::List { param_ty, datapath } => match datapath {
                Some(datapath) => format!(
                    "VariableList<{}, {}>",
                    &match &**param_ty {
                        ArgType::Rust { string } => string.to_string(),
                        ArgType::Bytes{..} => format!("CFBytes<{}>", datapath),
                        ArgType::String{..} => format!("CFString<{}>", datapath),
                        _ => unimplemented!("unhandled VariableList type"),
                    },
                    datapath,
                ),
                None => format!(
                    "VariableList<{}>",
                    match &**param_ty {
                        ArgType::Rust { string } => string,
                        ArgType::Bytes{..} => "CFBytes",
                        ArgType::String{..} => "CFString",
                        _ => unimplemented!("unhandled VariableList type"),
                    },
                ),
            },
            ArgType::VoidPtr{..} => unimplemented!("unknown struct probably"),
            ArgType::Ref{..} => unimplemented!("unknown struct ref probably"),
            ArgType::RefMut{..} => unimplemented!("unknown struct ref mut
                probably"),
        }
    }
}

/// Determine whether we need to add wrapper functions for CFString, CFBytes,
/// or VariableList<T> parameterized by some type T. Then add them.
pub fn add_cornflakes_structs(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    datapath: Option<&str>,
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
pub fn has_cf_string(msg_info: &MessageInfo) -> bool {
    for field in msg_info.get_fields().iter() {
        match &field.typ {
            FieldType::String | FieldType::RefCountedString => { return true; }
            _ => {}
        };
    }
    false
}

/// Returns whether the message has a field of type CFBytes.
pub fn has_cf_bytes(msg_info: &MessageInfo) -> bool {
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
pub fn has_variable_list(
    fd: &ProtoReprInfo,
    msg_info: &MessageInfo,
    datapath: Option<&str>,
) -> Result<Vec<ArgType>> {
    let mut param_tys = vec![];
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        let field_ty = ArgType::new(fd, &field_info, datapath)?;
        match field_ty {
            ArgType::List { param_ty, .. } => { param_tys.push(*param_ty) }
            _ => {}
        }
    }
    Ok(param_tys)
}

/// ty == "CFString" or "CFBytes"
pub fn add_cf_string_or_bytes(
    compiler: &mut SerializationCompiler,
    datapath: Option<&str>,
    ty: &str,
) -> Result<()> {
    ////////////////////////////////////////////////////////////////////////////
    // <ty>_new
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("buffer", "*const ::std::os::raw::c_uchar")),
        FunctionArg::CArg(CArgInfo::arg("buffer_len", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_uchar")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_new", ty), true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "value",
        "std::slice::from_raw_parts(buffer, buffer_len)")?;
    let new_from_bytes = match datapath {
        Some(datapath) => format!(
            "Box::into_raw(Box::new({}::<{}>::new_from_bytes(value)))",
            ty, datapath,
        ),
        None => format!(
            "Box::into_raw(Box::new({}::new_from_bytes(value)))", ty,
        ),
    };
    compiler.add_def_with_let(false, None, "value", &new_from_bytes)?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // <ty>_unpack
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("self_", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("*const ::std::os::raw::c_uchar")),
        FunctionArg::CArg(CArgInfo::ret_len_arg()),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_unpack", ty), true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    let box_from_raw = match datapath {
        Some(datapath) => format!(
            "Box::from_raw(self_ as *mut {}<{}>)", ty, datapath,
        ),
        None => format!("Box::from_raw(self_ as *mut {})", ty),
    };
    compiler.add_unsafe_def_with_let(false, None, "self_", &box_from_raw)?;
    // Note: The two different header types just have a different function name
    // to get a pointer to the bytes.
    compiler.add_unsafe_set("return_ptr", match datapath {
        Some(_) => "self_.as_bytes().as_ptr()",
        None => "self_.bytes().as_ptr()",
    })?;
    compiler.add_unsafe_set("return_len_ptr", "self_.len()")?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["self_".to_string()], false)?;
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
pub fn add_variable_list(
    compiler: &mut SerializationCompiler,
    param_ty: ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    let struct_name = match param_ty {
        ArgType::Rust { ref string } => format!("VariableList_{}", string),
        ArgType::Bytes{..} => "VariableList_CFBytes".to_string(),
        ArgType::String{..} => "VariableList_CFString".to_string(),
        _ => unimplemented!("unimplemented VariableList type"),
    };
    let struct_ty = ArgType::List {
        datapath: datapath.map(|d| d.to_string()),
        param_ty: Box::new(param_ty.clone()),
    }.to_cf_string();

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
        FunctionArg::CArg(CArgInfo::arg("self_", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("value", param_ty.to_string())),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_append", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "self_",
        &format!("Box::from_raw(self_ as *mut *mut {})", &struct_ty))?;
    compiler.add_unsafe_def_with_let(false, None, "self_ref", "&mut **self_")?;
    compiler.add_unsafe_def_with_let(false, None, "value",
        &format!("Box::from_raw(value as *mut {})", param_ty.to_cf_string()))?;
    compiler.add_func_call(Some("self_ref".to_string()), "append",
        vec!["*value".to_string()], false)?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["self_".to_string()], false)?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_len
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("self_", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::ret_arg("usize")),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_len", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "self_",
        &format!("Box::from_raw(self_ as *mut *const {})", &struct_ty))?;
    compiler.add_unsafe_def_with_let(false, None, "self_ref", "&**self_")?;
    compiler.add_def_with_let(false, None, "value", "self_ref.len()")?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["self_".to_string()], false)?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_index
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("self_", "*const ::std::os::raw::c_void")),
        FunctionArg::CArg(CArgInfo::arg("idx", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg(param_ty.to_string())),
    ];
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_index", &struct_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_unsafe_def_with_let(false, None, "self_",
        &format!("Box::from_raw(self_ as *mut *const {})", &struct_ty))?;
    compiler.add_unsafe_def_with_let(false, None, "self_ref", "&**self_")?;
    compiler.add_def_with_let(
        false, Some(format!("*const {}", &param_ty.to_cf_string())),
        "value", "self_ref.index(idx)")?;
    compiler.add_func_call(None, "Box::into_raw",
        vec!["self_".to_string()], false)?;
    compiler.add_unsafe_set("return_ptr", "value as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

pub fn add_extern_c_wrapper_function(
    compiler: &mut SerializationCompiler,
    extern_name: &str,
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
        extern_name, true, args, use_error_code,
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
            ArgType::Rust{..} => arg_name.to_string(),
            ArgType::Bytes{..} | ArgType::String{..} | ArgType::List{..} => format!(
                "unsafe {{ *Box::from_raw({} as *mut {}) }}",
                arg_name, arg_ty.to_cf_string(),
            ),
            ArgType::VoidPtr { inner_ty } | ArgType::Ref { inner_ty } => format!(
                "unsafe {{ Box::from_raw({} as *mut {}) }}",
                arg_name, inner_ty,
            ),
            ArgType::RefMut { inner_ty } => format!(
                "{} as *mut {}",
                arg_name, inner_ty,
            ),
        };
        compiler.add_def_with_let(false, None, &left, &right)?;
    }

    // Generate function arguments and return type
    let args = raw_args.iter()
        .enumerate()
        .map(|(i, (_, arg_ty))| match arg_ty {
            ArgType::Ref{..} => format!("&arg{}", i),
            ArgType::RefMut{..} => format!("unsafe {{ &mut *arg{} }}", i),
            _ => format!("arg{}", i),
        })
        .collect::<Vec<_>>();
    let ret_ty = if let Some(ref ret_ty) = raw_ret {
        match ret_ty {
            ArgType::List{..} => {
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
            ArgType::Rust{..} => {
                compiler.add_unsafe_set("return_ptr", "value")?;
            }
            ArgType::VoidPtr{..} | ArgType::Bytes{..} | ArgType::String{..}
                    | ArgType::Ref{..} | ArgType::List{..} => {
                compiler.add_func_call_with_let("value", None, None,
                   "Box::into_raw", vec!["Box::new(value)".to_string()],
                   false)?;
                compiler.add_unsafe_set("return_ptr", "value as _")?;
            }
            ArgType::RefMut{..} => unimplemented!(),
        }
    }

    // Unformat arguments
    if is_mut_self.is_some() {
        compiler.add_func_call(None, "Box::into_raw", vec!["self_".to_string()], false)?;
    }
    for (i, (_, arg_ty)) in raw_args.iter().enumerate() {
        match arg_ty {
            ArgType::Rust{..} => { continue; },
            ArgType::Bytes{..} => { continue; },
            ArgType::String{..} => { continue; },
            ArgType::VoidPtr{..} | ArgType::Ref{..} => {
                compiler.add_func_call(None, "Box::into_raw", vec![format!("arg{}", i)], false)?;
            },
            ArgType::RefMut{..} => { continue; },
            ArgType::List{..} => { continue; },
        };
    }

    if use_error_code {
        compiler.add_line("0")?;
    }

    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

pub fn add_default_impl(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: Option<&str>,
) -> Result<()> {
    let func_name = match datapath {
        Some(datapath) => format!("<{}>::default", datapath),
        None => "default".to_string(),
    };
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_default", msg_info.get_name()),
        &msg_info.get_name(),
        &func_name,
        None,
        vec![],
        Some(ArgType::VoidPtr { inner_ty: msg_info.get_name() }),
        false,
    )?;
    Ok(())
}

pub fn add_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    datapath: Option<&str>,
) -> Result<()> {
    compiler.add_newline()?;
    let func_name = match datapath {
        Some(datapath) => format!("<{}>::new", datapath),
        None => "new".to_string(),
    };
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_new", msg_info.get_name()),
        &msg_info.get_name(),
        &func_name,
        None,
        vec![],
        Some(ArgType::VoidPtr { inner_ty: msg_info.get_name() }),
        false,
    )?;

    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        add_field_methods(fd, compiler, msg_info, &field_info, datapath)?;
    }
    Ok(())
}

pub fn add_field_methods(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<()> {
    // add has_x, get_x, set_x
    compiler.add_newline()?;
    add_has(compiler, msg_info, field, datapath)?;
    compiler.add_newline()?;
    add_get(fd, compiler, msg_info, field, datapath)?;
    compiler.add_newline()?;
    add_set(fd, compiler, msg_info, field, datapath)?;
    compiler.add_newline()?;

    // if field is a list or a nested struct, add get_mut_x
    if field.is_list() || field.is_nested_msg() {
        add_get_mut(fd, compiler, msg_info, field, datapath)?;
    }

    // if field is list, add init_x
    if field.is_list() {
        add_list_init(compiler, msg_info, field, datapath)?;
    }
    Ok(())
}

pub fn add_has(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<()> {
    let struct_name = match datapath {
        Some(datapath) => format!("{}<{}>", msg_info.get_name(), datapath),
        None => msg_info.get_name(),
    };
    let func_name = format!("has_{}", field.get_name());
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_{}", msg_info.get_name(), &func_name),
        &struct_name,
        &func_name,
        Some(false),
        vec![],
        Some(ArgType::Rust { string: "bool".to_string() }),
        false,
    )?;
    Ok(())
}

pub fn add_get(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<()> {
    let return_type = ArgType::new(fd, field, datapath)?;
    let struct_name = match datapath {
        Some(datapath) => format!("{}<{}>", msg_info.get_name(), datapath),
        None => msg_info.get_name(),
    };
    let func_name = format!("get_{}", field.get_name());
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_{}", msg_info.get_name(), &func_name),
        &struct_name,
        &func_name,
        Some(false),
        vec![],
        Some(return_type),
        false,
    )?;
    Ok(())
}

/// NOTE(GY): Is this FFI function necessary? I think the mutability of the
/// pointer gets erased when it's cased to a C pointer anyway.
pub fn add_get_mut(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<()> {
    let return_type = ArgType::new(fd, field, datapath)?;
    let struct_name = match datapath {
        Some(datapath) => format!("{}<{}>", msg_info.get_name(), datapath),
        None => msg_info.get_name(),
    };
    let func_name = format!("get_mut_{}", field.get_name());
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_{}", msg_info.get_name(), &func_name),
        &struct_name,
        &func_name,
        Some(true),
        vec![],
        Some(return_type),
        false,
    )?;
    Ok(())
}

pub fn add_set(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<()> {
    let field_name = field.get_name();
    let field_type = ArgType::new(fd, field, datapath)?;
    let struct_name = match datapath {
        Some(datapath) => format!("{}<{}>", msg_info.get_name(), datapath),
        None => msg_info.get_name(),
    };
    let func_name = format!("set_{}", field.get_name());
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_{}", msg_info.get_name(), &func_name),
        &struct_name,
        &func_name,
        Some(true),
        vec![(&field_name, field_type)],
        None,
        false,
    )?;
    Ok(())
}

pub fn add_list_init(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<()> {
    let struct_name = match datapath {
        Some(datapath) => format!("{}<{}>", msg_info.get_name(), datapath),
        None => msg_info.get_name(),
    };
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_init_{}", msg_info.get_name(), field.get_name()),
        &struct_name,
        &format!("init_{}", field.get_name()),
        Some(true),
        vec![("num", ArgType::Rust { string: "usize".to_string() })],
        None,
        false,
    )?;
    Ok(())
}
