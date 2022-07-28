use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
};
use std::collections::HashSet;
use color_eyre::eyre::Result;
use protobuf_parser::FieldType;
use ffiber::{CDylibCompiler, types::*, compiler::*};

fn new_arg_type(
    fd: &ProtoReprInfo,
    field: &FieldInfo,
    datapath: Option<&str>,
) -> Result<ArgType> {
    let mut params = datapath.map(|d| vec![Box::new(ArgType::new_struct(d))])
        .unwrap_or(vec![]);
    let field_type = match &field.0.typ {
        FieldType::Bytes | FieldType::RefCountedBytes => ArgType::Struct {
            name: "CFBytes".to_string(),
            params: params.clone(),
        },
        FieldType::String | FieldType::RefCountedString => ArgType::Struct {
            name: "CFString".to_string(),
            params: params.clone(),
        },
        _ => ArgType::Primitive(fd.get_c_type(field.clone())?),
    };
    if field.is_list() {
        params.insert(0, Box::new(field_type));
        Ok(ArgType::Struct {
            name: "VariableList".to_string(),
            params,
        })
    } else {
        Ok(field_type)
    }
}

/// Determine whether we need to add wrapper functions for CFString, CFBytes,
/// or VariableList<T> parameterized by some type T. Then add them.
pub fn add_cornflakes_structs(
    fd: &ProtoReprInfo,
    compiler: &mut CDylibCompiler,
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
pub fn has_variable_list(
    fd: &ProtoReprInfo,
    msg_info: &MessageInfo,
    datapath: Option<&str>,
) -> Result<Vec<ArgType>> {
    let mut param_tys = vec![];
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        let field_ty = new_arg_type(fd, &field_info, datapath)?;
        match field_ty {
            ArgType::Struct { name, params } => {
                if name == "VariableList" {
                    assert!(!params.is_empty());
                    param_tys.push(*params[0].clone());
                }
            }
            _ => {}
        }
    }
    Ok(param_tys)
}

/// ty == "CFString" or "CFBytes"
fn add_cf_string_or_bytes(
    compiler: &mut CDylibCompiler,
    datapath: Option<&str>,
    ty: &str,
) -> Result<()> {
    let struct_ty = if let Some(d) = datapath {
        ArgType::Struct {
            name: ty.to_string(),
            params: vec![Box::new(ArgType::new_struct(d))],
        }
    } else {
        ArgType::new_struct(ty)
    };

    ////////////////////////////////////////////////////////////////////////////
    // <ty>_new_from_bytes
    compiler.add_extern_c_function(
        struct_ty.clone(),
        "new_from_bytes",
        None,
        vec![("buffer", ArgType::Buffer)],
        Some(struct_ty.clone()),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // <ty>_new
    if let Some(datapath) = datapath {
        compiler.add_extern_c_function(
            struct_ty.clone(),
            "new",
            None,
            vec![
                ("buffer", ArgType::Buffer),
                ("datapath", ArgType::Ref {
                    ty: Box::new(ArgType::new_struct(datapath)),
                }),
            ],
            Some(struct_ty.clone()),
            true,
        )?;
    }

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
    compiler.inner.add_context(Context::Function(func_context))?;
    let box_from_raw = format!(
        "Box::from_raw(self_ as *mut {})", &struct_ty.to_rust_str());
    compiler.inner.add_unsafe_def_with_let(false, None, "self_", &box_from_raw)?;
    // Note: The two different header types just have a different function name
    // to get a pointer to the bytes.
    compiler.inner.add_unsafe_set("return_ptr", match datapath {
        Some(_) => "self_.as_bytes().as_ptr()",
        None => "self_.bytes().as_ptr()",
    })?;
    compiler.inner.add_unsafe_set("return_len_ptr", "self_.len()")?;
    compiler.inner.add_func_call(None, "Box::into_raw",
        vec!["self_".to_string()], false)?;
    compiler.inner.pop_context()?; // end of function
    compiler.inner.add_newline()?;
    Ok(())
}

/// Note that the value passed around as a VariableList is actually a reference
/// &VariableList, so parsing the self_ argument would be a
/// *mut *const VariableList instead of just a *mut VariableList. We
/// additionally need to deref the Box<*const VariableList> as a
/// Box<&VariableList>. But careful using VariableList_init which returns an
/// actual VariableList... I think the C code could just pass a reference.
fn add_variable_list(
    compiler: &mut CDylibCompiler,
    param_ty: ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    let extern_prefix = match param_ty {
        ArgType::Primitive(ref string) => format!("VariableList_{}", string),
        ArgType::Struct { ref name, .. } => format!("VariableList_{}", name),
        _ => unimplemented!("unimplemented VariableList type"),
    };
    let struct_ty = {
        let mut params = vec![];
        params.push(Box::new(param_ty.clone()));
        if let Some(d) = datapath {
            params.push(Box::new(ArgType::new_struct(d)));
        }
        ArgType::Struct {
            name: "VariableList".to_string(),
            params,
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_init
    compiler.add_extern_c_function_with_name(
        &format!("{}_init", &extern_prefix),
        struct_ty.clone(),
        "init",
        None,
        vec![("num", ArgType::Primitive("usize".to_string()))],
        Some(struct_ty.clone()),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_append
    compiler.add_extern_c_function_with_name(
        &format!("{}_append", &extern_prefix),
        struct_ty.clone(),
        "append",
        Some(SelfArgType::Mut),
        vec![("val", param_ty.clone())],
        None,
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_len
    compiler.add_extern_c_function_with_name(
        &format!("{}_len", &extern_prefix),
        struct_ty.clone(),
        "len",
        Some(SelfArgType::Value),
        vec![],
        Some(ArgType::Primitive("usize".to_string())),
        false,
    )?;

    ////////////////////////////////////////////////////////////////////////////
    // VariableList_<param_ty>_index
    compiler.add_extern_c_function_with_name(
        &format!("{}_index", &extern_prefix),
        struct_ty.clone(),
        "index",
        Some(SelfArgType::Value),
        vec![("idx", ArgType::Primitive("usize".to_string()))],
        Some(ArgType::Ref { ty: Box::new(param_ty) }),
        false,
    )?;

    Ok(())
}

pub fn add_default_impl(
    compiler: &mut CDylibCompiler,
    msg_info: &MessageInfo,
    struct_ty: &ArgType,
) -> Result<()> {
    compiler.add_extern_c_function(
        struct_ty.clone(),
        "default",
        None,
        vec![],
        Some(ArgType::new_struct(&msg_info.get_name())),
        false,
    )?;
    Ok(())
}

pub fn add_impl(
    fd: &ProtoReprInfo,
    compiler: &mut CDylibCompiler,
    msg_info: &MessageInfo,
    struct_ty: &ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    compiler.add_extern_c_function(
        struct_ty.clone(),
        "new",
        None,
        vec![],
        Some(struct_ty.clone()),
        false,
    )?;

    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        add_field_methods(fd, compiler, &field_info, struct_ty, datapath)?;
    }
    Ok(())
}

fn add_field_methods(
    fd: &ProtoReprInfo,
    compiler: &mut CDylibCompiler,
    field: &FieldInfo,
    struct_ty: &ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    // add has_x, get_x, set_x
    add_has(compiler, field, struct_ty)?;
    add_get(fd, compiler, field, struct_ty, datapath)?;
    add_set(fd, compiler, field, struct_ty, datapath)?;

    // if field is a list or a nested struct, add get_mut_x
    if field.is_list() || field.is_nested_msg() {
        add_get_mut(fd, compiler, field, struct_ty, datapath)?;
    }

    // if field is list, add init_x
    if field.is_list() {
        add_list_init(compiler, field, struct_ty)?;
    }
    Ok(())
}

fn add_has(
    compiler: &mut CDylibCompiler,
    field: &FieldInfo,
    struct_ty: &ArgType,
) -> Result<()> {
    let func_name = format!("has_{}", field.get_name());
    compiler.add_extern_c_function(
        struct_ty.clone(),
        &func_name,
        Some(SelfArgType::Value),
        vec![],
        Some(ArgType::Primitive("bool".to_string())),
        false,
    )?;
    Ok(())
}

fn add_get(
    fd: &ProtoReprInfo,
    compiler: &mut CDylibCompiler,
    field: &FieldInfo,
    struct_ty: &ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    let return_type = {
        let ty = new_arg_type(fd, field, datapath)?;
        match ty {
            ArgType::Struct { ref name, .. } => {
                if name == "VariableList" {
                    ArgType::Ref { ty: Box::new(ty) }
                } else {
                    ty
                }
            }
            ty => ty,
        }
    };
    let func_name = format!("get_{}", field.get_name());
    compiler.add_extern_c_function(
        struct_ty.clone(),
        &func_name,
        Some(SelfArgType::Value),
        vec![],
        Some(return_type),
        false,
    )?;
    Ok(())
}

/// NOTE(GY): Is this FFI function necessary? I think the mutability of the
/// pointer gets erased when it's cased to a C pointer anyway.
fn add_get_mut(
    fd: &ProtoReprInfo,
    compiler: &mut CDylibCompiler,
    field: &FieldInfo,
    struct_ty: &ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    let return_type = {
        let ty = new_arg_type(fd, field, datapath)?;
        match ty {
            ArgType::Struct { ref name, .. } => {
                if name == "VariableList" {
                    ArgType::Ref { ty: Box::new(ty) }
                } else {
                    ty
                }
            }
            ty => ty,
        }
    };
    let func_name = format!("get_mut_{}", field.get_name());
    compiler.add_extern_c_function(
        struct_ty.clone(),
        &func_name,
        Some(SelfArgType::Mut),
        vec![],
        Some(return_type),
        false,
    )?;
    Ok(())
}

fn add_set(
    fd: &ProtoReprInfo,
    compiler: &mut CDylibCompiler,
    field: &FieldInfo,
    struct_ty: &ArgType,
    datapath: Option<&str>,
) -> Result<()> {
    let field_name = field.get_name();
    let field_type = new_arg_type(fd, field, datapath)?;
    let func_name = format!("set_{}", &field_name);
    compiler.add_extern_c_function(
        struct_ty.clone(),
        &func_name,
        Some(SelfArgType::Mut),
        vec![(&field_name, field_type)],
        None,
        false,
    )?;
    Ok(())
}

fn add_list_init(
    compiler: &mut CDylibCompiler,
    field: &FieldInfo,
    struct_ty: &ArgType,
) -> Result<()> {
    compiler.add_extern_c_function(
        struct_ty.clone(),
        &format!("init_{}", field.get_name()),
        Some(SelfArgType::Mut),
        vec![("num", ArgType::Primitive("usize".to_string()))],
        None,
        false,
    )?;
    Ok(())
}
