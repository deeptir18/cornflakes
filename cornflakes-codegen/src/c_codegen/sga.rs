use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
    },
};
use color_eyre::eyre::Result;
use protobuf_parser::FieldType;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    add_dependencies(fd, compiler)?;
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        add_default_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_header_repr(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_shared_header_repr(fd, compiler, &msg_info)?;
        break;
    }
    Ok(())
}

fn add_dependencies(repr: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    // compiler.add_dependency("cornflakes_libos::Sge")?;
    // compiler.add_dependency("color_eyre::eyre::Result")?;
    // compiler.add_dependency("cornflakes_codegen::utils::dynamic_sga_hdr::*")?;
    // compiler.add_dependency("cornflakes_codegen::utils::dynamic_sga_hdr::{SgaHeaderRepr}")?;

    // // if any message has integers, we need slice
    // if repr.has_int_field() {
    //     compiler.add_dependency("std::{slice}")?;
    // }
    Ok(())
}

fn is_array(field: &FieldInfo) -> bool {
    match &field.0.typ {
        FieldType::Bytes | FieldType::RefCountedBytes => true,
        _ => false,
    }
}

fn add_extern_c_wrapper_function(
    compiler: &mut SerializationCompiler,
    struct_name: &str,
    func_name: &str,
    use_self: bool,
    raw_args: Vec<(&str, &str, bool)>,  // name, type, is_array
    raw_ret: Option<(&str, bool)>,      //       type, is_array
    use_error_code: bool,
) -> Result<()> {
    let args = {
        let mut args = vec![];
        if use_self {
            args.push(FunctionArg::CSelfArg);
        }
        for (arg_name, arg_ty, arg_is_array) in raw_args {
            args.push(FunctionArg::CArg(CArgInfo::arg(arg_name, arg_ty)));
            if arg_is_array {
                args.push(FunctionArg::CArg(CArgInfo::len_arg(arg_name)));
            }
        }
        if let Some((ret_ty, ret_is_array)) = raw_ret {
            args.push(FunctionArg::CArg(CArgInfo::ret_arg(ret_ty)));
            if ret_is_array {
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
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?; // end of function

    compiler.add_newline()?;
    Ok(())
}

fn add_default_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "default",
        false,
        vec![],
        Some(("*mut ::std::os::raw::c_void", false)),
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
        false,
        vec![],
        Some(("*mut ::std::os::raw::c_void", false)),
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
        true,
        vec![],
        Some(("bool", false)),
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
    let field_type = fd.get_c_type(field.clone())?;
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        &format!("get_{}", field.get_name()),
        true,
        vec![],
        Some((&field_type, is_array(field))),
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
    let field_type = fd.get_c_type(field.clone())?;
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        &format!("set_{}", field.get_name()),
        true,
        vec![(&field_name, &field_type, is_array(field))],
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
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // add dynamic header size function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "dynamic_header_size",
        true,
        vec![],
        Some(("usize", false)),
        false,
    )?;

    // add dynamic header offset function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "dynamic_header_offset",
        true,
        vec![],
        Some(("usize", false)),
        false,
    )?;

    // add num scatter_gather_entries function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "num_scatter_gather_entries",
        true,
        vec![],
        Some(("usize", false)),
        false,
    )?;

    Ok(())
}

// See: cornflakes-codegen/src/utils/dynamic_sga_hdr.rs
fn add_shared_header_repr(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // add deserialize function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "deserialize",
        true,
        vec![("buffer", "*const ::std::os::raw::c_uchar", true)],
        None,
        true,
    )?;

    // add serialize_into_sga function
    add_extern_c_wrapper_function(
        compiler,
        &msg_info.get_name(),
        "serialize_into_sga",
        true,
        vec![
            ("ordered_sga", "*const ::std::os::raw::c_void", false),
            ("datapath", "*const ::std::os::raw::c_void", false),
        ],
        None,
        true,
    )?;

    Ok(())
}
