use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        ArgInfo, Context, FunctionArg, FunctionContext, SerializationCompiler,
        CArgInfo,
    },
};
use color_eyre::eyre::{bail, Result};
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

fn add_default_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_default", msg_info.get_name()),
        true,
        vec![FunctionArg::CArg(CArgInfo::ret_arg("*mut ::std::os::raw::c_void"))],
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?; // end of function
    Ok(())
}

fn add_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    compiler.add_newline()?;
    let new_func_context = FunctionContext::new_extern_c(
        &format!("{}_new", msg_info.get_name()),
        true,
        vec![FunctionArg::CArg(CArgInfo::ret_arg("*mut ::std::os::raw::c_void"))],
        false,
    );
    compiler.add_context(Context::Function(new_func_context))?;
    // let struct_def_context = StructDefContext::new(&msg_info.get_name());
    // compiler.add_context(Context::StructDef(struct_def_context))?;
    // compiler.add_struct_def_field("bitmap", "vec![0u8; Self::bitmap_length()]")?;
    // for field in msg_info.get_fields().iter() {
    //     let field_info = FieldInfo(field.clone());
    //     if field_info.is_list() {
    //         if field_info.is_int_list() {
    //             compiler.add_struct_def_field(&field_info.get_name(), "List::default()")?;
    //         } else {
    //             compiler.add_struct_def_field(&field_info.get_name(), "VariableList::default()")?;
    //         }
    //     } else {
    //         compiler
    //             .add_struct_def_field(&field_info.get_name(), &fd.get_default_type(field_info)?)?;
    //     }
    // }

    // compiler.pop_context()?; // end of struct definition
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?; // end of new function

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
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_has_{}", msg_info.get_name(), field.get_name()),
        true,
        vec![
            FunctionArg::CSelfArg,
            FunctionArg::CArg(CArgInfo::ret_arg("bool")),
        ],
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    // let bitmap_field_str = field.get_bitmap_idx_str(true);
    // compiler.add_return_val(
    //     &format!("self.get_bitmap_field({})", bitmap_field_str),
    //     false,
    // )?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;
    Ok(())
}

fn add_get(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let field_type = fd.get_c_type(field.clone())?;
    let mut args = vec![
        FunctionArg::CSelfArg,
        FunctionArg::CArg(CArgInfo::ret_arg(&field_type)),
    ];
    if is_array(field) {
        args.push(FunctionArg::CArg(CArgInfo::ret_len_arg()));
    }

    let func_context = FunctionContext::new_extern_c(
        &format!("{}_get_{}", msg_info.get_name(), field.get_name()),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    // let return_val = match field.is_list() || field.is_nested_msg() {
    //     true => format!("&self.{}", field.get_name()),
    //     false => format!("self.{}", field.get_name()),
    // };

    // compiler.add_return_val(&return_val, false)?;
    compiler.add_line("unimplemented!()")?;

    compiler.pop_context()?;
    Ok(())
}

fn add_get_mut(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let field_name = field.get_name();
    let rust_type = fd.get_rust_type(field.clone())?;
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_get_mut_{}", msg_info.get_name(), &field_name),
        true,
        vec![FunctionArg::MutSelfArg],
        // &format!("&mut {}", rust_type)
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_return_val(&format!("&mut self.{}", &field_name), false)?;
    compiler.pop_context()?;
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
    let mut args = vec![
        FunctionArg::CSelfArg,
        FunctionArg::CArg(CArgInfo::arg(&field_name, &field_type)),
    ];
    if is_array(field) {
        args.push(FunctionArg::CArg(CArgInfo::len_arg(&field_name)));
    }
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_set_{}", msg_info.get_name(), field_name),
        true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    // compiler.add_func_call(
    //     Some("self".to_string()),
    //     "set_bitmap_field",
    //     vec![format!("{}", bitmap_idx_str)],
    //     false,
    // )?;
    // compiler.add_statement(&format!("self.{}", &field_name), "field")?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;
    Ok(())
}

fn add_list_init(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let func_context = FunctionContext::new_extern_c(
        &format!("{}_init_{}", msg_info.get_name(), field.get_name()),
        true,
        vec![
            FunctionArg::MutSelfArg,
            FunctionArg::new_arg("num", ArgInfo::owned("usize")),
        ],
        false,
    );
    compiler.add_context(Context::Function(func_context))?;
    match &field.0.typ {
        FieldType::Int32
        | FieldType::Int64
        | FieldType::Uint32
        | FieldType::Uint64
        | FieldType::Float => {
            compiler.add_statement(
                &format!("self.{}", field.get_name()),
                &format!("List::init(num)"),
            )?;
        }
        FieldType::String
        | FieldType::Bytes
        | FieldType::RefCountedString
        | FieldType::RefCountedBytes => {
            compiler.add_statement(
                &format!("self.{}", field.get_name()),
                &format!("VariableList::init(num)"),
            )?;
        }
        FieldType::MessageOrEnum(_) => {
            compiler.add_statement(
                &format!("self.{}", field.get_name()),
                &format!("VariableList::init(num)"),
            )?;
        }
        x => {
            bail!("Field type not supported: {:?}", x);
        }
    }
    compiler.add_func_call(
        Some("self".to_string()),
        "set_bitmap_field",
        vec![format!("{}", field.get_bitmap_idx_str(true))],
        false,
    )?;
    compiler.pop_context()?;
    unimplemented!()
}

fn add_header_repr(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // add dynamic header size function
    let header_size_function_context = FunctionContext::new_extern_c(
        &format!("{}_dynamic_header_size", msg_info.get_name()),
        true,
        vec![
            FunctionArg::CSelfArg,
            FunctionArg::CArg(CArgInfo::ret_arg("usize")),
        ],
        false,
    );
    compiler.add_context(Context::Function(header_size_function_context))?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;
    compiler.add_newline()?;

    // add dynamic header offset function
    let header_offset_function_context = FunctionContext::new_extern_c(
        &format!("{}_dynamic_header_start", msg_info.get_name()),
        true,
        vec![
            FunctionArg::CSelfArg,
            FunctionArg::CArg(CArgInfo::ret_arg("usize")),
        ],
        false,
    );
    compiler.add_context(Context::Function(header_offset_function_context))?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;
    compiler.add_newline()?;

    // add num scatter_gather_entries function
    let num_sge_function_context = FunctionContext::new_extern_c(
        &format!("{}_num_scatter_gather_entries", msg_info.get_name()),
        true,
        vec![
            FunctionArg::CSelfArg,
            FunctionArg::CArg(CArgInfo::ret_arg("usize")),
        ],
        false,
    );
    compiler.add_context(Context::Function(num_sge_function_context))?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;

    Ok(())
}

// See: cornflakes-codegen/src/utils/dynamic_sga_hdr.rs
fn add_shared_header_repr(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // add deserialize function
    let deserialize_function_context = FunctionContext::new_extern_c(
        &format!("{}_deserialize", msg_info.get_name()),
        true,
        vec![
            FunctionArg::CSelfArg,
            FunctionArg::CArg(CArgInfo::arg("buffer", "*const ::std::os::raw::c_uchar")),
            FunctionArg::CArg(CArgInfo::len_arg("buffer")),
        ],
        true,
    );
    compiler.add_context(Context::Function(deserialize_function_context))?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;
    compiler.add_newline()?;

    // add serialize_into_sga function
    let serialize_into_sga_function_context = FunctionContext::new_extern_c(
        &format!("{}_serialize_into_sga", msg_info.get_name()),
        true,
        vec![
            FunctionArg::CSelfArg,
            FunctionArg::CArg(CArgInfo::arg("ordered_sga", "*const ::std::os::raw::c_void")),
            FunctionArg::CArg(CArgInfo::arg("datapath", "*const ::std::os::raw::c_void")),
        ],
        true,
    );
    compiler.add_context(Context::Function(serialize_into_sga_function_context))?;
    compiler.add_line("unimplemented!()")?;
    compiler.pop_context()?;
    compiler.add_newline()?;

    Ok(())
}
