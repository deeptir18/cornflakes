use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    Context, FunctionArg, FunctionContext, ImplContext, SerializationCompiler, StructContext,
    StructDefContext,
};
use color_eyre::eyre::Result;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    add_dependencies(fd, compiler)?;
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        add_struct_definition(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_default_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_header_repr(fd, compiler, &msg_info)?;
    }

    Ok(())
}

fn add_header_repr(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    Ok(())
}

fn add_has(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    _msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let func_context = FunctionContext::new(
        &format!("has_{}", field.get_name()),
        true,
        vec![FunctionArg::SelfArg],
        "",
    );
    compiler.add_context(Context::Function(func_context))?;
    let bitmap_field_str = field.get_bitmap_idx_str(true);
    compiler.add_return_val(&format!("self.bitmap[{}] == 1", bitmap_field_str), false)?;

    compiler.pop_context()?;
    Ok(())
}

fn add_get(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    let field_type = fd.get_rust_type(field.clone())?;
    let return_type = match (field.is_list() || field.is_nested_msg()) {
        true => format!("&{}", field_type),
        false => field_type.to_string(),
    };
    let func_context = FunctionContext::new(
        &format!("get_{}", field.get_name()),
        true,
        vec![FunctionArg::SelfArg],
        &return_type,
    );
    compiler.add_context(Context::Function(func_context))?;
    let return_val = match field.is_list() || field.is_nested_msg() {
        true => format!("&self.{}", field.get_name()),
        false => field.get_name(),
    };

    compiler.add_return_val(&return_val, false)?;

    compiler.pop_context()?;
    Ok(())
}

fn add_set(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    Ok(())
}

fn add_get_mut(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    Ok(())
}

fn add_list_init(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    Ok(())
}

fn add_field_methods(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field: &FieldInfo,
) -> Result<()> {
    // add has_x, get_x, set_x
    add_has(fd, compiler, msg_info, field)?;
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
        add_list_init(fd, compiler, msg_info, field)?;
    }
    Ok(())
}

fn add_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let struct_lifetime = match msg_info.requires_lifetime(&fd.get_message_map())? {
        true => fd.get_lifetime(),
        false => "".to_string(),
    };
    let impl_context = ImplContext::new(&msg_info.get_name(), None, &struct_lifetime);
    compiler.add_context(Context::Impl(impl_context))?;
    // add constants at the top of the impl
    compiler.add_const_def(
        "BITMAP_SIZE",
        "usize",
        &format!("{}", msg_info.get_bitmap_size()),
    )?;

    for field in msg_info.get_fields().iter() {
        compiler.add_newline()?;
        let field_info = FieldInfo(field.clone());
        for (var, typ, def) in msg_info.get_constants(&field_info, false)?.iter() {
            compiler.add_const_def(var, typ, def)?;
        }
    }

    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        add_field_methods(fd, compiler, msg_info, &field_info)?;
    }

    compiler.pop_context()?;
    Ok(())
}

fn add_default_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let struct_lifetime = match msg_info.requires_lifetime(&fd.get_message_map())? {
        true => fd.get_lifetime(),
        false => "".to_string(),
    };
    let impl_context = ImplContext::new(
        &msg_info.get_name(),
        Some("Default".to_string()),
        &struct_lifetime,
    );
    compiler.add_context(Context::Impl(impl_context))?;
    let func_context = FunctionContext::new("default", false, Vec::default(), "Self");
    compiler.add_context(Context::Function(func_context))?;
    let struct_def_context = StructDefContext::new(&msg_info.get_name());
    compiler.add_context(Context::StructDef(struct_def_context))?;
    compiler.add_struct_def_field("bitmap", "vec![0u8; Self::BITMAP_SIZE]")?;
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        compiler.add_struct_def_field(&field_info.get_name(), &fd.get_default_type(field_info)?)?;
    }
    compiler.pop_context()?; // end of struct definition
    compiler.pop_context()?; // end of function
    compiler.pop_context()?;
    Ok(())
}

fn add_struct_definition(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let struct_ctx = StructContext::new(
        msg_info.get_name().as_str(),
        msg_info.derives_copy(&fd.get_message_map())?,
        fd.get_lifetime().as_str(),
    );

    // add struct header
    compiler.add_context(Context::Struct(struct_ctx))?;
    compiler.add_struct_field("bitmap", "Vec<u8>")?;
    // write in struct fields
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        compiler.add_struct_field(&field_info.get_name(), &fd.get_rust_type(field_info)?)?;
    }
    compiler.pop_context()?;
    Ok(())
}

fn add_dependencies(repr: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_codegen::utils::dynamic_hdr::*")?;
    compiler.add_dependency("cornflakes_libos::CornPtr")?;
    compiler.add_dependency("cornflakes_codegen::utils::dynamic_hdr::HeaderRepr")?;
    compiler.add_dependency("libc")?;
    compiler.add_dependency("std::slice")?;
    if repr.has_int_field() {
        compiler.add_dependency("byteorder::{ByteOrder, LittleEndian}")?;
    }
    Ok(())
}
