use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    ArgInfo, Context, FunctionArg, FunctionContext, ImplContext, LoopBranch, LoopContext,
    SerializationCompiler, StructContext, StructDefContext, StructName, TraitName, UnsafeContext,
};
use color_eyre::eyre::{bail, Result};
use protobuf_parser::FieldType;

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

fn add_deserialization_func(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let func_context = FunctionContext::new(
        "inner_deserialize",
        false,
        vec![
            FunctionArg::MutSelfArg,
            FunctionArg::new_arg("buf", ArgInfo::owned("*const u8")),
            FunctionArg::new_arg("size", ArgInfo::owned("usize")),
            FunctionArg::new_arg("relative_offset", ArgInfo::owned("usize")),
        ],
        "",
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_line("assert!(size >= Self::BITMAP_SIZE);")?;
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "bitmap_slice",
        "slice::from_raw_parts(buf, Self::BITMAP_SIZE)",
    )?;
    compiler.add_newline()?;

    let cur_header_ptr_mut = msg_info.num_fields() > 1;
    compiler.add_unsafe_def_with_let(
        cur_header_ptr_mut,
        None,
        "cur_header_ptr",
        "buf.offset(Self::BITMAP_SIZE as isize)",
    )?;
    let (has_cur_header_offset, cur_header_offset_mut) = {
        if !msg_info.string_or_bytes_fields_left(-1)? {
            (false, false)
        } else {
            // if the LAST string or bytes field is past position 0
            let mut last_idx = 0;
            for idx in 0..msg_info.num_fields() {
                let field_info = msg_info.get_field_from_id(idx as i32)?;
                if field_info.is_bytes_or_string() {
                    last_idx = idx;
                }
            }
            (true, last_idx > 0)
        }
    };
    if has_cur_header_offset {
        compiler.add_def_with_let(
            cur_header_offset_mut,
            None,
            "cur_header_offset",
            "Self::BITMAP_SIZE",
        )?;
    }
    compiler.add_newline()?;

    // iterate over fields adn add field deserialization for each field
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        let loop_context = LoopContext::new(vec![LoopBranch::ifbranch(&format!(
            "bitmap_slice[{}] == 1",
            field_info.get_bitmap_idx_str(true)
        ))]);
        compiler.add_context(Context::Loop(loop_context))?;
        add_field_deserialization(fd, compiler, msg_info, &field_info)?;
        compiler.pop_context()?;
    }
    compiler.pop_context()?;
    Ok(())
}

// deserializes a field and stores the result in SELF
fn add_field_deserialization(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field_info: &FieldInfo,
) -> Result<()> {
    compiler.add_statement(
        &format!("self.bitmap[{}]", field_info.get_bitmap_idx_str(true)),
        "1",
    )?;
    if field_info.is_list() {
        match &field_info.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint64
            | FieldType::Uint32
            | FieldType::Float
            | FieldType::String
            | FieldType::Bytes
            | FieldType::MessageOrEnum(_) => {
                compiler.add_def_with_let(false, None, "list_ref", "ObjectRef(cur_header_ptr)")?;
                compiler.add_func_call(
                    Some(format!("self.{}", field_info.get_name())),
                    "inner_deserialize",
                    vec![
                        "unsafe { buf.offset((list_ref.get_offset() - relative_offset) as isize) }"
                            .to_string(),
                        "list_ref.get_size()".to_string(),
                        "list_ref.get_offset()".to_string(),
                    ],
                    false,
                )?;
            }
            _ => {
                bail!("Field type {:?} not supported.", field_info.0.typ);
            }
        }
    } else {
        let rust_type = fd.get_rust_type(field_info.clone())?;
        match &field_info.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint64
            | FieldType::Uint32
            | FieldType::Float => {
                compiler.add_statement(&format!("self.{}", field_info.get_name()), &format!("LittleEndian::read_{}( unsafe {{ slice::from_raw_parts(cur_header_ptr, {}) }} )", rust_type, field_info.get_header_size_str(true, false)?) )?;
            }
            FieldType::String | FieldType::Bytes => {
                compiler.add_func_call(
                    Some(format!("self.{}", field_info.get_name())),
                    "inner_deserialize",
                    vec![
                        "cur_header_ptr".to_string(),
                        format!("{}", field_info.get_header_size_str(true, false)?),
                        "relative_offset + cur_header_offset".to_string(),
                    ],
                    false,
                )?;
            }
            FieldType::MessageOrEnum(_) => {
                compiler.add_def_with_let(
                    false,
                    None,
                    "object_ref",
                    "ObjectRef(cur_header_ptr)",
                )?;
                compiler.add_func_call(
                    Some(format!("self.{}", field_info.get_name())),
                    "inner_deserialize",
                    vec![
                        "unsafe { buf.offset((object_ref.get_offset() - relative_offset) as isize) }"
                            .to_string(),
                        "object_ref.get_size()".to_string(),
                        "object_ref.get_offset()".to_string(),
                    ],
                    false
                )?;
            }
            _ => {
                bail!("Field type {:?} not supported.", field_info.0.typ);
            }
        }
    }
    if msg_info.constant_fields_left(field_info.get_idx()) > 0 {
        compiler.add_unsafe_statement(
            "cur_header_ptr",
            &format!(
                "cur_header_ptr.offset({} as isize)",
                field_info.get_header_size_str(true, false)?
            ),
        )?;
        if msg_info.string_or_bytes_fields_left(field_info.get_idx())?
            || msg_info.int_fields_left(field_info.get_idx())?
        {
            compiler.add_plus_equals(
                "cur_header_offset",
                &field_info.get_header_size_str(true, false)?,
            )?;
        }
    }
    Ok(())
}

fn add_serialization_func(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // only uses offset argument if there are any dynamic arguments
    let offset_str = match msg_info.dynamic_fields_left(-1, true, fd.get_message_map())? > 0 {
        true => "offset",
        false => "_offset",
    };
    let func_context = FunctionContext::new_with_lifetime(
        "inner_serialize",
        false,
        vec![
        FunctionArg::SelfArg,
            FunctionArg::new_arg("header_ptr", ArgInfo::owned("*mut u8")),
            FunctionArg::new_arg("copy_func", ArgInfo::owned("unsafe fn (dst: *mut ::std::os::raw::c_void, src: *const ::std::os::raw::c_void, len: usize,)")),
            FunctionArg::new_arg(offset_str, ArgInfo::owned("usize")),
        ],
        "Vec<(CornPtr<'registered, 'normal>, *mut u8)>",
        "'normal",
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_def_with_let(
        true,
        Some(format!(
            "Vec<(CornPtr<{}, 'normal>, *mut u8)>",
            fd.get_lifetime()
        )),
        "ret",
        "Vec::default()",
    )?;

    // copy in bitmap to the head of the object
    compiler.add_context(Context::Unsafe(UnsafeContext::new()))?;
    compiler.add_func_call(
        None,
        "copy_func",
        vec![
            "header_ptr as _".to_string(),
            "self.bitmap.as_ptr() as _".to_string(),
            "Self::BITMAP_SIZE".to_string(),
        ],
        false,
    )?;
    // end of unsafe block
    compiler.pop_context()?;

    // define variables to use while serializing
    let header_off_mut = msg_info.constant_fields_left(-1) > 1;
    compiler.add_unsafe_def_with_let(
        header_off_mut,
        None,
        "cur_header_ptr",
        "header_ptr.offset(Self::BITMAP_SIZE as isize)",
    )?;

    let has_dynamic_fields = msg_info.has_dynamic_fields(true, fd.get_message_map())?;
    if has_dynamic_fields {
        let dynamic_fields_off_mut =
            msg_info.dynamic_fields_left(-1, true, fd.get_message_map())? > 1;
        compiler.add_unsafe_def_with_let(
            dynamic_fields_off_mut,
            None,
            "cur_dynamic_ptr",
            "header_ptr.offset(self.dynamic_header_offset() as isize)",
        )?;
        compiler.add_def_with_let(
            dynamic_fields_off_mut,
            None,
            "cur_dynamic_offset",
            "offset + self.dynamic_header_offset()",
        )?;
    }

    for field_idx in 0..msg_info.num_fields() {
        let field_info = msg_info.get_field_from_id(field_idx as i32)?;
        compiler.add_newline()?;
        let loop_context = LoopContext::new(vec![LoopBranch::ifbranch(&format!(
            "self.has_{}()",
            &field_info.get_name()
        ))]);
        compiler.add_context(Context::Loop(loop_context))?;
        add_serialization_for_field(fd, compiler, msg_info, &field_info)?;

        compiler.pop_context()?;
    }

    compiler.add_newline()?;
    compiler.add_return_val("ret", false)?;

    compiler.pop_context()?;
    Ok(())
}

fn add_serialization_for_field(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field_info: &FieldInfo,
) -> Result<()> {
    if field_info.is_list() {
        match &field_info.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float
            | FieldType::String
            | FieldType::Bytes
            | FieldType::MessageOrEnum(_) => {
                compiler.add_def_with_let(
                    true,
                    None,
                    "list_field_ref",
                    "ObjectRef(cur_header_ptr as _)",
                )?;
                compiler.add_func_call(
                    Some("list_field_ref".to_string()),
                    "write_size",
                    vec![format!("self.{}.len()", &field_info.get_name())],
                    false,
                )?;
                compiler.add_func_call(
                    Some("list_field_ref".to_string()),
                    "write_offset",
                    vec!["cur_dynamic_offset".to_string()],
                    false,
                )?;
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!(
                        "&mut self.{}.inner_serialize(cur_dynamic_ptr, copy_func, cur_dynamic_offset)",
                        field_info.get_name()
                    )],
                    false
                )?;
            }
            _ => {
                bail!("Field type not supported: {:?}", &field_info.0.typ);
            }
        }
    } else {
        match &field_info.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float => {
                let rust_type = &field_info.get_base_type_str()?;
                let field_size = &field_info.get_header_size_str(true, false)?;
                compiler.add_line(&format!("LittleEndian::write_{}( unsafe {{ slice::from_raw_parts_mut(cur_header_ptr as _, {}) }}, self.{});", rust_type, field_size, &field_info.get_name()))?;
            }
            FieldType::String | FieldType::Bytes => {
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!(
                        "& mut self.{}.inner_serialize(cur_header_ptr, copy_func, 0)",
                        &field_info.get_name()
                    )],
                    false,
                )?;
            }
            FieldType::MessageOrEnum(_) => {
                compiler.add_def_with_let(
                    true,
                    None,
                    "nested_field_ref",
                    "ObjectRef(cur_header_ptr as _)",
                )?;
                compiler.add_func_call(
                    Some("nested_field_ref".to_string()),
                    "write_size",
                    vec![format!(
                        "self.{}.dynamic_header_size()",
                        &field_info.get_name()
                    )],
                    false,
                )?;
                compiler.add_func_call(
                    Some("nested_field_ref".to_string()),
                    "write_offset",
                    vec![format!("cur_dynamic_offset")],
                    false,
                )?;
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!(
                        "&mut self.{}.inner_serialize(cur_dynamic_ptr, copy_func, cur_dynamic_offset)",
                        &field_info.get_name()
                    )],
                    false
                )?;
            }
            _ => {
                bail!("Field type not supported: {:?}", &field_info.0.typ);
            }
        }
    }
    compiler.add_newline()?;
    if msg_info.constant_fields_left(field_info.get_idx()) > 0 {
        let field_size = &field_info.get_header_size_str(true, false)?;
        compiler.add_unsafe_statement(
            "cur_header_ptr",
            &format!("cur_header_ptr.offset({} as isize)", field_size),
        )?;
    }

    if msg_info.dynamic_fields_left(field_info.get_idx(), true, fd.get_message_map())? > 0 {
        if field_info.is_dynamic(true, fd.get_message_map())? {
            // modify cur_dynamic_ptr and cur_dynamic_offset
            compiler.add_unsafe_statement(
                "cur_dynamic_ptr",
                &format!(
                    "cur_dynamic_ptr.offset(self.{}.dynamic_header_size() as isize)",
                    &field_info.get_name()
                ),
            )?;
            compiler.add_plus_equals(
                "cur_dynamic_offset",
                &format!("self.{}.dynamic_header_size()", &field_info.get_name()),
            )?;
        }
    }

    Ok(())
}

fn add_header_repr(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let type_annotations = msg_info.get_type_params(false, &fd)?;
    let where_clause = msg_info.get_where_clause(false, &fd)?;
    let struct_name = StructName::new(&msg_info.get_name(), type_annotations.clone());
    let trait_name = TraitName::new("HeaderRepr", type_annotations.clone());
    let impl_context = ImplContext::new(struct_name, Some(trait_name), where_clause);
    compiler.add_context(Context::Impl(impl_context))?;
    // add constant header size
    compiler.add_const_def("CONSTANT_HEADER_SIZE", "usize", "SIZE_FIELD + OFFSET_FIELD")?;
    compiler.add_newline()?;

    // add dynamic header size function
    let header_size_function_context = FunctionContext::new(
        "dynamic_header_size",
        false,
        vec![FunctionArg::SelfArg],
        "usize",
    );
    compiler.add_context(Context::Function(header_size_function_context))?;
    let mut dynamic_size = "Self::BITMAP_SIZE".to_string();
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        dynamic_size = format!(
            "{} + self.bitmap[{}] as usize * {}",
            dynamic_size,
            &field_info.get_bitmap_idx_str(true),
            &field_info.get_total_header_size_str(true, false, false)?
        );
    }
    compiler.add_return_val(&dynamic_size, false)?;
    compiler.pop_context()?;
    compiler.add_newline()?;

    // add dynamic header offset function
    let header_offset_function_context = FunctionContext::new(
        "dynamic_header_offset",
        false,
        vec![FunctionArg::SelfArg],
        "usize",
    );
    compiler.add_context(Context::Function(header_offset_function_context))?;
    let mut dynamic_offset = "Self::BITMAP_SIZE".to_string();
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        dynamic_offset = format!(
            "{} + self.bitmap[{}] as usize * {}",
            dynamic_offset,
            &field_info.get_bitmap_idx_str(true),
            &field_info.get_header_size_str(true, false)?
        );
    }
    compiler.add_return_val(&dynamic_offset, false)?;
    compiler.pop_context()?;
    compiler.add_newline()?;

    add_serialization_func(fd, compiler, msg_info)?;
    compiler.add_newline()?;

    add_deserialization_func(fd, compiler, msg_info)?;
    compiler.add_newline()?;

    compiler.pop_context()?;
    Ok(())
}

fn add_has(compiler: &mut SerializationCompiler, field: &FieldInfo) -> Result<()> {
    let func_context = FunctionContext::new(
        &format!("has_{}", field.get_name()),
        true,
        vec![FunctionArg::SelfArg],
        "bool",
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
    field: &FieldInfo,
) -> Result<()> {
    let field_type = fd.get_rust_type(field.clone())?;
    let return_type = match field.is_list() || field.is_nested_msg() {
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
        false => format!("self.{}", field.get_name()),
    };

    compiler.add_return_val(&return_val, false)?;

    compiler.pop_context()?;
    Ok(())
}

fn add_set(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    field: &FieldInfo,
) -> Result<()> {
    let field_name = field.get_name();
    let bitmap_idx_str = field.get_bitmap_idx_str(true);
    let rust_type = fd.get_rust_type(field.clone())?;
    let func_context = FunctionContext::new(
        &format!("set_{}", field_name),
        true,
        vec![
            FunctionArg::MutSelfArg,
            FunctionArg::new_arg("field", ArgInfo::owned(&rust_type)),
        ],
        "",
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_statement(&format!("self.bitmap[{}]", bitmap_idx_str), "1")?;
    compiler.add_statement(&format!("self.{}", &field_name), "field")?;
    compiler.pop_context()?;
    Ok(())
}

fn add_get_mut(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    field: &FieldInfo,
) -> Result<()> {
    let field_name = field.get_name();
    let rust_type = fd.get_rust_type(field.clone())?;
    let func_context = FunctionContext::new(
        &format!("get_mut_{}", &field_name),
        true,
        vec![FunctionArg::MutSelfArg],
        &format!("&mut {}", rust_type),
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_return_val(&format!("&mut self.{}", &field_name), false)?;
    compiler.pop_context()?;
    Ok(())
}

fn add_list_init(compiler: &mut SerializationCompiler, field: &FieldInfo) -> Result<()> {
    let func_context = FunctionContext::new(
        &format!("init_{}", field.get_name()),
        true,
        vec![
            FunctionArg::MutSelfArg,
            FunctionArg::new_arg("num", ArgInfo::owned("usize")),
        ],
        "",
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
        FieldType::String | FieldType::Bytes => {
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
    compiler.add_statement(
        &format!("self.bitmap[{}]", field.get_bitmap_idx_str(true)),
        "1",
    )?;
    compiler.pop_context()?;
    Ok(())
}

fn add_field_methods(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    field: &FieldInfo,
) -> Result<()> {
    // add has_x, get_x, set_x
    compiler.add_newline()?;
    add_has(compiler, field)?;
    compiler.add_newline()?;
    add_get(fd, compiler, field)?;
    compiler.add_newline()?;
    add_set(fd, compiler, field)?;
    compiler.add_newline()?;

    // if field is a list or a nested struct, add get_mut_x
    if field.is_list() || field.is_nested_msg() {
        add_get_mut(fd, compiler, field)?;
    }

    // if field is list, add init_x
    if field.is_list() {
        add_list_init(compiler, field)?;
    }
    Ok(())
}

fn add_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let type_annotations = msg_info.get_type_params(false, &fd)?;
    let where_clause = msg_info.get_where_clause(false, &fd)?;
    let impl_context = ImplContext::new(
        StructName::new(&msg_info.get_name(), type_annotations.clone()),
        None,
        where_clause,
    );

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
        for (var, typ, def) in msg_info.get_constants(&field_info, false, false)?.iter() {
            compiler.add_const_def(var, typ, def)?;
        }
    }

    compiler.add_newline()?;
    let new_func_context = FunctionContext::new("new", true, vec![], "Self");
    compiler.add_context(Context::Function(new_func_context))?;
    let struct_def_context = StructDefContext::new(&msg_info.get_name());
    compiler.add_context(Context::StructDef(struct_def_context))?;
    compiler.add_struct_def_field("bitmap", "[0u8; Self::BITMAP_SIZE]")?;
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        if field_info.is_list() {
            if field_info.is_int_list() {
                compiler.add_struct_def_field(&field_info.get_name(), "List::default()")?;
            } else {
                compiler.add_struct_def_field(&field_info.get_name(), "VariableList::default()")?;
            }
        } else {
            compiler
                .add_struct_def_field(&field_info.get_name(), &fd.get_default_type(field_info)?)?;
        }
    }

    compiler.pop_context()?; // end of struct definition
    compiler.pop_context()?; // end of new function

    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        add_field_methods(fd, compiler, &field_info)?;
    }

    compiler.pop_context()?;
    Ok(())
}

fn add_default_impl(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let type_annotations = msg_info.get_type_params(false, &fd)?;
    let where_clause = msg_info.get_where_clause(false, &fd)?;
    let struct_name = StructName::new(&msg_info.get_name(), type_annotations.clone());
    let trait_name = TraitName::new("Default", vec![]);
    let impl_context = ImplContext::new(struct_name, Some(trait_name), where_clause);
    compiler.add_context(Context::Impl(impl_context))?;
    let func_context = FunctionContext::new("default", false, Vec::default(), "Self");
    compiler.add_context(Context::Function(func_context))?;
    let struct_def_context = StructDefContext::new(&msg_info.get_name());
    compiler.add_context(Context::StructDef(struct_def_context))?;
    compiler.add_struct_def_field("bitmap", "[0u8; Self::BITMAP_SIZE]")?;
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        if field_info.is_list() {
            if field_info.is_int_list() {
                compiler.add_struct_def_field(&field_info.get_name(), "List::default()")?;
            } else {
                compiler.add_struct_def_field(&field_info.get_name(), "VariableList::default()")?;
            }
        } else {
            compiler
                .add_struct_def_field(&field_info.get_name(), &fd.get_default_type(field_info)?)?;
        }
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
    let type_annotations = msg_info.get_type_params(false, &fd)?;
    let where_clause = msg_info.get_where_clause(false, &fd)?;
    let struct_name = StructName::new(&msg_info.get_name(), type_annotations.clone());
    let struct_ctx = StructContext::new(
        struct_name,
        msg_info.derives_copy(&fd.get_message_map(), false)?,
        where_clause,
    );

    // add struct header
    compiler.add_context(Context::Struct(struct_ctx))?;
    compiler.add_struct_field("bitmap", &format!("[u8; {}]", msg_info.get_bitmap_size()))?;
    // write in struct fields
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        compiler.add_struct_field(&field_info.get_name(), &fd.get_rust_type(field_info)?)?;
    }
    compiler.pop_context()?;
    Ok(())
}

fn add_dependencies(repr: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_codegen::utils::{ObjectRef, dynamic_hdr::*}")?;
    compiler.add_dependency("cornflakes_libos::CornPtr")?;
    compiler.add_dependency("cornflakes_codegen::utils::dynamic_hdr::HeaderRepr")?;
    compiler.add_dependency("std::slice")?;
    if repr.has_int_field() {
        compiler.add_dependency("byteorder::{ByteOrder, LittleEndian}")?;
    }
    Ok(())
}
