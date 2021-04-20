use super::{
    super::header_utils::{FieldInfo, MessageInfo, ProtoReprInfo},
    ArgInfo, Context, FunctionArg, FunctionContext, ImplContext, LoopBranch, LoopContext,
    MatchContext, SerializationCompiler, StructContext, StructDefContext, UnsafeContext,
};
use color_eyre::eyre::{bail, Result};
use protobuf_parser::FieldType;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    add_dependencies(fd, compiler)?;
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        compiler.add_newline()?;
        add_struct_definition(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_default_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_impl(fd, compiler, &msg_info)?;
        add_header_repr(fd, compiler, &msg_info)?;
    }

    Ok(())
}

fn add_deserialization_func(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    _msg_info: &MessageInfo,
) -> Result<()> {
    let func_context = FunctionContext::new(
        "inner_deserialize",
        false,
        vec![
            FunctionArg::MutSelfArg,
            FunctionArg::new_arg("ref_buf", ArgInfo::ref_arg("[u8]", Some(fd.get_lifetime()))),
            FunctionArg::new_arg("constant_header_offset", ArgInfo::owned("usize")),
        ],
        "",
    );
    compiler.add_context(Context::Function(func_context))?;
    // constant time deserialization: just set header pointer
    compiler.add_statement("self.header_ptr", "ref_buf")?;
    compiler.add_statement("self.has_header_ptr", "true")?;
    compiler.add_statement("self.offset", "constant_header_offset")?;

    compiler.pop_context()?;
    Ok(())
}

fn add_serialization_func(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    let func_context = FunctionContext::new_with_lifetime(
        "inner_serialize",
        false,
        vec![
            FunctionArg::SelfArg,
            FunctionArg::new_arg("constant_header_ptr", ArgInfo::owned("*mut u8")),
            FunctionArg::new_arg("dynamic_header_ptr", ArgInfo::owned("*mut u8")),
            FunctionArg::new_arg("constant_header_offset", ArgInfo::owned("usize")),
            FunctionArg::new_arg("dynamic_header_offset", ArgInfo::owned("usize")),
            FunctionArg::new_arg("copy_func", ArgInfo::owned("unsafe fn (dst: *mut ::std::os::raw::c_void, src: *const ::std::os::raw::c_void, len: usize,)"))
        ],
        "Vec<(CornPtr<'registered, 'normal>, *mut u8)>",
        "normal",
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_def_with_let(
        true,
        Some(format!(
            "Vec<(CornPtr<'{}, 'normal>, *mut u8)>",
            fd.get_lifetime()
        )),
        "ret",
        "Vec::default()",
    )?;

    // copy in bitmap to the head of the object
    let match_context = MatchContext::new_with_def(
        "self.has_header_ptr",
        vec!["true".to_string(), "false".to_string()],
        "final_bitmap",
    );
    compiler.add_context(Context::Match(match_context))?;
    // true: if self.has_header_ptr
    compiler.add_def_with_let(
        false,
        None,
        "ref_header_bitmap_slice",
        &format!("&self.header_ptr[self.offset..(self.offset + Self::BITMAP_SIZE)]"),
    )?;
    compiler.add_def_with_let(false, None, "bitmap_slice", "&self.bitmap")?;
    compiler.add_def_with_let(
        false,
        Some("Vec<u8>".to_string()),
        "final_bitmap",
        "ref_header_bitmap_slice.iter().zip(bitmap_slice.iter()).map(|(a, b)| a | b).collect()",
    )?;
    compiler.add_return_val("final_bitmap", false)?;
    // false: if !self.has_header_ptr
    compiler.pop_context()?;
    compiler.add_def_with_let(
        false,
        Some("Vec<u8>".to_string()),
        "final_bitmap",
        "self.bitmap.iter().map(|x| *x).collect()",
    )?;
    compiler.add_return_val("final_bitmap", false)?;
    // end of match
    compiler.pop_context()?;

    // add unsafe block to copy the bitmap
    compiler.add_context(Context::Unsafe(UnsafeContext::new()))?;
    compiler.add_func_call(
        None,
        "copy_func",
        vec![
            "constant_header_ptr as _".to_string(),
            "final_bitmap.as_ptr() as _".to_string(),
            "Self::BITMAP_SIZE".to_string(),
        ],
    )?;
    // end of unsafe block
    compiler.pop_context()?;

    // define variables to use while serializing
    let has_dynamic_fields = msg_info.has_dynamic_fields(false, fd.get_message_map())?;
    if has_dynamic_fields {
        let dynamic_fields_off_mut =
            msg_info.dynamic_fields_left(-1, false, fd.get_message_map())? > 1;
        compiler.add_def_with_let(
            dynamic_fields_off_mut,
            None,
            "cur_dynamic_ptr",
            "dynamic_header_ptr",
        )?;
        compiler.add_def_with_let(
            dynamic_fields_off_mut,
            None,
            "cur_dynamic_offset",
            "dynamic_header_offset",
        )?;
    }

    // TODO: optimization where if nothing is dirty (entire bitmap is filled with 0's), provide
    // 1 scatter-gather array pointing to the original input buffer
    // But this might not be a "valid" feature for the echo-server benchmark

    for field_idx in 0..msg_info.num_fields() {
        let field_info = msg_info.get_field_from_id(field_idx as i32)?;
        compiler.add_newline()?;
        let loop_context = LoopContext::new(vec![LoopBranch::ifbranch(&format!(
            "self.has_{}()",
            &field_info.get_name()
        ))]);
        add_serialization_for_field(fd, compiler, msg_info, &field_info)?;
        compiler.add_context(Context::Loop(loop_context))?;
        compiler.pop_context()?;
    }

    compiler.pop_context()?;
    Ok(())
}

fn add_serialization_for_field(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
    field_info: &FieldInfo,
) -> Result<()> {
    // calculate current head ptr
    compiler.add_unsafe_def_with_let(
        false,
        None,
        "cur_header_ptr",
        &format!(
            "constant_header_ptr.offset({})",
            field_info.get_header_offset_str(true),
        ),
    )?;

    // if not int, also need to calculate cur_header_offset
    if !field_info.is_int() {
        compiler.add_def_with_let(
            false,
            None,
            "cur_header_offset",
            &format!("self.offset + {}", field_info.get_header_offset_str(true)),
        )?;
    }
    compiler.add_newline()?;
    let loop_context = LoopContext::new(vec![
        LoopBranch::ifbranch(&format!(
            "self.bitmap[{}] == 1",
            field_info.get_bitmap_idx_str(true)
        )),
        LoopBranch::elsebranch(),
    ]);
    compiler.add_context(Context::Loop(loop_context))?;
    // if branch
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
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!("&mut self.{}.inner_serialize(cur_header_ptr, cur_dynamic_ptr, cur_header_offset, cur_dynamic_offset, copy_func)", field_info.get_name())])?;
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
                let field_size = &field_info.get_header_size_str(true)?;
                compiler.add_line(&format!("LittleEndian::write_{}( unsafe {{ slice::from_raw_parts_mut(cur_header_ptr as _, {}) }}, self.{})", rust_type, field_size, &field_info.get_name()))?;
            }
            FieldType::String | FieldType::Bytes | FieldType::MessageOrEnum(_) => {
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!("&mut self.{}.inner_serialize(cur_header_ptr, cur_dynamic_ptr, cur_header_offset, cur_dynamic_offset, copy_func)", field_info.get_name())])?;
            }
            _ => {
                bail!("Field type not supported: {:?}", &field_info.0.typ);
            }
        }
    }
    // else branch: need to manually deserialize the object and re-serialize it into the current
    // serialization buffer
    compiler.pop_context()?;
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
                add_field_deserialization(
                    compiler,
                    field_info,
                    Some((true, "list_field".to_string())),
                )?;
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!("&mut list_field.inner_serialize(cur_header_ptr, cur_dynamic_ptr, cur_header_offset, cur_dynamic_offset, copy_func)")
                    ],
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
                compiler.add_context(Context::Unsafe(UnsafeContext::new()))?;
                compiler.add_func_call(
                    None,
                    "copy_func",
                    vec![
                        "cur_header_ptr as _".to_string(),
                        format!(
                            "self.header_ptr.as_ptr().offset(self.offset + {} as isize) as _",
                            field_info.get_header_offset_str(true)
                        ),
                        field_info.get_header_size_str(true)?,
                    ],
                )?;
                compiler.pop_context()?;
            }
            FieldType::String | FieldType::Bytes | FieldType::MessageOrEnum(_) => {
                add_field_deserialization(compiler, field_info, Some((true, "field".to_string())))?;
                compiler.add_func_call(
                    Some("ret".to_string()),
                    "append",
                    vec![format!("&mut field.inner_serialize(cur_header_ptr, cur_dynamic_ptr, cur_header_offset, cur_dynamic_offset, copy_func)")
                    ],
                )?;
            }
            _ => {
                bail!("Field type not supported: {:?}", &field_info.0.typ);
            }
        }
    }

    // end if-loop
    compiler.pop_context()?;

    // increment dynamic header offset and pointer if necessary
    if msg_info.dynamic_fields_left(field_info.get_idx(), false, fd.get_message_map())? > 0 {
        if field_info.is_dynamic(false, fd.get_message_map())? {
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
    let lifetime = match msg_info.requires_lifetime(fd.get_message_map())? {
        true => fd.get_lifetime(),
        false => "".to_string(),
    };
    let impl_context = ImplContext::new(
        &msg_info.get_name(),
        Some("HeaderRepr".to_string()),
        &lifetime,
    );
    compiler.add_context(Context::Impl(impl_context))?;
    // add constant header size
    let mut constant_size = "Self::BITMAP_SIZE".to_string();
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        let header_size_str = field_info.get_header_size_str(true)?;
        constant_size = format!("{} + {}", constant_size, header_size_str);
    }
    compiler.add_const_def("CONSTANT_HEADER_SIZE", "usize", &constant_size)?;
    compiler.add_newline()?;

    // add dynamic header size function
    let header_size_function_context = FunctionContext::new(
        "dynamic_header_size",
        false,
        vec![FunctionArg::SelfArg],
        "usize",
    );
    compiler.add_context(Context::Function(header_size_function_context))?;
    let mut dynamic_size = "0".to_string();
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        if field_info.is_list() || field_info.is_nested_msg() {
            dynamic_size = format!(
                "{} + self.get_{}.dynamic_header_size()",
                dynamic_size,
                &field_info.get_name()
            );
        }
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
    compiler.add_return_val("Self::CONSTANT_HEADER_SIZE", false)?;
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
        "",
    );
    compiler.add_context(Context::Function(func_context))?;
    let bitmap_field_str = field.get_bitmap_idx_str(true);
    compiler.add_return_val(
        &format!(
            "self.bitmap[{}] == 1 || (self.has_header_ptr && self.header_ptr[self.offset + {}] == 1)",
            &bitmap_field_str, &bitmap_field_str
        ),
        false,
    )?;

    compiler.pop_context()?; // func context
    Ok(())
}

fn add_field_deserialization(
    compiler: &mut SerializationCompiler,
    field: &FieldInfo,
    output: Option<(bool, String)>,
) -> Result<()> {
    // based on the field type, there's a special way to deserialize
    let field_size_type = field.get_header_size_str(true)?;
    let field_offset_type = field.get_header_offset_str(true);
    if field.is_list() {
        match &field.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float => {
                let right = format!(
                    "List::<{}>from_buffer(self.header_ptr, self.offset + {})",
                    field.get_base_type_str()?,
                    field_offset_type
                );
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::String | FieldType::Bytes => {
                let right = format!(
                    "VariableList::<{}>::from_buffer(self.header_ptr, self.offset + {})",
                    field.get_base_type_str()?,
                    field_offset_type,
                );
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::MessageOrEnum(msg_name) => {
                let right = format!(
                    "VariableList::<{}>::from_buffer(self.header_ptr, self.offset + {})",
                    msg_name, field_offset_type
                );
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            x => {
                bail!("Field type not supported: {:?}", x);
            }
        }
    } else {
        match &field.0.typ {
            FieldType::Int32 => {
                let right  = format!("LittleEndian::read_i32(&self.header_ptr[self.offset + {}..(self.offset + {} + {})])", &field_offset_type, &field_offset_type, &field_size_type);
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::Int64 => {
                let right = format!("LittleEndian::read_i64(&self.header_ptr[self.offset + {}..(self.offset + {} + {})])", 
                &field_offset_type, &field_offset_type, &field_size_type);
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::Uint32 => {
                let right = format!("LittleEndian::read_u32(&self.header_ptr[self.offset + {}..(self.offset + {} + {})])", 
                &field_offset_type, &field_offset_type, &field_size_type);
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::Uint64 => {
                let right = format!("LittleEndian::read_u64(&self.header_ptr[self.offset + {}..(self.offset + {} + {})])", 
                &field_offset_type, &field_offset_type, &field_size_type);
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::Float => {
                let right = format!("LittleEndian::read_f64(&self.header_ptr[self.offset + {}..(self.offset + {} + {})])", 
                &field_offset_type, &field_offset_type, &field_size_type);
                match output {
                    Some((mut_var, var_name)) => {
                        compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                    }
                    None => {
                        compiler.add_return_val(&right, false)?;
                    }
                }
            }
            FieldType::String => {
                let right = format!("CFString::default()");
                let (mut_var, var_name) = match output {
                    Some((_, ref x)) => (true, x.clone()),
                    None => (true, "cf_string".to_string()),
                };
                compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                compiler.add_func_call(
                    Some(var_name.to_string()),
                    "inner_deserialize",
                    vec![
                        "self.header_ptr".to_string(),
                        format!("self.offset + {}", field_offset_type),
                    ],
                )?;
                match &output {
                    Some(_) => {}
                    None => {
                        compiler.add_return_val(&var_name, false)?;
                    }
                }
            }
            FieldType::Bytes => {
                let right = format!("CFBytes::default()");
                let (_, var_name) = match output {
                    Some((_, ref x)) => (true, x.clone()),
                    None => (true, "cf_string".to_string()),
                };
                compiler.add_def_with_let(true, None, &var_name, &right)?;
                compiler.add_func_call(
                    Some(var_name.to_string()),
                    "inner_deserialize",
                    vec![
                        "self.header_ptr".to_string(),
                        format!("self.offset + {}", field_offset_type),
                    ],
                )?;
                match &output {
                    Some(_) => {}
                    None => {
                        compiler.add_return_val(&var_name, false)?;
                    }
                }
            }
            FieldType::MessageOrEnum(msg_name) => {
                let right = format!("{}::new()", msg_name);
                let (mut_var, var_name) = match output {
                    Some((_, ref x)) => (true, x.clone()),
                    None => (true, "cf_string".to_string()),
                };
                compiler.add_def_with_let(mut_var, None, &var_name, &right)?;
                compiler.add_func_call(
                    Some(var_name.to_string()),
                    "inner_deserialize",
                    vec![
                        "self.header_ptr".to_string(),
                        format!("self.offset + {}", field_offset_type),
                    ],
                )?;
                match &output {
                    Some(_) => {}
                    None => {
                        compiler.add_return_val(&var_name, false)?;
                    }
                }
            }
            x => {
                bail!("Field type not supported: {:?}", x);
            }
        }
    }
    Ok(())
}

fn add_get(
    fd: &ProtoReprInfo,
    compiler: &mut SerializationCompiler,
    field: &FieldInfo,
) -> Result<()> {
    let return_type = fd.get_rust_type(field.clone())?;
    let func_context = FunctionContext::new(
        &format!("get_{}", field.get_name()),
        true,
        vec![FunctionArg::SelfArg],
        &return_type,
    );
    compiler.add_context(Context::Function(func_context))?;
    let branches = vec![
        LoopBranch::ifbranch(&format!(
            "self.bitmap[{}] == 1",
            field.get_bitmap_idx_str(true)
        )),
        LoopBranch::elseif(&format!(
            "self.has_header_ptr && self.header_ptr[self.offset + {}] == 1",
            field.get_bitmap_idx_str(true)
        )),
        LoopBranch::elsebranch(),
    ];
    let loop_context = LoopContext::new(branches);
    compiler.add_context(Context::Loop(loop_context))?;
    // If branch: if value has been set externally,
    // return external value
    let self_return_value = match field.derives_copy(&fd.get_message_map())? {
        true => format!("self.{}", field.get_name()),
        false => format!("self.{}.clone()", field.get_name()),
    };
    compiler.add_return_val(&self_return_value, false)?;
    // pop out of if: read reference value from buffer
    compiler.pop_context()?;
    // add field deser + return
    add_field_deserialization(compiler, field, None)?;
    // pop out of else if
    compiler.pop_context()?;
    compiler.add_return_val(&self_return_value, false)?;
    // pop out of else
    compiler.pop_context()?;
    // function pop
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
        &format!("set_{}", &field_name),
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
    let loop_context = LoopContext::new(vec![LoopBranch::ifbranch(&format!(
        "self.has_header_ptr && self.header_ptr[self.offset + {}] == 1 && self.bitmap[{}] != 1",
        field.get_header_offset_str(true),
        field.get_header_offset_str(true)
    ))]);
    compiler.add_context(Context::Loop(loop_context))?;
    add_field_deserialization(compiler, field, Some((false, "field".to_string())))?;
    compiler.add_statement(&format!("self.{}", &field_name), "field")?;
    compiler.add_statement(
        &format!("self.bitmap[{}]", field.get_bitmap_idx_str(true)),
        "1",
    )?;
    compiler.pop_context()?;
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
    compiler.add_statement(
        &format!("self.bitmap[{}])", field.get_bitmap_idx_str(true)),
        "1",
    )?;
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
        for (var, typ, def) in msg_info.get_constants(&field_info, true)?.iter() {
            compiler.add_const_def(var, typ, def)?;
        }
    }

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
    compiler.add_struct_def_field("has_header_ptr", "false")?;
    compiler.add_struct_def_field("offset", "0")?;
    compiler.add_struct_def_field("header_ptr", "&[]")?;
    compiler.add_struct_def_field("bitmap", "[0u8; Self::BITMAP_SIZE]")?;
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
    let bitmap_size = msg_info.get_bitmap_size();
    let struct_ctx = StructContext::new(
        msg_info.get_name().as_str(),
        msg_info.derives_copy(&fd.get_message_map())?,
        fd.get_lifetime().as_str(),
    );

    // add struct header
    compiler.add_context(Context::Struct(struct_ctx))?;
    compiler.add_struct_field("has_header_ptr", "bool")?;
    compiler.add_struct_field("header_offset", "usize")?;
    compiler.add_struct_field("header_ptr", &format!("&'{} [u8]", fd.get_lifetime()))?;
    compiler.add_struct_field("bitmap", &format!("[u8; {}]", bitmap_size))?;
    for field in msg_info.get_fields().iter() {
        let field_info = FieldInfo(field.clone());
        compiler.add_struct_field(&field_info.get_name(), &fd.get_rust_type(field_info)?)?;
    }
    compiler.pop_context()?;
    Ok(())
}

fn add_dependencies(repr: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_codegen::utils::fixed_hdr::*")?;
    compiler.add_dependency("cornflakes_codegen::utils::fixed_hdr::HeaderRepr")?;
    compiler.add_dependency("cornflakes_libos::CornPtr")?;
    compiler.add_dependency("libc")?;
    compiler.add_dependency("std::slice")?;
    if repr.has_int_field() {
        compiler.add_dependency("byteorder::{ByteOrder, LittleEndian}")?;
    }
    Ok(())
}
