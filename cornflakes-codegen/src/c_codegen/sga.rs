use super::{
    super::header_utils::{MessageInfo, ProtoReprInfo},
    super::rust_codegen::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
        MatchContext,
    },
    common::*,
};
use color_eyre::eyre::Result;

pub fn compile(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    add_sga_dependencies(fd, compiler)?;
    compiler.add_newline()?;
    add_cornflakes_structs(fd, compiler)?;

    // For each message type: add basic constructors, getters and setters, and
    // header trait functions.
    for message in fd.get_repr().messages.iter() {
        compiler.add_newline()?;
        let msg_info = MessageInfo(message.clone());
        add_default_impl(compiler, &msg_info)?;
        compiler.add_newline()?;
        add_impl(fd, compiler, &msg_info)?;
        compiler.add_newline()?;
        add_sga_header_repr(compiler, &msg_info)?;
        compiler.add_newline()?;
        add_shared_sga_header_repr(compiler, &msg_info)?;
        break;
    }
    Ok(())
}

fn add_sga_dependencies(fd: &ProtoReprInfo, compiler: &mut SerializationCompiler) -> Result<()> {
    compiler.add_dependency("cornflakes_libos::OrderedSga")?;
    compiler.add_dependency("cornflakes_libos::dynamic_sga_hdr::*")?;
    compiler.add_dependency("mlx5_datapath::datapath::connection::Mlx5Connection")?;

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

fn add_sga_header_repr(
    compiler: &mut SerializationCompiler,
    msg_info: &MessageInfo,
) -> Result<()> {
    // add dynamic header size function
    add_extern_c_wrapper_function(
        compiler,
        &format!("{}_dynamic_header_size", &msg_info.get_name()),
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
        &format!("{}_dynamic_header_start", &msg_info.get_name()),
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
        &format!("{}_num_scatter_gather_entries", &msg_info.get_name()),
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
fn add_shared_sga_header_repr(
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
