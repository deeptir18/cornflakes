use super::{
    super::header_utils::{MessageInfo, ProtoReprInfo},
    common::*,
};
use color_eyre::eyre::Result;
use ffiber::{
    CDylibCompiler,
    types::{SelfArgType, ArgType},
    compiler::{
        Context, FunctionArg, FunctionContext, SerializationCompiler, CArgInfo,
    },
};

pub fn compile(fd: &ProtoReprInfo, compiler: &mut CDylibCompiler) -> Result<()> {
    let datapath = "Mlx5Connection";  // hardcoded datapath
    add_rcsga_dependencies(fd, compiler)?;
    add_bumpalo_functions(compiler)?;
    add_arena_allocate(compiler, datapath)?;
    add_cornflakes_structs(fd, compiler, Some(datapath))?;

    // For each message type: add basic constructors, getters and setters, and
    // header trait functions.
    for message in fd.get_repr().messages.iter() {
        let msg_info = MessageInfo(message.clone());
        let struct_ty = ArgType::Struct {
            name: msg_info.get_name(),
            params: vec![Box::new(ArgType::new_struct(datapath))],
        };
        add_default_impl(compiler, &msg_info, &struct_ty)?;
        add_impl(fd, compiler, &msg_info, &struct_ty, Some(datapath))?;
        add_rcsga_header_repr(compiler, &struct_ty)?;
        add_shared_rcsga_header_repr(compiler, &struct_ty, datapath)?;
    }
    Ok(())
}

fn add_rcsga_dependencies(fd: &ProtoReprInfo, compiler: &mut CDylibCompiler) -> Result<()> {
    compiler.add_dependency("bumpalo::Bump")?;
    compiler.add_dependency("cornflakes_libos::{ArenaOrderedSga, ArenaOrderedRcSga}")?;
    compiler.add_dependency("cornflakes_libos::dynamic_rcsga_hdr::*")?;
    compiler.add_dependency("mlx5_datapath::datapath::connection::Mlx5Connection")?;

    // For VariableList_<param_ty>_index
    for message in fd.get_repr().messages.iter() {
        let msg_info = MessageInfo(message.clone());
        if !has_variable_list(fd, &msg_info, None)?.is_empty() {
            compiler.add_dependency("std::ops::Index")?;
            break;
        }
    }
    Ok(())
}

fn add_bumpalo_functions(compiler: &mut CDylibCompiler) -> Result<()> {
    // add bump intialization function (manually-generated)
    add_bump_initialization_function(&mut compiler.inner)?;

    // add arena reset function
    compiler.add_extern_c_function(
        ArgType::new_struct("Bump"),
        SelfArgType::Mut,
        "reset",
        vec![],
        None,
        false,
    )?;
    Ok(())
}

/// cornflakes-libos/src/state_machine/server.rs
fn add_bump_initialization_function(
    compiler: &mut SerializationCompiler,
) -> Result<()> {
    let args = vec![
        FunctionArg::CArg(CArgInfo::arg("batch_size", "usize")),
        FunctionArg::CArg(CArgInfo::arg("max_packet_size", "usize")),
        FunctionArg::CArg(CArgInfo::arg("max_entries", "usize")),
        FunctionArg::CArg(CArgInfo::ret_arg("*mut ::std::os::raw::c_void")),
    ];
    let func_context = FunctionContext::new_extern_c(
        "Bump_with_capacity", true, args, false,
    );
    compiler.add_context(Context::Function(func_context))?;
    compiler.add_func_call_with_let(
        "capacity",
        None,
        None,
        "ArenaOrderedSga::arena_size",
        vec![
            "batch_size".to_string(),
            "max_packet_size".to_string(),
            "max_entries".to_string(),
        ],
        false,
    )?;
    compiler.add_func_call_with_let(
        "arena",
        None,
        None,
        "Bump::with_capacity",
        vec!["capacity * 100".to_string()],
        false,
    )?;
    compiler.add_def_with_let(false, None, "arena",
        "Box::into_raw(Box::new(arena))")?;
    compiler.add_unsafe_set("return_ptr", "arena as _")?;
    compiler.pop_context()?; // end of function
    compiler.add_newline()?;
    Ok(())
}

/// cornflakes-libos/src/lib.rs
fn add_arena_allocate(
    compiler: &mut CDylibCompiler,
    datapath: &str,
) -> Result<()> {
    let struct_ty = ArgType::Struct {
        name: "ArenaOrderedRcSga".to_string(),
        params: vec![Box::new(ArgType::new_struct(datapath))],
    };

    // add allocate function
    compiler.add_extern_c_function(
        struct_ty.clone(),
        SelfArgType::None,
        "allocate",
        vec![
            ("num_entries", ArgType::Primitive("usize".to_string())),
            ("arena", ArgType::new_ref("Bump")),
        ],
        Some(struct_ty),
        false,
    )?;
    Ok(())
}

fn add_rcsga_header_repr(
    compiler: &mut CDylibCompiler,
    struct_ty: &ArgType,
) -> Result<()> {
    // add num scatter_gather_entries function
    compiler.add_extern_c_function(
        struct_ty.clone(),
        SelfArgType::Value,
        "num_scatter_gather_entries",
        vec![],
        Some(ArgType::Primitive("usize".to_string())),
        false,
    )?;
    Ok(())
}

// These aren't generated functions so we generate the wrappers manually.
// See: cornflakes-codegen/src/utils/dynamic_rcsga_hdr.rs
fn add_shared_rcsga_header_repr(
    compiler: &mut CDylibCompiler,
    struct_ty: &ArgType,
    datapath: &str,
) -> Result<()> {
    // add deserialize_from_buf function
    compiler.add_extern_c_function(
        struct_ty.clone(),
        SelfArgType::Mut,
        "deserialize_from_buf",
        vec![("buffer", ArgType::new_u8_buffer())],
        None,
        true,
    )?;

    // add serialize_into_arena_sga function
    compiler.add_extern_c_function(
        struct_ty.clone(),
        SelfArgType::Value,
        "serialize_into_arena_sga",
        vec![
            ("ordered_sga", ArgType::RefMut(
                Box::new(ArgType::Struct {
                    name: "ArenaOrderedRcSga".to_string(),
                    params: vec![Box::new(ArgType::new_struct(datapath))],
                }),
            )),
            ("arena", ArgType::new_ref("Bump")),
            ("datapath", ArgType::new_ref(datapath)),
            ("with_copy", ArgType::Primitive("bool".to_string())),
        ],
        None,
        true,
    )?;
    Ok(())
}
