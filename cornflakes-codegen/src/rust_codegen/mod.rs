use super::{header_utils::ProtoReprInfo, CompileOptions, HeaderType};
use color_eyre::eyre::{bail, Result, WrapErr};
use std::{fs::File, io::Write, path::Path, process::Command, str};
use which::which;

mod constant_codegen;
mod hybridarenaobject;
mod hybridobject;
mod hybridrcsga;
mod linear_codegen;
mod linear_codegen_rc;
mod rcsga;
mod sga;

pub fn compile(repr: &ProtoReprInfo, output_folder: &str, options: CompileOptions) -> Result<()> {
    let mut compiler = SerializationCompiler::new();
    match options.header_type {
        HeaderType::ConstantDeserialization => {
            constant_codegen::compile(repr, &mut compiler)
                .wrap_err("Constant codegen failed to generate code.")?;
        }
        HeaderType::LinearDeserialization => {
            linear_codegen::compile(repr, &mut compiler)
                .wrap_err("Linear codegen failed to generate code.")?;
        }

        HeaderType::LinearDeserializationRefCnt => {
            linear_codegen_rc::compile(repr, &mut compiler)
                .wrap_err("Linear codegen refcnt failed to generate code.")?;
        }
        HeaderType::Sga => {
            sga::compile(repr, &mut compiler).wrap_err("Sga codegen failed to generate code.")?;
        }
        HeaderType::RcSga => {
            rcsga::compile(&repr, &mut compiler)
                .wrap_err("RcSga codegen failed to generate code.")?;
        }
        HeaderType::HybridRcSga => {
            hybridrcsga::compile(&repr, &mut compiler)
                .wrap_err("Hybrid RcSga codegen failed to generate code.")?;
        }
        HeaderType::HybridObject => {
            hybridobject::compile(&repr, &mut compiler)
                .wrap_err("Hybrid object codegen failed to generate code")?;
        }
        HeaderType::HybridArenaObject => {
            hybridarenaobject::compile(&repr, &mut compiler)
                .wrap_err("Hybrid arena object failed to generate code")?;
        }
    }
    compiler.flush(&repr.get_output_file(output_folder).as_path())?;
    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArgInfo {
    is_ref: bool,
    is_mut: bool,
    lifetime: Option<String>,
    arg_name: String,
}

impl ArgInfo {
    pub fn ref_arg(name: &str, lifetime: Option<String>) -> ArgInfo {
        ArgInfo {
            is_ref: true,
            is_mut: false,
            lifetime: lifetime,
            arg_name: String::from(name),
        }
    }

    pub fn ref_mut_arg(name: &str, lifetime: Option<String>) -> ArgInfo {
        ArgInfo {
            is_ref: true,
            is_mut: true,
            lifetime: lifetime,
            arg_name: String::from(name),
        }
    }

    pub fn owned(name: &str) -> ArgInfo {
        ArgInfo {
            is_ref: false,
            is_mut: false,
            lifetime: None,
            arg_name: String::from(name),
        }
    }

    pub fn get_type_string(&self) -> String {
        if self.is_ref && !self.is_mut {
            match &self.lifetime {
                Some(lifetime) => {
                    format!("&{} {}", lifetime, self.arg_name)
                }
                None => {
                    format!("& {}", self.arg_name)
                }
            }
        } else if self.is_ref && self.is_mut {
            match &self.lifetime {
                Some(lifetime) => {
                    format!("&{} mut {}", lifetime, self.arg_name)
                }
                None => {
                    format!("&mut {}", self.arg_name)
                }
            }
        } else {
            format!("{}", self.arg_name)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CArgInfo {
    is_ret: bool,
    name: String,
    ty: String,
}

impl CArgInfo {
    pub fn len_arg(name: &str) -> CArgInfo {
        CArgInfo {
            is_ret: false,
            name: format!("{}_len", name),
            ty: String::from("usize"),
        }
    }

    pub fn arg(name: &str, ty: &str) -> CArgInfo {
        CArgInfo {
            is_ret: false,
            name: String::from(name),
            ty: String::from(ty),
        }
    }

    pub fn ret_len_arg() -> CArgInfo {
        CArgInfo {
            is_ret: true,
            name: "return_len_ptr".to_string(),
            ty: String::from("usize"),
        }
    }

    pub fn ret_arg(ty: &str) -> CArgInfo {
        CArgInfo {
            is_ret: true,
            name: "return_ptr".to_string(),
            ty: String::from(ty),
        }
    }

    pub fn get_string(&self) -> String {
        if self.is_ret {
            format!("{}: *mut {}", self.name, self.ty)
        } else {
            format!("{}: {}", self.name, self.ty)
        }
    }
}

pub trait ContextPop {
    /// Pops the next thing from this context and writes it as a line.
    /// Returns whether the context should be added back to the context list.
    fn pop(&mut self) -> Result<(String, bool)>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FunctionArg {
    SelfArg,
    MutSelfArg,
    Arg(String, ArgInfo),
    CSelfArg,
    CArg(CArgInfo),
}

impl FunctionArg {
    pub fn get_string(&self) -> String {
        match self {
            FunctionArg::SelfArg => "&self".to_string(),
            FunctionArg::MutSelfArg => "&mut self".to_string(),
            FunctionArg::Arg(name, info) => format!("{}: {}", name, info.get_type_string()),
            FunctionArg::CSelfArg => "self_: *mut ::std::os::raw::c_void".to_string(),
            FunctionArg::CArg(info) => info.get_string(),
        }
    }

    pub fn new_arg(name: &str, info: ArgInfo) -> Self {
        FunctionArg::Arg(name.to_string(), info)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FunctionContext {
    pub name: String,
    pub is_pub: bool,
    pub args: Vec<FunctionArg>,
    ret_type: Option<String>,
    is_extern_c: bool,
    started: bool,
    func_lifetime: Option<String>,
    where_clause: Option<String>,
}

impl FunctionContext {
    pub fn new(name: &str, is_pub: bool, args: Vec<FunctionArg>, ret: &str) -> Self {
        let ret_type: Option<String> = match ret {
            "" => None,
            x => Some(x.to_string()),
        };
        FunctionContext {
            name: name.to_string(),
            is_pub: is_pub,
            args: args,
            is_extern_c: false,
            started: false,
            ret_type: ret_type,
            func_lifetime: None,
            where_clause: None,
        }
    }

    pub fn new_extern_c(name: &str, is_pub: bool, args: Vec<FunctionArg>, err_code: bool) -> Self {
        let ret = if err_code { "u32" } else { "" };
        let mut func_context = Self::new(name, is_pub, args, ret);
        func_context.is_extern_c = true;
        func_context
    }

    pub fn new_with_lifetime(
        name: &str,
        is_pub: bool,
        args: Vec<FunctionArg>,
        ret: &str,
        func_lifetime: &str,
        where_clause: &str,
    ) -> Self {
        let ret_type: Option<String> = match ret {
            "" => None,
            x => Some(x.to_string()),
        };

        let lifetime: Option<String> = Some(func_lifetime.to_string());

        let where_cl = match where_clause {
            "" => None,
            x => Some(x.to_string()),
        };
        FunctionContext {
            name: name.to_string(),
            is_pub: is_pub,
            args: args,
            is_extern_c: false,
            started: false,
            ret_type: ret_type,
            func_lifetime: lifetime,
            where_clause: where_cl,
        }
    }
}

impl ContextPop for FunctionContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        if !self.started {
            self.started = true;
            let inline_str = "#[inline]";
            let is_pub_str = match self.is_pub {
                true => "pub ".to_string(),
                false => "".to_string(),
            };
            let (no_mangle_str, extern_c_str) = match self.is_extern_c {
                true => ("\n#[no_mangle]".to_string(), "extern \"C\" ".to_string()),
                false => ("".to_string(), "".to_string()),
            };
            let lifetime_str = match &self.func_lifetime {
                Some(x) => format!("<{}>", x),
                None => "".to_string(),
            };

            let where_str = match &self.where_clause {
                Some(x) => format!("where {}", x),
                None => "".to_string(),
            };

            let args: Vec<String> = self.args.iter().map(|arg| arg.get_string()).collect();
            let args_string = args.join(", ");
            let ret_value = match &self.ret_type {
                Some(r) => format!(" -> {}", r),
                None => "".to_string(),
            };
            Ok((
                format!(
                    "{}{}\n{}{}fn {}{}({}) {} {} {{",
                    inline_str,
                    no_mangle_str,
                    is_pub_str,
                    extern_c_str,
                    self.name,
                    lifetime_str,
                    args_string,
                    ret_value,
                    where_str,
                ),
                false,
            ))
        } else {
            Ok(("}".to_string(), true))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WherePair {
    pub key: String,
    pub name: String,
}

impl WherePair {
    pub fn new(key: &str, name: &str) -> Self {
        WherePair {
            key: key.to_string(),
            name: name.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}: {}", self.key, self.name)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct WhereClause {
    pub pairs: Vec<WherePair>,
    started: bool,
}

impl WhereClause {
    pub fn new(pairs: Vec<WherePair>) -> Self {
        WhereClause {
            pairs: pairs,
            started: false,
        }
    }

    pub fn to_string(&self) -> String {
        if self.pairs.len() == 0 {
            return "".to_string();
        }
        let pair_strings: Vec<String> = self.pairs.iter().map(|x| x.to_string()).collect();
        format!("where\n{}", pair_strings.join(", \n"))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructContext {
    struct_name: StructName,
    derives_copy: bool,
    where_clause: WhereClause,
    started: bool,
    no_derives: bool,
}

impl StructContext {
    pub fn new(struct_name: StructName, derives_copy: bool, where_clause: WhereClause) -> Self {
        StructContext {
            struct_name: struct_name,
            derives_copy: derives_copy,
            where_clause: where_clause,
            started: false,
            no_derives: false,
        }
    }

    pub fn set_no_derives(&mut self) {
        self.no_derives = true;
    }
}

impl ContextPop for StructContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        if !self.started {
            self.started = true;
            let derives = match self.derives_copy {
                true => "Debug, Clone, PartialEq, Eq, Copy",
                false => "Debug, Clone, PartialEq, Eq",
            };
            let derive_string = match self.no_derives {
                true => "".to_string(),
                false => format!("#[derive({})]\n", derives),
            };
            return Ok((
                format!(
                    "{} pub struct {} {} {{",
                    derive_string,
                    self.struct_name.to_string(),
                    self.where_clause.to_string(),
                ),
                false,
            ));
        } else {
            Ok(("}".to_string(), true))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructDefContext {
    name: String,
    started: bool,
}

impl StructDefContext {
    pub fn new(name: &str) -> Self {
        StructDefContext {
            name: name.to_string(),
            started: false,
        }
    }
}

impl ContextPop for StructDefContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        if !self.started {
            self.started = true;
            Ok((format!("{} {{", self.name), false))
        } else {
            Ok(("}".to_string(), true))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructName {
    pub struct_name: String,
    pub type_annotation: Vec<String>,
}

impl StructName {
    pub fn new(name: &str, type_annotation: Vec<String>) -> Self {
        StructName {
            struct_name: name.to_string(),
            type_annotation: type_annotation,
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "{}{}",
            self.struct_name,
            bracket_type_vec(self.type_annotation.clone())
        )
    }

    pub fn type_vec(&self) -> Vec<String> {
        self.type_annotation.clone()
    }

    pub fn combined_type_vec(&self, trait_name: Option<TraitName>) -> Vec<String> {
        let mut base = self.type_annotation.clone();
        match trait_name {
            Some(t) => {
                for typ in t.type_vec().iter() {
                    if !base.contains(&typ) {
                        if typ.to_string().contains("'") {
                            base.insert(0, typ.to_string());
                        } else {
                            base.push(typ.to_string());
                        }
                    }
                }
            }
            None => {}
        }
        base
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TraitName {
    pub trait_name: String,
    pub type_annotation: Vec<String>,
}

impl TraitName {
    pub fn new(name: &str, type_annotation: Vec<String>) -> Self {
        TraitName {
            trait_name: name.to_string(),
            type_annotation: type_annotation,
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "{}{}",
            self.trait_name,
            bracket_type_vec(self.type_annotation.clone())
        )
    }

    pub fn type_vec(&self) -> Vec<String> {
        self.type_annotation.clone()
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImplContext {
    pub struct_name: StructName,
    pub trait_name: Option<TraitName>,
    pub where_clause: WhereClause,
    started: bool,
}

impl ImplContext {
    pub fn new(
        struct_name: StructName,
        trait_name: Option<TraitName>,
        where_clause: WhereClause,
    ) -> Self {
        ImplContext {
            struct_name: struct_name,
            trait_name: trait_name,
            where_clause: where_clause,
            started: false,
        }
    }
}

fn bracket_type_vec(vec: Vec<String>) -> String {
    match vec.len() {
        0 => "".to_string(),
        _x => format!("<{}>", vec.join(", ")),
    }
}

impl ContextPop for ImplContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        if !self.started {
            self.started = true;
            let impl_type =
                bracket_type_vec(self.struct_name.combined_type_vec(self.trait_name.clone()));

            let type_str = self.struct_name.to_string();
            let where_clause_string = self.where_clause.to_string();

            match &self.trait_name {
                Some(x) => Ok((
                    format!(
                        "impl{} {} for {} {} {{",
                        impl_type,
                        x.to_string(),
                        type_str,
                        where_clause_string,
                    ),
                    false,
                )),
                None => Ok((
                    format!("impl{} {} {} {{", impl_type, type_str, where_clause_string,),
                    false,
                )),
            }
        } else {
            Ok(("}".to_string(), true))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LoopBranch {
    If(String),
    ElseIf(String),
    Else,
    Finished,
}

impl LoopBranch {
    pub fn ifbranch(cond: &str) -> Self {
        LoopBranch::If(cond.to_string())
    }

    pub fn elseif(cond: &str) -> Self {
        LoopBranch::ElseIf(cond.to_string())
    }

    pub fn elsebranch() -> Self {
        LoopBranch::Else
    }

    pub fn fin() -> Self {
        LoopBranch::Finished
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LoopContext {
    branches: Vec<LoopBranch>,
    current_idx: usize,
}

impl LoopContext {
    pub fn new(mut branches: Vec<LoopBranch>) -> Self {
        branches.push(LoopBranch::fin());
        LoopContext {
            branches: branches,
            current_idx: 0,
        }
    }
}

impl ContextPop for LoopContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        let last_branch = &self.branches[self.current_idx];
        self.current_idx += 1;
        match last_branch {
            LoopBranch::If(cond) => Ok((format!("if {} {{", cond), false)),
            LoopBranch::ElseIf(cond) => Ok((format!("}} else if {} {{", cond), false)),
            LoopBranch::Else => Ok(("} else {".to_string(), false)),
            LoopBranch::Finished => Ok(("}".to_string(), true)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UnsafeContext {
    is_started: bool,
}

impl UnsafeContext {
    pub fn new() -> Self {
        UnsafeContext { is_started: false }
    }
}

impl ContextPop for UnsafeContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        if !self.is_started {
            self.is_started = true;
            Ok(("unsafe {".to_string(), false))
        } else {
            Ok(("}".to_string(), true))
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MatchContext {
    cond: String,
    variants: Vec<String>,
    num_variants: usize,
    variant_idx: usize,
    var_name: Option<String>,
}

impl MatchContext {
    pub fn new(cond: &str, variants: Vec<String>) -> Self {
        assert!(variants.len() > 0);
        let num_variants = variants.len();
        MatchContext {
            cond: cond.to_string(),
            variants: variants,
            num_variants: num_variants,
            variant_idx: 0,
            var_name: None,
        }
    }

    pub fn new_with_def(cond: &str, variants: Vec<String>, var_name: &str) -> Self {
        let var_name_str: Option<String> = match var_name {
            "" => None,
            x => Some(x.to_string()),
        };
        assert!(variants.len() > 0);
        let num_variants = variants.len();
        MatchContext {
            cond: cond.to_string(),
            variants: variants,
            num_variants: num_variants,
            variant_idx: 0,
            var_name: var_name_str,
        }
    }
}

impl ContextPop for MatchContext {
    fn pop(&mut self) -> Result<(String, bool)> {
        let mut ret = "".to_string();
        if self.variant_idx == self.variants.len() {
            let ret = match &self.var_name {
                Some(_) => "}\n};".to_string(),
                None => "}\n}".to_string(),
            };
            return Ok((ret, true));
        } else if self.variant_idx == 0 {
            let var_def_string = match &self.var_name {
                Some(x) => format!("let {} =", x),
                None => "".to_string(),
            };
            ret = format!("{} {} match {} {{\n", ret, var_def_string, &self.cond);
            let cond = &self.variants[0];
            ret = format!("{} {} => {{", ret, cond);
            self.variant_idx += 1;
            return Ok((ret, false));
        } else {
            let cond = &self.variants[self.variant_idx];
            ret = format!("}} \n {} => {{", cond);
            self.variant_idx += 1;
            return Ok((ret, false));
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Context {
    Function(FunctionContext),
    Struct(StructContext),
    StructDef(StructDefContext),
    Impl(ImplContext),
    Loop(LoopContext),
    Unsafe(UnsafeContext),
    Match(MatchContext),
}

impl ContextPop for Context {
    fn pop(&mut self) -> Result<(String, bool)> {
        match self {
            Context::Function(ref mut func_context) => func_context.pop(),
            Context::Struct(ref mut struct_context) => struct_context.pop(),
            Context::StructDef(ref mut struct_def_context) => struct_def_context.pop(),
            Context::Impl(ref mut impl_context) => impl_context.pop(),
            Context::Loop(ref mut loop_context) => loop_context.pop(),
            Context::Unsafe(ref mut unsafe_context) => unsafe_context.pop(),
            Context::Match(ref mut match_context) => match_context.pop(),
        }
    }
}

pub struct SerializationCompiler {
    current_string: String,
    current_context: Vec<Context>,
}

impl SerializationCompiler {
    pub fn new() -> Self {
        SerializationCompiler {
            current_string: "".to_string(),
            current_context: Vec::default(),
        }
    }

    pub fn add_extern_crate(&mut self, crate_name: &str) -> Result<()> {
        self.current_string
            .push_str(&format!("extern crate {};", crate_name));
        self.add_newline()?;
        Ok(())
    }

    pub fn add_mod_declaration(&mut self, mod_name: &str) -> Result<()> {
        self.current_string.push_str(&format!("mod {};", mod_name));
        self.add_newline()?;
        Ok(())
    }

    pub fn add_dependency(&mut self, dependency: &str) -> Result<()> {
        self.current_string
            .push_str(&format!("use {};", dependency));
        self.add_newline()?;
        Ok(())
    }

    pub fn add_macro_call(&mut self, macro_name: &str, args: Vec<String>) -> Result<()> {
        let line = format!("{}!({});", macro_name, args.join(", "));
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_line(&mut self, line: &str) -> Result<()> {
        self.current_string
            .push_str(&("\t".repeat(self.current_indent_level())));
        self.current_string.push_str(line);
        self.add_newline()?;
        Ok(())
    }

    pub fn add_struct_field(&mut self, name: &str, typ: &str) -> Result<()> {
        let last_ctx = &self.current_context[self.current_context.len() - 1];
        match last_ctx {
            Context::Struct(_) => {}
            _ => {
                bail!("Previous context must be struct context.");
            }
        }

        self.add_line(&format!("{}: {},", name, typ))?;
        Ok(())
    }

    pub fn add_struct_def_field(&mut self, name: &str, typ: &str) -> Result<()> {
        let last_ctx = &self.current_context[self.current_context.len() - 1];
        match last_ctx {
            Context::StructDef(_) => {}
            _ => {
                bail!("Previous context must be struct context.");
            }
        }

        self.add_line(&format!("{}: {},", name, typ))?;
        Ok(())
    }

    pub fn add_unsafe_def_with_let(
        &mut self,
        is_mut: bool,
        typ: Option<String>,
        left: &str,
        right: &str,
    ) -> Result<()> {
        let mut_str = match is_mut {
            true => "mut ",
            false => "",
        };
        let type_str = match typ {
            Some(x) => format!(": {}", x),
            None => "".to_string(),
        };
        let line = format!(
            "let {}{}{} = unsafe {{ {} }};",
            mut_str, left, type_str, right
        );
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_constant_def(
        &mut self,
        is_pub: bool,
        left: &str,
        typ: &str,
        right: &str,
    ) -> Result<()> {
        let pub_str = match is_pub {
            true => "pub ",
            false => "",
        };

        let line = format!("{}const {}: {} = {};", pub_str, left, typ, right);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_def_with_let(
        &mut self,
        is_mut: bool,
        typ: Option<String>,
        left: &str,
        right: &str,
    ) -> Result<()> {
        let mut_str = match is_mut {
            true => "mut",
            false => "",
        };
        let type_str = match typ {
            Some(x) => format!(": {}", x),
            None => "".to_string(),
        };
        let line = format!("let {} {}{} = {};", mut_str, left, type_str, right);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_unsafe_statement(&mut self, left: &str, right: &str) -> Result<()> {
        let line = format!("{} = unsafe {{ {} }};", left, right);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_unsafe_set(&mut self, left: &str, right: &str) -> Result<()> {
        let line = format!("unsafe {{ *{} = {} }};", left, right);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_plus_equals(&mut self, left: &str, right: &str) -> Result<()> {
        let line = format!("{} += {};", left, right);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_block(&mut self, block: &str) -> Result<()> {
        self.add_line(block)?;
        Ok(())
    }

    pub fn add_statement(&mut self, left: &str, right: &str) -> Result<()> {
        let line = format!("{} = {};", left, right);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_func_call_with_plus_equals(
        &mut self,
        left: &str,
        caller: Option<String>,
        func: &str,
        args: Vec<String>,
        add_res: bool,
    ) -> Result<()> {
        let caller_str = match caller {
            Some(x) => format!("{}.", x),
            None => "".to_string(),
        };

        let res = match add_res {
            true => "?",
            false => "",
        };

        let line = format!(
            "{} += {}{}({}){};",
            left,
            caller_str,
            func,
            args.join(", "),
            res
        );
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_func_call(
        &mut self,
        caller: Option<String>,
        func: &str,
        args: Vec<String>,
        add_res: bool,
    ) -> Result<()> {
        let caller_str = match caller {
            Some(x) => format!("{}.", x),
            None => "".to_string(),
        };

        let res = match add_res {
            true => "?",
            false => "",
        };

        let line = format!("{}{}({}){};", caller_str, func, args.join(", "), res);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_func_call_with_let(
        &mut self,
        left: &str,
        return_ty: Option<String>,
        caller: Option<String>,
        func: &str,
        args: Vec<String>,
        add_res: bool,
    ) -> Result<()> {
        let caller_str = match caller {
            Some(x) => format!("{}.", x),
            None => "".to_string(),
        };

        let res = match add_res {
            true => "?",
            false => "",
        };

        let line = format!(
            "let {}{} = {}{}({}){};",
            left,
            return_ty
                .map(|ty| format!(": {}", ty))
                .unwrap_or("".to_string()),
            caller_str,
            func,
            args.join(", "),
            res
        );
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_return_val(&mut self, statement: &str, with_return: bool) -> Result<()> {
        let line = match with_return {
            true => format!("return {};", statement),
            false => format!("{}", statement),
        };
        self.add_line(&line)?;
        Ok(())
    }

    pub fn current_indent_level(&self) -> usize {
        self.current_context.len()
    }

    pub fn add_context(&mut self, ctx: Context) -> Result<()> {
        self.current_context.push(ctx);
        self.pop_context()?;
        Ok(())
    }

    pub fn pop_context(&mut self) -> Result<()> {
        let mut last_ctx = match self.current_context.pop() {
            Some(ctx) => ctx,
            None => {
                bail!("No context to pop.");
            }
        };
        let (line, end) = last_ctx.pop()?;
        self.add_line(&line)?;

        if !end {
            self.current_context.push(last_ctx);
        }
        Ok(())
    }

    pub fn add_const_def_pub(&mut self, var_name: &str, typ: &str, def: &str) -> Result<()> {
        let line = format!("pub const {}: {} = {};", var_name, typ, def);
        self.add_line(&line)?;
        Ok(())
    }
    pub fn add_const_def(&mut self, var_name: &str, typ: &str, def: &str) -> Result<()> {
        let line = format!("const {}: {} = {};", var_name, typ, def);
        self.add_line(&line)?;
        Ok(())
    }

    pub fn add_newline(&mut self) -> Result<()> {
        self.current_string.push_str("\n");
        Ok(())
    }

    pub fn flush(&self, output_file: &Path) -> Result<()> {
        let mut of = File::create(output_file)
            .wrap_err(format!("Failed to create output file at {:?}", output_file))?;
        let mut pos: usize = 0;
        while pos < self.current_string.as_str().len() {
            pos += of.write(&self.current_string.as_str().as_bytes()[pos..])?;
        }
        of.flush().wrap_err("Failed to flush file.")?;
        run_rustfmt(output_file).wrap_err("Failure from run_rustfmt.")?;
        Ok(())
    }
}

/// Programmatically runs rustfmt on a file.
pub fn run_rustfmt(output_file: &Path) -> Result<()> {
    let rustfmt = which("rustfmt").wrap_err("Failed to find rustfmt.")?;
    let str_file = match output_file.to_str() {
        Some(s) => s,
        None => {
            bail!("Failed to convert path to str: {:?}", output_file);
        }
    };
    Command::new(rustfmt)
        .arg(str_file)
        .output()
        .wrap_err("failed to execute Rustfmt process")?;
    Ok(())
}
