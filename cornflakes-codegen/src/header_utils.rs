use super::rust_codegen::{WhereClause, WherePair};
use color_eyre::eyre::{bail, Result};
use protobuf_parser::{Field, FieldType, FileDescriptor, Message, Rule};
use std::collections::HashMap;
use std::path::PathBuf;

const ALIGN_SIZE: usize = 8;
static LIFETIME_NAME: &str = "registered";
static DATAPATH_TRAIT_KEY: &str = "D";
static DATAPATH_TRAIT: &str = "Datapath";

fn align_up(x: usize, align_size: usize) -> usize {
    // find value aligned up to align_size
    let divisor = x / align_size;
    if (divisor * align_size) < x {
        return (divisor + 1) * align_size;
    } else {
        assert!(divisor * align_size == x);
        return x;
    }
}

#[derive(Debug, Clone)]
pub struct ProtoReprInfo {
    repr: FileDescriptor,
    message_map: HashMap<String, Message>,
    lifetime_name: String,
    datapath_trait_key: String,
    datapath_trait: String,
    ref_counted_mode: bool,
    hybrid_mode: bool,
    needs_datapath_param: bool,
}

impl ProtoReprInfo {
    pub fn new(repr: FileDescriptor) -> Self {
        let mut message_map: HashMap<String, Message> = HashMap::default();
        for message in repr.messages.iter() {
            message_map.insert(message.name.clone(), message.clone());
        }
        ProtoReprInfo {
            repr: repr,
            message_map: message_map,
            lifetime_name: LIFETIME_NAME.to_string(),
            datapath_trait_key: DATAPATH_TRAIT_KEY.to_string(),
            datapath_trait: DATAPATH_TRAIT.to_string(),
            ref_counted_mode: false,
            needs_datapath_param: false,
            hybrid_mode: false,
        }
    }

    pub fn hybrid_mode(&self) -> bool {
        self.hybrid_mode
    }

    pub fn set_needs_datapath_param(&mut self) {
        self.needs_datapath_param = true;
    }

    pub fn always_requires_datapath_param(&self) -> bool {
        self.needs_datapath_param
    }

    pub fn needs_datapath_param(&self) -> bool {
        self.needs_datapath_param
    }

    pub fn set_lifetime_name(&mut self, name: &str) {
        self.lifetime_name = name.to_string();
    }

    pub fn set_ref_counted(&mut self) {
        self.ref_counted_mode = true;
    }

    pub fn set_hybrid_mode(&mut self) {
        self.hybrid_mode = true;
    }

    pub fn get_message_map(&self) -> &HashMap<String, Message> {
        &self.message_map
    }

    pub fn get_datapath_trait_key(&self) -> String {
        self.datapath_trait_key.to_string()
    }

    pub fn get_datapath_trait(&self) -> String {
        self.datapath_trait.to_string()
    }

    pub fn get_lifetime(&self) -> String {
        format!("'{}", self.lifetime_name)
    }

    pub fn get_repr(&self) -> FileDescriptor {
        self.repr.clone()
    }

    /// Do any of the fields in any of the messages contain integers.
    /// If so, including LittleEndian libraries is required.
    pub fn has_int_field(&self) -> bool {
        for message in self.repr.messages.iter() {
            for field in message.fields.iter() {
                let cont = match &field.typ {
                    FieldType::Int32
                    | FieldType::Int64
                    | FieldType::Uint32
                    | FieldType::Uint64
                    | FieldType::Float => false,
                    _ => true,
                };
                if !cont {
                    return true;
                }
            }
        }
        return false;
    }

    pub fn get_c_package_name(&self, folder: &str) -> PathBuf {
        let mut pathbuf = PathBuf::new();
        if folder != "" {
            pathbuf.push(folder);
        }
        pathbuf.push(&format!("{}-c", str::replace(&self.repr.package, "_", "-")));
        pathbuf
    }

    pub fn get_output_file(&self, folder: &str) -> PathBuf {
        let mut pathbuf = PathBuf::new();
        if folder != "" {
            pathbuf.push(folder);
        }
        pathbuf.push(&format!("{}.rs", self.repr.package));
        pathbuf
    }

    pub fn get_default_type_object(&self, field: FieldInfo, use_arena: bool) -> Result<String> {
        let default_val = match &field.0.typ {
            FieldType::Int32 | FieldType::Int64 | FieldType::Uint32 | FieldType::Uint64 => {
                "0".to_string()
            }
            FieldType::Float => "0.0".to_string(),
            FieldType::String | FieldType::RefCountedString => {
                if !use_arena {
                    "CFString::default()".to_string()
                } else {
                    "CFString::new_in(arena)".to_string()
                }
            }
            FieldType::Bytes | FieldType::RefCountedBytes => {
                if !use_arena {
                    "CFBytes::default()".to_string()
                } else {
                    "CFBytes::new_in(arena)".to_string()
                }
            }
            FieldType::MessageOrEnum(msg_name) => match self.hybrid_mode {
                true => {
                    format!("{}::new_in(arena)", msg_name)
                }
                false => {
                    if use_arena {
                        format!("{}::new_in(arena)", msg_name)
                    } else {
                        format!("{}::new()", msg_name)
                    }
                }
            },
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };
        Ok(default_val)
    }

    pub fn get_default_type(&self, field: FieldInfo) -> Result<String> {
        let default_val = match &field.0.typ {
            FieldType::Int32 | FieldType::Int64 | FieldType::Uint32 | FieldType::Uint64 => {
                "0".to_string()
            }
            FieldType::Float => "0.0".to_string(),
            FieldType::String => "CFString::default()".to_string(),
            FieldType::Bytes => "CFBytes::default()".to_string(),
            FieldType::RefCountedString => "CFString::default()".to_string(),
            FieldType::RefCountedBytes => "CFBytes::default()".to_string(),
            FieldType::MessageOrEnum(msg_name) => match self.hybrid_mode {
                true => {
                    format!("{}::new_in(arena)", msg_name)
                }
                false => {
                    format!("{}::default()", msg_name)
                }
            },
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };
        Ok(default_val)
    }

    pub fn get_c_type(&self, field: FieldInfo) -> Result<String> {
        let base_type = match &field.0.typ {
            FieldType::Int32 => "i32".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Uint32 => "u32".to_string(),
            FieldType::Uint64 => "u64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::Bytes | FieldType::RefCountedBytes => {
                "*const ::std::os::raw::c_uchar".to_string()
            }
            FieldType::String | FieldType::RefCountedString => {
                "*const ::std::os::raw::c_uchar".to_string()
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };
        Ok(base_type)
    }

    pub fn get_rust_type_hybrid_object(&self, field: FieldInfo, use_arena: bool) -> Result<String> {
        let base_type = match &field.0.typ {
            FieldType::Int32 => "i32".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Uint32 => "u32".to_string(),
            FieldType::Uint64 => "u64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::String | FieldType::RefCountedString => {
                if use_arena {
                    format!("CFString<'arena, {}>", self.get_datapath_trait_key())
                } else {
                    format!("CFString<{}>", self.get_datapath_trait_key())
                }
            }
            FieldType::Bytes | FieldType::RefCountedBytes => {
                if use_arena {
                    format!("CFBytes<'arena, {}>", self.get_datapath_trait_key())
                } else {
                    format!("CFBytes<{}>", self.get_datapath_trait_key())
                }
            }
            FieldType::MessageOrEnum(msg_name) => {
                let mut type_params = vec![self.get_datapath_trait_key()];
                if use_arena {
                    type_params.insert(0, "'arena".to_string());
                }
                format!("{}<{}>", msg_name, type_params.join(", "))
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };

        if field.is_list() {
            match &field.0.typ {
                FieldType::Int32
                | FieldType::Int64
                | FieldType::Uint32
                | FieldType::Uint64
                | FieldType::Float => {
                    bail!("List of Int/Float not supported yet in this codegen mode.");
                }
                FieldType::String
                | FieldType::Bytes
                | FieldType::RefCountedBytes
                | FieldType::RefCountedString
                | FieldType::MessageOrEnum(_) => {
                    if use_arena {
                        return Ok(format!(
                            "VariableList<'arena, {}, {}>",
                            base_type,
                            self.get_datapath_trait_key()
                        ));
                    } else {
                        return Ok(format!(
                            "VariableList<{}, {}>",
                            base_type,
                            self.get_datapath_trait_key()
                        ));
                    }
                }
                _ => {
                    bail!("FieldType {:?} not supported by compiler", field.0.typ);
                }
            }
        } else {
            return Ok(base_type);
        }
    }

    pub fn get_rust_type_hybrid(&self, field: FieldInfo) -> Result<String> {
        let base_type = match &field.0.typ {
            FieldType::Int32 => "i32".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Uint32 => "u32".to_string(),
            FieldType::Uint64 => "u64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::String | FieldType::RefCountedString => {
                format!(
                    "CFString<'{}, {}>",
                    self.lifetime_name,
                    self.get_datapath_trait_key()
                )
            }
            FieldType::Bytes | FieldType::RefCountedBytes => {
                format!(
                    "CFBytes<'{}, {}>",
                    self.lifetime_name,
                    self.get_datapath_trait_key()
                )
            }
            FieldType::MessageOrEnum(msg_name) => {
                let mut type_params = vec![
                    format!("'{}", self.lifetime_name),
                    self.get_datapath_trait_key(),
                ];
                // if this message contains a list or a message that contains a list, insert
                // "arena" into the type params
                let msg = match self.message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Field type: {} not in message_map", msg_name);
                    }
                };

                if msg.contains_variable_list(&self.message_map)? {
                    type_params.insert(0, "'arena".to_string());
                    //panic!("Contains variable list true for {:?}", msg);
                }

                format!("{}<{}>", msg_name, type_params.join(", "))
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };

        if field.is_list() {
            match &field.0.typ {
                FieldType::Int32
                | FieldType::Int64
                | FieldType::Uint32
                | FieldType::Uint64
                | FieldType::Float => {
                    bail!("List of Int/Float not supported yet in this codegen mode.");
                }
                FieldType::String
                | FieldType::Bytes
                | FieldType::RefCountedBytes
                | FieldType::RefCountedString
                | FieldType::MessageOrEnum(_) => {
                    return Ok(format!(
                        "VariableList<'arena, {}, {}>",
                        base_type,
                        self.get_datapath_trait_key()
                    ));
                }
                _ => {
                    bail!("FieldType {:?} not supported by compiler", field.0.typ);
                }
            }
        } else {
            return Ok(base_type);
        }
    }

    pub fn get_rust_type(&self, field: FieldInfo) -> Result<String> {
        let mut type_params = match self.ref_counted_mode {
            true => match self.hybrid_mode {
                true => vec![
                    format!("'{}", &self.lifetime_name),
                    self.get_datapath_trait_key(),
                ],
                false => vec![
                    format!("'{}", self.lifetime_name),
                    self.get_datapath_trait_key(),
                ],
            },

            false => {
                vec![format!("'{}", self.lifetime_name)]
            }
        };
        let base_type = match &field.0.typ {
            FieldType::Int32 => "i32".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Uint32 => "u32".to_string(),
            FieldType::Uint64 => "u64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::String | FieldType::RefCountedString => {
                format!("CFString<{}>", type_params.join(", "))
            }
            FieldType::Bytes | FieldType::RefCountedBytes => {
                format!("CFBytes<{}>", type_params.join(", "))
            }
            FieldType::MessageOrEnum(msg_name) => {
                if self.hybrid_mode {
                    type_params.insert(0, "'arena".to_string());
                }
                let msg = match self.message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Field type: {} not in message_map", msg_name);
                    }
                };
                let mut params: Vec<String> = Vec::default();
                if msg.requires_lifetime(&self.message_map)? || self.hybrid_mode() {
                    params.push(format!("'{}", self.lifetime_name));
                }
                if msg.requires_datapath_type_param(self.ref_counted_mode, &self.message_map)? {
                    params.push(self.get_datapath_trait_key());
                }
                if params.len() > 0 {
                    format!("{}<{}>", msg_name, params.join(", "))
                } else {
                    format!("{}", msg_name)
                }
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };

        if self.ref_counted_mode {
            let mut params = match self.hybrid_mode {
                true => vec![base_type.clone(), self.datapath_trait_key.clone()],
                false => vec![
                    format!("'{}", self.lifetime_name),
                    base_type.clone(),
                    self.datapath_trait_key.clone(),
                ],
            };
            if field.is_list() {
                match &field.0.typ {
                    FieldType::Int32
                    | FieldType::Int64
                    | FieldType::Uint32
                    | FieldType::Uint64
                    | FieldType::Float => {
                        return Ok(format!("List<{}>", params.join(","),));
                    }
                    FieldType::String
                    | FieldType::Bytes
                    | FieldType::RefCountedBytes
                    | FieldType::RefCountedString
                    | FieldType::MessageOrEnum(_) => {
                        if self.hybrid_mode {
                            params.insert(0, "'arena".to_string());
                        }
                        return Ok(format!("VariableList<{}>", params.join(","),));
                    }
                    _ => {
                        bail!("FieldType {:?} not supported by compiler", field.0.typ);
                    }
                }
            } else {
                return Ok(base_type);
            }
        } else {
            if field.is_list() {
                let lifetime_name_to_add = match self.hybrid_mode {
                    false => format!("'{}, ", self.lifetime_name),
                    true => "".to_string(),
                };
                match &field.0.typ {
                    FieldType::Int32
                    | FieldType::Int64
                    | FieldType::Uint32
                    | FieldType::Uint64
                    | FieldType::Float => {
                        return Ok(format!("List<{}{}>", lifetime_name_to_add, base_type));
                    }
                    FieldType::String
                    | FieldType::Bytes
                    | FieldType::RefCountedBytes
                    | FieldType::RefCountedString
                    | FieldType::MessageOrEnum(_) => {
                        return Ok(format!(
                            "VariableList<{}{}>",
                            lifetime_name_to_add, base_type
                        ));
                    }
                    _ => {
                        bail!("FieldType {:?} not supported by compiler", field.0.typ);
                    }
                }
            } else {
                return Ok(base_type);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageInfo(pub Message);

impl MessageInfo {
    pub fn num_fields(&self) -> usize {
        self.0.fields.len()
    }

    pub fn get_num_u32_bitmaps(&self) -> usize {
        let num_fields = self.0.fields.len();
        // align up to 32 * 8 (e.g., number of fields that can fit in a 4-byte bitmap)
        let aligned_fields = align_up(num_fields, 32 * 8);
        let num_u32_bitmaps = aligned_fields / (32 * 8);
        num_u32_bitmaps
    }

    pub fn get_bitmap_size(&self) -> usize {
        let factor = ((self.0.fields.len() as f64) / (ALIGN_SIZE as f64)).ceil() as usize;
        factor * ALIGN_SIZE
    }

    pub fn get_bitmap_var_name(&self) -> String {
        let constant = &self.0.name.to_uppercase().to_string();
        format!("{}_BITMAP_SIZE", constant)
    }

    pub fn get_name(&self) -> String {
        self.0.name.clone()
    }

    pub fn get_fields(&self) -> Vec<Field> {
        self.0.fields.clone()
    }

    pub fn get_function_params_hybrid(&self, use_arena: bool) -> Result<Vec<String>> {
        let mut ret = vec![];
        if use_arena {
            ret.push("'arena".to_string());
        }
        Ok(ret)
    }

    pub fn get_function_params(&self, fd: &ProtoReprInfo) -> Result<Vec<String>> {
        let mut ret: Vec<String> = Vec::default();
        if fd.hybrid_mode() {
            if self.contains_variable_list(&fd.get_message_map())? {
                ret.insert(0, "'arena".to_string());
                // TODO: hack for c codegen
                ret.push(format!("{}: 'arena", fd.get_lifetime()));
            } else {
                ret.push(fd.get_lifetime());
            }
            return Ok(ret);
        }
        ret.push(fd.get_lifetime());
        Ok(ret)
    }

    pub fn get_type_params_hybrid_object_ffi(
        &self,
        use_arena: bool,
        datapath: &str,
    ) -> Result<Vec<String>> {
        let mut ret = vec![];
        if use_arena {
            ret.push("'arena".to_string());
        }
        ret.push(datapath.to_string());
        Ok(ret)
    }

    pub fn get_type_params_with_lifetime_ffi(
        &self,
        _is_ref_counted: bool,
        fd: &ProtoReprInfo,
        datapath: &str,
    ) -> Result<Vec<String>> {
        let mut ret: Vec<String> = Vec::default();
        if fd.hybrid_mode() {
            if self.contains_variable_list(&fd.get_message_map())? {
                ret.insert(0, "'arena".to_string());
            }
        }
        ret.push(fd.get_lifetime());
        ret.push(datapath.to_string());
        Ok(ret)
    }

    pub fn get_type_params_hybrid_object(
        &self,
        fd: &ProtoReprInfo,
        use_arena: bool,
    ) -> Result<Vec<String>> {
        let mut ret: Vec<String> = Vec::default();
        if use_arena {
            ret.push("'arena".to_string());
        }
        ret.push(fd.get_datapath_trait_key());
        Ok(ret)
    }

    pub fn get_type_params_with_lifetime(
        &self,
        is_ref_counted: bool,
        fd: &ProtoReprInfo,
    ) -> Result<Vec<String>> {
        let mut ret: Vec<String> = Vec::default();
        if fd.hybrid_mode() {
            if self.contains_variable_list(&fd.get_message_map())? {
                ret.insert(0, "'arena".to_string());
            }
        }
        ret.push(fd.get_lifetime());
        if is_ref_counted
            && (self.requires_datapath_type_param(is_ref_counted, &fd.get_message_map())?
                || fd.needs_datapath_param())
        {
            ret.push(fd.get_datapath_trait_key());
        }

        Ok(ret)
    }

    pub fn get_type_params(&self, is_ref_counted: bool, fd: &ProtoReprInfo) -> Result<Vec<String>> {
        let mut ret: Vec<String> = Vec::default();
        if self.requires_lifetime(&fd.get_message_map())? && !fd.hybrid_mode() {
            ret.push(fd.get_lifetime());
        }
        if self.requires_datapath_type_param(is_ref_counted, &fd.get_message_map())?
            || fd.hybrid_mode
        {
            ret.push(fd.get_datapath_trait_key());
        }

        Ok(ret)
    }

    /*pub fn get_where_clause_with_more_traits(
        &self,
        is_ref_counted: bool,
        fd: &ProtoReprInfo,
    ) -> Result<WhereClause> {
        if self.requires_datapath_type_param(is_ref_counted, &fd.get_message_map())? {
            return Ok(WhereClause::new(vec![WherePair::new(
                &fd.get_datapath_trait_key(),
                &format!(
                    "{} + std::fmt::Debug + Eq + PartialEq + Default + Clone",
                    &fd.get_datapath_trait()
                ),
            )]));
        } else {
            return Ok(WhereClause::default());
        }
    }*/

    pub fn get_where_clause_hybrid_object(&self, fd: &ProtoReprInfo) -> Result<WhereClause> {
        return Ok(WhereClause::new(vec![WherePair::new(
            &fd.get_datapath_trait_key(),
            &fd.get_datapath_trait(),
        )]));
    }

    pub fn get_where_clause(
        &self,
        is_ref_counted: bool,
        fd: &ProtoReprInfo,
    ) -> Result<WhereClause> {
        if is_ref_counted
            && (self.requires_datapath_type_param(is_ref_counted, &fd.get_message_map())?
                || fd.needs_datapath_param()
                || fd.hybrid_mode())
        {
            return Ok(WhereClause::new(vec![WherePair::new(
                &fd.get_datapath_trait_key(),
                &fd.get_datapath_trait(),
            )]));
        } else {
            return Ok(WhereClause::default());
        }
    }

    pub fn contains_variable_list(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.contains_list(message_map)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub fn requires_datapath_type_param(
        &self,
        is_ref_counted: bool,
        message_map: &HashMap<String, Message>,
    ) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.requires_datapath_type_param(is_ref_counted, message_map)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    // Does any field in this message require a lifetime?
    pub fn requires_lifetime(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.requires_lifetime(message_map)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub fn get_field_from_id(&self, id: i32) -> Result<FieldInfo> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.get_idx() == id {
                return Ok(field_info);
            }
        }
        bail!("Field info for idx {} not found", id);
    }

    pub fn get_constants_with_u32_bitmaps(
        &self,
        field: &FieldInfo,
        include_constant_offset: bool,
        ref_counted_mode: bool,
    ) -> Result<Vec<(String, String, String)>> {
        let mut ret: Vec<(String, String, String)> = Vec::default();
        let field_idx = field.get_idx();
        ret.push((
            field.get_bitmap_idx_str(false),
            "usize".to_string(),
            format!("{}", field_idx % (32 * 8)),
        ));
        ret.push((
            field.get_u32_bitmap_offset_str(false),
            "usize".to_string(),
            format!("{}", field_idx / (32 * 8)),
        ));
        if !field.is_list() {
            match field.0.typ {
                FieldType::Int32 | FieldType::Uint32 => {
                    let field_size = 4;
                    ret.push((
                        field.get_header_size_str(false, ref_counted_mode)?,
                        "usize".to_string(),
                        format!("{}", field_size),
                    ));
                }
                FieldType::Int64 | FieldType::Uint64 | FieldType::Float => {
                    let field_size = 8;
                    ret.push((
                        field.get_header_size_str(false, ref_counted_mode)?,
                        "usize".to_string(),
                        format!("{}", field_size),
                    ));
                }
                _ => {}
            }
        }
        if include_constant_offset {
            let mut offset_string = "Self::BITMAP_SIZE".to_string();
            for i in 0..field_idx {
                let preceeding_field = self.get_field_from_id(i)?;
                offset_string = format!(
                    "{} + {}",
                    offset_string,
                    preceeding_field.get_header_size_str(true, ref_counted_mode)?
                );
            }
            ret.push((
                field.get_header_offset_str(false),
                "usize".to_string(),
                offset_string,
            ));
        }

        Ok(ret)
    }

    pub fn get_constants(
        &self,
        field: &FieldInfo,
        include_constant_offset: bool,
        ref_counted_mode: bool,
    ) -> Result<Vec<(String, String, String)>> {
        let mut ret: Vec<(String, String, String)> = Vec::default();
        let field_idx = field.get_idx();
        ret.push((
            field.get_bitmap_idx_str(false),
            "usize".to_string(),
            format!("{}", field_idx),
        ));
        if !field.is_list() {
            match field.0.typ {
                FieldType::Int32 | FieldType::Uint32 => {
                    let field_size = 4;
                    ret.push((
                        field.get_header_size_str(false, ref_counted_mode)?,
                        "usize".to_string(),
                        format!("{}", field_size),
                    ));
                }
                FieldType::Int64 | FieldType::Uint64 | FieldType::Float => {
                    let field_size = 8;
                    ret.push((
                        field.get_header_size_str(false, ref_counted_mode)?,
                        "usize".to_string(),
                        format!("{}", field_size),
                    ));
                }
                _ => {}
            }
        }
        if include_constant_offset {
            let mut offset_string = "Self::BITMAP_SIZE".to_string();
            for i in 0..field_idx {
                let preceeding_field = self.get_field_from_id(i)?;
                offset_string = format!(
                    "{} + {}",
                    offset_string,
                    preceeding_field.get_header_size_str(true, ref_counted_mode)?
                );
            }
            ret.push((
                field.get_header_offset_str(false),
                "usize".to_string(),
                offset_string,
            ));
        }

        Ok(ret)
    }

    pub fn derives_copy(
        &self,
        message_map: &HashMap<String, Message>,
        is_ref_counted: bool,
    ) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if !field_info.derives_copy(message_map, is_ref_counted)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    pub fn refers_to_bytes(&self, msg_map: &HashMap<String, Message>) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.refers_to_bytes(msg_map)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub fn has_only_int_fields(
        &self,
        include_nested: bool,
        msg_map: &HashMap<String, Message>,
    ) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.is_int() {
                return Ok(true);
            }
            if include_nested && field_info.is_nested_msg() {
                match field_info.0.typ {
                    FieldType::MessageOrEnum(msg_name) => match msg_map.get(&msg_name) {
                        Some(m) => {
                            let msg_info = MessageInfo(m.clone());
                            if msg_info.has_only_int_fields(include_nested, &msg_map)? {
                                return Ok(true);
                            }
                        }
                        None => {
                            bail!("Message name not found in map: {}", msg_name);
                        }
                    },
                    _ => unreachable!(),
                }
            }
        }
        return Ok(false);
    }

    pub fn has_dynamic_fields(
        &self,
        include_nested: bool,
        msg_map: &HashMap<String, Message>,
    ) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if field_info.is_list() {
                return Ok(true);
            }
            // for case where header format considers nested field as something "dynamic"
            if include_nested && field_info.is_nested_msg() {
                return Ok(true);
            }
            if !include_nested && field_info.is_nested_msg() {
                match field_info.0.typ {
                    FieldType::MessageOrEnum(msg_name) => match msg_map.get(&msg_name) {
                        Some(m) => {
                            let msg_info = MessageInfo(m.clone());
                            if msg_info.has_dynamic_fields(include_nested, &msg_map)? {
                                return Ok(true);
                            }
                        }
                        None => {
                            bail!("Message name not found in map: {}", msg_name);
                        }
                    },
                    _ => unreachable!(),
                }
            }
        }
        return Ok(false);
    }

    pub fn constant_fields_left(&self, field_idx: i32) -> usize {
        self.num_fields() - ((field_idx + 1) as usize)
    }

    pub fn num_string_or_bytes_fields_left(&self, field_idx: i32) -> Result<usize> {
        let mut sum = 0;
        for idx in (field_idx + 1)..self.num_fields() as i32 {
            let field_info = self.get_field_from_id(idx)?;
            if field_info.is_bytes_or_string() {
                sum += 1;
            }
        }
        Ok(sum)
    }
    pub fn string_or_bytes_fields_left(&self, field_idx: i32) -> Result<bool> {
        for idx in (field_idx + 1)..self.num_fields() as i32 {
            let field_info = self.get_field_from_id(idx)?;
            if field_info.is_bytes_or_string() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn int_fields_left(&self, field_idx: i32) -> Result<bool> {
        for idx in (field_idx + 1)..self.num_fields() as i32 {
            let field_info = self.get_field_from_id(idx)?;
            if field_info.is_int() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn dynamic_fields_left(
        &self,
        field_idx: i32,
        include_nested: bool,
        msg_map: &HashMap<String, Message>,
    ) -> Result<usize> {
        let mut num_left: usize = 0;
        for idx in (field_idx + 1)..self.num_fields() as i32 {
            let field_info = self.get_field_from_id(idx)?;
            if field_info.is_list() || (include_nested && field_info.is_nested_msg()) {
                num_left += 1;
            }
            if !include_nested && field_info.is_nested_msg() {
                match field_info.0.typ {
                    FieldType::MessageOrEnum(msg_name) => match msg_map.get(&msg_name) {
                        Some(m) => {
                            let msg_info = MessageInfo(m.clone());
                            if msg_info.has_dynamic_fields(include_nested, &msg_map)? {
                                num_left += 1;
                            }
                        }
                        None => {
                            bail!("Message name not found in map: {}", msg_name);
                        }
                    },
                    _ => unreachable!(),
                }
            }
        }
        Ok(num_left)
    }
}

#[derive(Debug, Clone)]
pub struct FieldInfo(pub Field);

impl FieldInfo {
    pub fn get_base_type_str(&self) -> Result<String> {
        let base_type = match &self.0.typ {
            FieldType::Int32 => "i32".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Uint32 => "u32".to_string(),
            FieldType::Uint64 => "u64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::String => "CFString".to_string(),
            FieldType::Bytes => "CFBytes".to_string(),
            FieldType::RefCountedString => "CFString".to_string(),
            FieldType::RefCountedBytes => "CFBytes".to_string(),
            FieldType::MessageOrEnum(msg_name) => msg_name.clone(),
            _ => {
                bail!("FieldType {:?} not supported by compiler", self.0.typ);
            }
        };
        Ok(base_type)
    }

    pub fn get_u32_bitmap_offset_str(&self, with_self: bool) -> String {
        let self_str = match with_self {
            true => "Self::",
            false => "",
        };
        let mut ret = format!("{}_BITMAP_OFFSET", self.0.name).to_uppercase();
        ret = format!("{}{}", self_str, ret);
        ret
    }

    pub fn get_bitmap_idx_str(&self, with_self: bool) -> String {
        let self_str = match with_self {
            true => "Self::",
            false => "",
        };
        let mut ret = format!("{}_BITMAP_IDX", self.0.name).to_uppercase();
        ret = format!("{}{}", self_str, ret);
        ret
    }

    pub fn get_header_offset_str(&self, with_self: bool) -> String {
        let self_str = match with_self {
            true => "Self::",
            false => "",
        };

        let mut ret = format!("{}_HEADER_OFFSET", self.0.name).to_uppercase();
        ret = format!("{}{}", self_str, ret);
        ret
    }

    pub fn is_ref_counted(&self) -> bool {
        match &self.0.typ {
            FieldType::RefCountedBytes | FieldType::RefCountedString => true,
            _ => false,
        }
    }

    pub fn is_int(&self) -> bool {
        if self.is_list() {
            return false;
        }
        match &self.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float => true,
            _ => false,
        }
    }

    pub fn is_int_list(&self) -> bool {
        if !self.is_list() {
            return false;
        }
        match &self.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float => true,
            _ => false,
        }
    }

    pub fn contains_list(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        if self.is_list() {
            // TODO: int list here will produce the wrong code
            return Ok(true);
        }

        match &self.0.typ {
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Msg name: {} not found in message map.", msg_name);
                    }
                };
                return msg.contains_variable_list(message_map);
            }
            _ => {
                return Ok(false);
            }
        }
    }

    pub fn get_total_header_size_str_hybrid(
        &self,
        with_self: bool,
        with_pointer_size: bool,
    ) -> Result<String> {
        let with_pointer_size_str = match with_pointer_size {
            true => "true",
            false => "",
        };
        if !self.is_list() {
            match &self.0.typ {
                FieldType::Int32
                | FieldType::Int64
                | FieldType::Uint32
                | FieldType::Uint64
                | FieldType::Float => self.get_header_size_str(with_self, true),
                FieldType::Bytes
                | FieldType::String
                | FieldType::RefCountedBytes
                | FieldType::RefCountedString => Ok(format!(
                    "self.{}.total_header_size({})",
                    self.get_name(),
                    with_pointer_size_str,
                )),
                FieldType::MessageOrEnum(_) => Ok(format!(
                    "self.{}.total_header_size({})",
                    self.get_name(),
                    with_pointer_size_str,
                )),
                _ => {
                    bail!("FieldType {:?} not supported by compiler", self.0.typ);
                }
            }
        } else {
            Ok(format!(
                "self.{}.total_header_size({})",
                self.get_name(),
                with_pointer_size_str,
            ))
        }
    }

    pub fn get_total_header_size_str(
        &self,
        with_self: bool,
        ref_counted_mode: bool,
        with_pointer_size: bool,
        include_bitmap: bool,
    ) -> Result<String> {
        let with_pointer_size_str = match with_pointer_size {
            true => "true",
            false => "",
        };

        let with_bitmap_size_str = match include_bitmap {
            false => "",
            true => {
                if !self.is_list() && !ref_counted_mode && {
                    match self.0.typ {
                        FieldType::MessageOrEnum(_) => true,
                        _ => false,
                    }
                } {
                    ", true"
                } else {
                    ", false"
                }
            }
        };

        if !self.is_list() {
            match &self.0.typ {
                FieldType::Int32
                | FieldType::Int64
                | FieldType::Uint32
                | FieldType::Uint64
                | FieldType::Float => self.get_header_size_str(with_self, ref_counted_mode),
                FieldType::Bytes
                | FieldType::String
                | FieldType::RefCountedBytes
                | FieldType::RefCountedString => Ok(format!(
                    "self.{}.total_header_size({} {})",
                    self.get_name(),
                    with_pointer_size_str,
                    with_bitmap_size_str,
                )),
                FieldType::MessageOrEnum(_) => Ok(format!(
                    "self.{}.total_header_size({} {})",
                    self.get_name(),
                    with_pointer_size_str,
                    with_bitmap_size_str,
                )),
                _ => {
                    bail!("FieldType {:?} not supported by compiler", self.0.typ);
                }
            }
        } else {
            Ok(format!(
                "self.{}.total_header_size({} {})",
                self.get_name(),
                with_pointer_size_str,
                with_bitmap_size_str,
            ))
        }
    }

    pub fn get_header_size_str_ref_counted(&self, with_self: bool) -> Result<String> {
        let self_str = match with_self {
            true => "Self::",
            false => "",
        };
        match &self.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float => {
                if self.is_list() {
                    Ok(format!(
                        "List::<{}>::CONSTANT_HEADER_SIZE",
                        self.get_base_type_str()?
                    ))
                } else {
                    Ok(format!(
                        "{}{}",
                        self_str,
                        format!("{}_HEADER_SIZE", self.0.name).to_uppercase()
                    ))
                }
            }
            FieldType::String | FieldType::RefCountedString => {
                if self.is_list() {
                    Ok(format!(
                        "VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE",
                    ))
                } else {
                    Ok(format!("CFString::<D>::CONSTANT_HEADER_SIZE"))
                }
            }
            FieldType::Bytes | FieldType::RefCountedBytes => {
                if self.is_list() {
                    Ok(format!(
                        "VariableList::<CFBytes<D>, D>::CONSTANT_HEADER_SIZE",
                    ))
                } else {
                    Ok(format!("CFBytes::<D>::CONSTANT_HEADER_SIZE",))
                }
            }
            FieldType::MessageOrEnum(msg_name) => {
                if self.is_list() {
                    Ok(format!(
                        "VariableList::<{}<D>, D>::CONSTANT_HEADER_SIZE",
                        msg_name
                    ))
                } else {
                    Ok(format!("{}::<D>::CONSTANT_HEADER_SIZE", msg_name))
                }
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", self.0.typ);
            }
        }
    }

    pub fn get_header_size_str(&self, with_self: bool, is_ref_counted: bool) -> Result<String> {
        if is_ref_counted {
            return self.get_header_size_str_ref_counted(with_self);
        }
        let self_str = match with_self {
            true => "Self::",
            false => "",
        };
        match &self.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float => {
                if self.is_list() {
                    Ok(format!(
                        "List::<{}>::CONSTANT_HEADER_SIZE",
                        self.get_base_type_str()?
                    ))
                } else {
                    Ok(format!(
                        "{}{}",
                        self_str,
                        format!("{}_HEADER_SIZE", self.0.name).to_uppercase()
                    ))
                }
            }
            FieldType::String | FieldType::RefCountedString => {
                if self.is_list() {
                    Ok(format!("VariableList::<CFString>::CONSTANT_HEADER_SIZE",))
                } else {
                    Ok(format!("CFString::CONSTANT_HEADER_SIZE"))
                }
            }
            FieldType::Bytes | FieldType::RefCountedBytes => {
                if self.is_list() {
                    Ok(format!("VariableList::<CFBytes>::CONSTANT_HEADER_SIZE",))
                } else {
                    Ok(format!("CFBytes::CONSTANT_HEADER_SIZE",))
                }
            }
            FieldType::MessageOrEnum(msg_name) => {
                if self.is_list() {
                    Ok(format!(
                        "VariableList::<{}>::CONSTANT_HEADER_SIZE",
                        msg_name
                    ))
                } else {
                    Ok(format!("{}::CONSTANT_HEADER_SIZE", msg_name))
                }
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", self.0.typ);
            }
        }
    }

    pub fn get_idx(&self) -> i32 {
        self.0.number - 1
    }
    pub fn get_name(&self) -> String {
        self.0.name.clone()
    }

    pub fn is_list(&self) -> bool {
        match self.0.rule {
            Rule::Repeated => true,
            _ => false,
        }
    }

    pub fn get_type(&self) -> FieldType {
        return self.0.typ.clone();
    }

    pub fn is_bytes_or_string(&self) -> bool {
        if self.is_list() {
            return false;
        }
        match self.0.typ {
            FieldType::String
            | FieldType::Bytes
            | FieldType::RefCountedString
            | FieldType::RefCountedBytes => true,
            _ => false,
        }
    }

    pub fn derives_copy(
        &self,
        message_map: &HashMap<String, Message>,
        is_ref_counted: bool,
    ) -> Result<bool> {
        if self.is_list() {
            return Ok(false);
        }
        if is_ref_counted {
            match &self.0.typ {
                FieldType::RefCountedBytes
                | FieldType::RefCountedString
                | FieldType::Bytes
                | FieldType::String => {
                    return Ok(false);
                }
                _ => {}
            }
        }
        match &self.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float
            | FieldType::String
            | FieldType::Bytes => Ok(true),
            FieldType::RefCountedBytes | FieldType::RefCountedString => {
                bail!("Cannot have ref counted bytes or string when ref counted bool is not true");
            }
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Msg name: {} not found in message map.", msg_name);
                    }
                };
                msg.derives_copy(message_map, is_ref_counted)
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", self.0.typ);
            }
        }
    }

    pub fn refers_to_bytes(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        match &self.0.typ {
            FieldType::String
            | FieldType::Bytes
            | FieldType::RefCountedString
            | FieldType::RefCountedBytes => Ok(true),
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Msg name: {} not found in message map.", msg_name);
                    }
                };
                msg.refers_to_bytes(message_map)
            }
            _ => Ok(false),
        }
    }

    pub fn requires_datapath_type_param(
        &self,
        is_ref_counted: bool,
        message_map: &HashMap<String, Message>,
    ) -> Result<bool> {
        if !is_ref_counted {
            return Ok(false);
        }
        if self.is_list() {
            return Ok(true);
        }

        match &self.0.typ {
            FieldType::String
            | FieldType::Bytes
            | FieldType::RefCountedString
            | FieldType::RefCountedBytes => Ok(true),
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Msg name: {} not found in message map.", msg_name);
                    }
                };
                msg.requires_datapath_type_param(is_ref_counted, message_map)
            }
            _ => Ok(false),
        }
    }

    // type requires a lifetime if it is CFBytes, CFString, or some sort of list
    pub fn requires_lifetime(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        if self.is_list() {
            return Ok(true);
        }

        match &self.0.typ {
            FieldType::String
            | FieldType::Bytes
            | FieldType::RefCountedBytes
            | FieldType::RefCountedString => Ok(true),
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Msg name: {} not found in message map.", msg_name);
                    }
                };
                msg.requires_lifetime(message_map)
            }
            _ => Ok(false),
        }
    }

    pub fn is_dynamic(
        &self,
        include_nested: bool,
        message_map: &HashMap<String, Message>,
    ) -> Result<bool> {
        if self.is_list() {
            return Ok(true);
        }

        if self.is_nested_msg() {
            if include_nested {
                return Ok(true);
            }

            match &self.0.typ {
                FieldType::MessageOrEnum(msg_name) => {
                    let msg = match message_map.get(msg_name.as_str()) {
                        Some(m) => MessageInfo(m.clone()),
                        None => {
                            bail!("Msg name: {} not found in message map.", msg_name);
                        }
                    };
                    return msg.has_dynamic_fields(include_nested, message_map);
                }
                _ => {
                    unreachable!();
                }
            }
        }
        Ok(false)
    }

    pub fn is_nested_msg(&self) -> bool {
        if self.is_list() {
            return false;
        }
        match &self.0.typ {
            FieldType::MessageOrEnum(_) => true,
            _ => false,
        }
    }
}
