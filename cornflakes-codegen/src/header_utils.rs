use color_eyre::eyre::{bail, Result};
use protobuf_parser::{Field, FieldType, FileDescriptor, Message, Rule};
use std::collections::HashMap;
use std::path::PathBuf;

const ALIGN_SIZE: usize = 8;
static LIFETIME_NAME: &str = "registered";

#[derive(Debug, Clone)]
pub struct ProtoReprInfo {
    repr: FileDescriptor,
    message_map: HashMap<String, Message>,
    lifetime_name: String,
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
        }
    }

    pub fn get_message_map(&self) -> &HashMap<String, Message> {
        &self.message_map
    }

    pub fn get_lifetime(&self) -> String {
        self.lifetime_name.clone()
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

    pub fn get_output_file(&self, folder: &str) -> PathBuf {
        let mut pathbuf = PathBuf::new();
        if folder != "" {
            pathbuf.push(folder);
        }
        pathbuf.push(&format!("{}.rs", self.repr.package));
        pathbuf
    }

    pub fn get_default_type(&self, field: FieldInfo) -> Result<String> {
        let default_val = match &field.0.typ {
            FieldType::Int32 | FieldType::Int64 | FieldType::Uint32 | FieldType::Uint64 => {
                "0".to_string()
            }
            FieldType::Float => "0.0".to_string(),
            FieldType::String => "CFString::default()".to_string(),
            FieldType::Bytes => "CFBytes::default()".to_string(),
            FieldType::MessageOrEnum(msg_name) => {
                format!("{}::default()", msg_name)
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", field.0.typ);
            }
        };
        Ok(default_val)
    }

    pub fn get_rust_type(&self, field: FieldInfo) -> Result<String> {
        let base_type = match &field.0.typ {
            FieldType::Int32 => "i32".to_string(),
            FieldType::Int64 => "i64".to_string(),
            FieldType::Uint32 => "u32".to_string(),
            FieldType::Uint64 => "u64".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::String => format!("CFString<'{}>", self.lifetime_name),
            FieldType::Bytes => format!("CFBytes<'{}>", self.lifetime_name),
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match self.message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Field type: {} not in message_map", msg_name);
                    }
                };
                if msg.requires_lifetime(&self.message_map)? {
                    format!("{}<'{}>", msg_name, self.lifetime_name)
                } else {
                    format!("{}", msg_name)
                }
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
                    return Ok(format!("List<'{}, {}>", self.lifetime_name, base_type));
                }
                FieldType::String | FieldType::Bytes | FieldType::MessageOrEnum(_) => {
                    return Ok(format!(
                        "VariableList<'{}, {}>",
                        self.lifetime_name, base_type
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

#[derive(Debug, Clone)]
pub struct MessageInfo(pub Message);

impl MessageInfo {
    pub fn get_bitmap_size(&self) -> usize {
        let factor = ((self.0.fields.len() as f64) / (ALIGN_SIZE as f64)).ceil() as usize;
        factor * ALIGN_SIZE
    }

    pub fn get_name(&self) -> String {
        self.0.name.clone()
    }

    pub fn get_fields(&self) -> Vec<Field> {
        self.0.fields.clone()
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

    pub fn get_constants(
        &self,
        field: &FieldInfo,
        include_constant_offset: bool,
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
                        field.get_header_size_str(false)?,
                        "usize".to_string(),
                        format!("{}", field_size),
                    ));
                }
                FieldType::Int64 | FieldType::Uint64 | FieldType::Float => {
                    let field_size = 8;
                    ret.push((
                        field.get_header_size_str(false)?,
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
                    preceeding_field.get_header_size_str(true)?
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

    pub fn derives_copy(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        for field in self.0.fields.iter() {
            let field_info = FieldInfo(field.clone());
            if !field_info.derives_copy(message_map)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    pub fn num_fields(&self) -> usize {
        self.0.fields.len()
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
            FieldType::MessageOrEnum(msg_name) => msg_name.clone(),
            _ => {
                bail!("FieldType {:?} not supported by compiler", self.0.typ);
            }
        };
        Ok(base_type)
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

    pub fn get_total_header_size_str(&self, with_self: bool) -> Result<String> {
        if !self.is_list() {
            match &self.0.typ {
                FieldType::Int32
                | FieldType::Int64
                | FieldType::Uint32
                | FieldType::Uint64
                | FieldType::Float => self.get_header_size_str(with_self),
                FieldType::Bytes | FieldType::String => {
                    Ok(format!("self.{}.total_header_size()", self.get_name()))
                }
                FieldType::MessageOrEnum(_) => {
                    Ok(format!("self.{}.total_header_size()", self.get_name()))
                }
                _ => {
                    bail!("FieldType {:?} not supported by compiler", self.0.typ);
                }
            }
        } else {
            Ok(format!("self.{}.total_header_size()", self.get_name()))
        }
    }

    pub fn get_header_size_str(&self, with_self: bool) -> Result<String> {
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
            FieldType::String => {
                if self.is_list() {
                    Ok(format!("VariableList::<CFString>::CONSTANT_HEADER_SIZE",))
                } else {
                    Ok(format!("CFString::CONSTANT_HEADER_SIZE"))
                }
            }
            FieldType::Bytes => {
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

    pub fn is_bytes_or_string(&self) -> bool {
        if self.is_list() {
            return false;
        }
        match self.0.typ {
            FieldType::String | FieldType::Bytes => true,
            _ => false,
        }
    }

    pub fn derives_copy(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        if self.is_list() {
            return Ok(false);
        }
        match &self.0.typ {
            FieldType::Int32
            | FieldType::Int64
            | FieldType::Uint32
            | FieldType::Uint64
            | FieldType::Float
            | FieldType::String
            | FieldType::Bytes => Ok(true),
            FieldType::MessageOrEnum(msg_name) => {
                let msg = match message_map.get(msg_name.as_str()) {
                    Some(m) => MessageInfo(m.clone()),
                    None => {
                        bail!("Msg name: {} not found in message map.", msg_name);
                    }
                };
                msg.derives_copy(message_map)
            }
            _ => {
                bail!("FieldType {:?} not supported by compiler", self.0.typ);
            }
        }
    }

    // type requires a lifetime if it is CFBytes, CFString, or some sort of list
    pub fn requires_lifetime(&self, message_map: &HashMap<String, Message>) -> Result<bool> {
        if self.is_list() {
            return Ok(true);
        }

        match &self.0.typ {
            FieldType::String | FieldType::Bytes => Ok(true),
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
