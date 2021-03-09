use super::cf_utils::*;
use byteorder::{ByteOrder, LittleEndian};
use cornflakes_libos::CornPtr;
use std::slice;

// test data structure (in protobuf format):
// message TestObject {
//  int32 int_field = 1;
//  repeated int32 int_list_field = 2;
//  repeated bytes bytes_list_field = 3;
//  string  string_field = 4;
//  NestedObject1 nested_field = 5;
//  repeated NestedObject2 nested_list = 6;
// }
//
// message NestedObject1 {
//   bytes field1 = 1;
//   NestedObject2 field2 = 2;
// }
//
// message NestedObject2 {
//  bytes field1 = 1;
// }

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TestObject<'registered> {
    bitmap: Vec<u8>,
    int_field: i32,
    int_list_field_ptr: List<'registered, i32>,
    bytes_list_field_ptr: VariableList<'registered, CFBytes<'registered>>,
    string_field_ptr: CFString<'registered>,
    nested_field_ptr: NestedObject1<'registered>,
    nested_list_field_ptr: VariableList<'registered, NestedObject2<'registered>>,
}

impl<'registered> TestObject<'registered> {
    const BITMAP_SIZE: usize = 8; // align up to 8
    const INT_FIELD_BITMAP_IDX: usize = 0;
    const INT_LIST_FIELD_BITMAP_IDX: usize = 1;
    const BYTES_LIST_FIELD_BITMAP_IDX: usize = 2;
    const STRING_FIELD_BITMAP_IDX: usize = 3;
    const NESTED_FIELD_BITMAP_IDX: usize = 4;
    const NESTED_LIST_BITMAP_IDX: usize = 5;

    const INT_FIELD_HEADER_SIZE: usize = 4;

    // Initialize with a slice to hold bytes
    pub fn new() -> TestObject<'registered> {
        TestObject {
            bitmap: vec![0u8; Self::BITMAP_SIZE],
            int_field: 0,
            int_list_field_ptr: List::default(),
            bytes_list_field_ptr: VariableList::default(),
            string_field_ptr: CFString::default(),
            nested_field_ptr: NestedObject1::default(),
            nested_list_field_ptr: VariableList::default(),
        }
    }

    pub fn has_int_field(&self) -> bool {
        self.bitmap[Self::INT_FIELD_BITMAP_IDX] == 1
    }

    pub fn get_int_field(&self) -> i32 {
        self.int_field
    }

    pub fn set_int_field(&mut self, val: i32) {
        self.int_field = val;
        self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] = 1;
    }

    pub fn has_int_list_field(&self) -> bool {
        self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] == 1
    }

    pub fn get_int_list_field(&self) -> &List<'registered, i32> {
        &self.int_list_field_ptr
    }

    pub fn get_mut_int_list_field(&mut self) -> &mut List<'registered, i32> {
        &mut self.int_list_field_ptr
    }
    pub fn set_int_list_field(&mut self, list: List<'registered, i32>) {
        self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] = 1;
        self.int_list_field_ptr = list;
    }

    pub fn init_int_list_field(&mut self, num: usize) {
        self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] = 1;
        self.int_list_field_ptr = List::init(num);
    }

    pub fn has_bytes_list_field(&self) -> bool {
        self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] == 1
    }

    pub fn get_bytes_list_field(&self) -> &VariableList<'registered, CFBytes<'registered>> {
        &self.bytes_list_field_ptr
    }

    pub fn get_mut_bytes_list_field(
        &mut self,
    ) -> &mut VariableList<'registered, CFBytes<'registered>> {
        &mut self.bytes_list_field_ptr
    }

    pub fn set_bytes_list_field(&mut self, list: VariableList<'registered, CFBytes<'registered>>) {
        self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] = 1;
        self.bytes_list_field_ptr = list;
    }

    pub fn init_bytes_list_field(&mut self, num: usize) {
        self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] = 1;
        self.bytes_list_field_ptr = VariableList::init(num);
    }

    pub fn has_string_field(&self) -> bool {
        self.bitmap[Self::STRING_FIELD_BITMAP_IDX] == 1
    }

    pub fn get_string_field(&self) -> CFString<'registered> {
        self.string_field_ptr
    }

    pub fn set_string_field(&mut self, field: CFString<'registered>) {
        self.bitmap[Self::STRING_FIELD_BITMAP_IDX] = 1;
        self.string_field_ptr = field;
    }

    pub fn has_nested_field(&self) -> bool {
        self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] == 1
    }

    pub fn get_nested_field(&self) -> &NestedObject1<'registered> {
        &self.nested_field_ptr
    }

    pub fn get_mut_nested_field(&mut self) -> &mut NestedObject1<'registered> {
        // assumes that `get_mut` will modify / set the object in some way
        self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] = 1;
        &mut self.nested_field_ptr
    }

    pub fn set_nested_field(&mut self, obj: NestedObject1<'registered>) {
        self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] = 1;
        self.nested_field_ptr = obj;
    }

    pub fn has_nested_list_field(&self) -> bool {
        self.bitmap[Self::NESTED_LIST_BITMAP_IDX] == 1
    }

    pub fn get_nested_list_field(&self) -> &VariableList<'registered, NestedObject2<'registered>> {
        &self.nested_list_field_ptr
    }

    pub fn get_mut_nested_list_field(
        &mut self,
    ) -> &mut VariableList<'registered, NestedObject2<'registered>> {
        &mut self.nested_list_field_ptr
    }

    pub fn set_nested_list_field(
        &mut self,
        list: VariableList<'registered, NestedObject2<'registered>>,
    ) {
        self.bitmap[Self::NESTED_LIST_BITMAP_IDX] = 1;
        self.nested_list_field_ptr = list;
    }

    pub fn init_nested_list_field(&mut self, num: usize) {
        self.bitmap[Self::NESTED_LIST_BITMAP_IDX] = 1;
        self.nested_list_field_ptr = VariableList::init(num);
    }

    /// Initializes a header _buffer large enough to store the entire serialization header.
    pub fn init_header_buffer(&self) -> Vec<u8> {
        Vec::with_capacity(self.dynamic_header_size())
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] as usize * Self::INT_FIELD_HEADER_SIZE
            + self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] as usize
                * List::<i32>::CONSTANT_HEADER_SIZE
            + self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] as usize
                * VariableList::<CFBytes>::CONSTANT_HEADER_SIZE
            + self.bitmap[Self::STRING_FIELD_BITMAP_IDX] as usize * CFString::CONSTANT_HEADER_SIZE
            + self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] as usize
                * NestedObject1::CONSTANT_HEADER_SIZE
            + self.bitmap[Self::NESTED_LIST_BITMAP_IDX] as usize
                * VariableList::<NestedObject2>::CONSTANT_HEADER_SIZE
    }
}

impl<'registered> HeaderRepr<'registered> for TestObject<'registered> {
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] as usize * Self::INT_FIELD_HEADER_SIZE
            + self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] as usize
                * self.int_list_field_ptr.total_header_size()
            + self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] as usize
                * self.bytes_list_field_ptr.total_header_size()
            + self.bitmap[Self::STRING_FIELD_BITMAP_IDX] as usize
                * self.string_field_ptr.total_header_size()
            + self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] as usize
                * self.nested_field_ptr.total_header_size()
            + self.bitmap[Self::NESTED_LIST_BITMAP_IDX] as usize
                * self.nested_list_field_ptr.total_header_size()
    }

    fn inner_serialize<'normal>(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();

        let mut cur_header_off = header_ptr;
        let mut cur_dynamic_off =
            unsafe { header_ptr.offset(self.dynamic_header_offset() as isize) };
        let mut cur_dynamic_off_size = offset + self.dynamic_header_offset();

        unsafe {
            copy_func(
                cur_header_off as _,
                self.bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            )
        };
        cur_header_off = unsafe { cur_header_off.offset(Self::BITMAP_SIZE as isize) };

        // for each field, if field is there, copy in constant reference in header
        // if requires dynamic serialization, run inner serialize with dynamic offset
        // append any pointers to the the return vector
        if self.has_int_field() {
            LittleEndian::write_i32(
                unsafe { slice::from_raw_parts_mut(cur_header_off, 4) },
                self.int_field,
            );
            cur_header_off = unsafe { cur_header_off.offset(Self::INT_FIELD_HEADER_SIZE as isize) };
        }

        if self.has_int_list_field() {
            let mut int_list_field_ref = ObjectRef(cur_header_off as _);
            int_list_field_ref.write_size(self.int_list_field_ptr.size());
            int_list_field_ref.write_offset(cur_dynamic_off_size);
            cur_header_off =
                unsafe { cur_header_off.offset(List::<i32>::CONSTANT_HEADER_SIZE as isize) };
            cur_dynamic_off = unsafe {
                cur_dynamic_off.offset(self.int_list_field_ptr.dynamic_header_size() as isize)
            };
            cur_dynamic_off_size += self.int_list_field_ptr.dynamic_header_size();
        }

        if self.has_bytes_list_field() {
            let mut bytes_list_field_ref = ObjectRef(cur_header_off as _);
            bytes_list_field_ref.write_size(self.bytes_list_field_ptr.size());
            bytes_list_field_ref.write_offset(cur_dynamic_off_size);
            cur_header_off = unsafe {
                cur_header_off.offset(VariableList::<CFBytes>::CONSTANT_HEADER_SIZE as isize)
            };
            ret.append(&mut self.bytes_list_field_ptr.inner_serialize(
                cur_dynamic_off,
                copy_func,
                cur_dynamic_off_size,
            ));
            cur_dynamic_off = unsafe {
                cur_dynamic_off.offset(self.bytes_list_field_ptr.dynamic_header_size() as isize)
            };
            cur_dynamic_off_size += self.bytes_list_field_ptr.dynamic_header_size();
        }

        if self.has_string_field() {
            ret.append(
                &mut self
                    .string_field_ptr
                    .inner_serialize(cur_header_off, copy_func, 0),
            );
            cur_header_off =
                unsafe { cur_header_off.offset(CFString::CONSTANT_HEADER_SIZE as isize) };
        }

        if self.has_nested_field() {
            let mut nested_field_ref = ObjectRef(cur_header_off as _);
            nested_field_ref.write_size(self.nested_field_ptr.dynamic_header_size());
            nested_field_ref.write_offset(cur_dynamic_off_size);
            ret.append(&mut self.nested_field_ptr.inner_serialize(
                cur_dynamic_off,
                copy_func,
                cur_dynamic_off_size,
            ));
            cur_header_off =
                unsafe { cur_header_off.offset(NestedObject1::CONSTANT_HEADER_SIZE as isize) };
            cur_dynamic_off = unsafe {
                cur_dynamic_off.offset(self.nested_field_ptr.dynamic_header_size() as isize)
            };
            cur_dynamic_off_size += self.nested_field_ptr.dynamic_header_size();
        }

        if self.has_nested_list_field() {
            let mut nested_list_field_ref = ObjectRef(cur_header_off as _);
            nested_list_field_ref.write_size(self.nested_list_field_ptr.size());
            nested_list_field_ref.write_offset(cur_dynamic_off_size);
            ret.append(&mut self.nested_list_field_ptr.inner_serialize(
                cur_dynamic_off,
                copy_func,
                cur_dynamic_off_size,
            ));
        }

        ret
    }

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        assert!(size >= Self::BITMAP_SIZE);
        self.bitmap = vec![0u8; Self::BITMAP_SIZE];
        let bitmap_slice = unsafe { slice::from_raw_parts(buf, Self::BITMAP_SIZE) };

        let mut cur_header_off = unsafe { buf.offset(Self::BITMAP_SIZE as isize) };
        let mut cur_constant_offset = Self::BITMAP_SIZE;
        if bitmap_slice[Self::INT_FIELD_BITMAP_IDX] == 1 {
            self.bitmap[Self::INT_FIELD_BITMAP_IDX];
            self.int_field =
                LittleEndian::read_i32(unsafe { slice::from_raw_parts(cur_header_off, 4) });
            cur_header_off = unsafe { cur_header_off.offset(Self::INT_FIELD_HEADER_SIZE as isize) };
            cur_constant_offset += Self::INT_FIELD_HEADER_SIZE;
        }

        if bitmap_slice[Self::INT_LIST_FIELD_BITMAP_IDX] == 1 {
            self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] = 1;
            let object_ref = ObjectRef(cur_header_off);
            let cur_dynamic_off = object_ref.get_offset() - relative_offset;
            let cur_dynamic_ptr = unsafe { buf.offset(cur_dynamic_off as isize) };
            self.int_list_field_ptr.inner_deserialize(
                cur_dynamic_ptr,
                object_ref.get_size(),
                object_ref.get_offset(),
            );
            cur_header_off =
                unsafe { cur_header_off.offset(List::<i32>::CONSTANT_HEADER_SIZE as isize) };
            cur_constant_offset += List::<i32>::CONSTANT_HEADER_SIZE;
        }

        if bitmap_slice[Self::BYTES_LIST_FIELD_BITMAP_IDX] == 1 {
            self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] = 1;
            let object_ref = ObjectRef(cur_header_off);
            let cur_dynamic_off = object_ref.get_offset() - relative_offset;
            let cur_dynamic_ptr = unsafe { buf.offset(cur_dynamic_off as isize) };
            self.bytes_list_field_ptr.inner_deserialize(
                cur_dynamic_ptr,
                object_ref.get_size(),
                object_ref.get_offset(),
            );
        }

        if bitmap_slice[Self::STRING_FIELD_BITMAP_IDX] == 1 {
            self.bitmap[Self::STRING_FIELD_BITMAP_IDX] = 1;
            self.string_field_ptr.inner_deserialize(
                cur_header_off,
                CFString::CONSTANT_HEADER_SIZE,
                relative_offset + cur_constant_offset,
            );
            cur_header_off =
                unsafe { cur_header_off.offset(CFString::CONSTANT_HEADER_SIZE as isize) };
        }

        if bitmap_slice[Self::NESTED_FIELD_BITMAP_IDX] == 1 {
            self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] = 1;
            let object_ref = ObjectRef(cur_header_off);
            let cur_dynamic_off = object_ref.get_offset() - relative_offset;
            let cur_dynamic_ptr = unsafe { buf.offset(cur_dynamic_off as isize) };
            self.nested_field_ptr.inner_deserialize(
                cur_dynamic_ptr,
                object_ref.get_size(),
                object_ref.get_offset(),
            );
            cur_header_off =
                unsafe { cur_header_off.offset(NestedObject1::CONSTANT_HEADER_SIZE as isize) };
        }

        if bitmap_slice[Self::NESTED_LIST_BITMAP_IDX] == 1 {
            self.bitmap[Self::NESTED_LIST_BITMAP_IDX] = 1;
            let object_ref = ObjectRef(cur_header_off);
            let cur_dynamic_off = object_ref.get_offset() - relative_offset;
            let cur_dynamic_ptr = unsafe { buf.offset(cur_dynamic_off as isize) };
            self.nested_list_field_ptr.inner_deserialize(
                cur_dynamic_ptr,
                object_ref.get_size(),
                object_ref.get_offset(),
            );
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NestedObject1<'registered> {
    bitmap: Vec<u8>,
    field1_ptr: CFBytes<'registered>,
    field2_ptr: NestedObject2<'registered>,
}

impl<'registered> NestedObject1<'registered> {
    const BITMAP_SIZE: usize = 8; // align up to 1 word
    const FIELD1_BITMAP_IDX: usize = 0;
    const FIELD2_BITMAP_IDX: usize = 1;

    pub fn new() -> NestedObject1<'registered> {
        NestedObject1 {
            bitmap: vec![0u8; Self::BITMAP_SIZE],
            field1_ptr: CFBytes::default(),
            field2_ptr: NestedObject2::default(),
        }
    }

    pub fn has_field1(&self) -> bool {
        self.bitmap[Self::FIELD1_BITMAP_IDX] == 1
    }

    pub fn get_field1(&self) -> CFBytes<'registered> {
        self.field1_ptr
    }

    pub fn set_field1(&mut self, bytes: CFBytes<'registered>) {
        self.bitmap[Self::FIELD1_BITMAP_IDX] = 1;
        self.field1_ptr = bytes;
    }

    pub fn has_field2(&self) -> bool {
        self.bitmap[Self::FIELD2_BITMAP_IDX] == 1
    }

    pub fn get_field2(&self) -> &NestedObject2<'registered> {
        &self.field2_ptr
    }

    pub fn get_field2_mut(&mut self) -> &mut NestedObject2<'registered> {
        &mut self.field2_ptr
    }

    pub fn set_field2(&mut self, obj: NestedObject2<'registered>) {
        self.bitmap[Self::FIELD2_BITMAP_IDX] = 1;
        self.field2_ptr = obj;
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::FIELD1_BITMAP_IDX] as usize * (CFBytes::CONSTANT_HEADER_SIZE)
            + self.bitmap[Self::FIELD2_BITMAP_IDX] as usize * (NestedObject2::CONSTANT_HEADER_SIZE)
    }
}

impl<'registered> HeaderRepr<'registered> for NestedObject1<'registered> {
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::FIELD1_BITMAP_IDX] as usize * (self.field1_ptr.total_header_size())
            + self.bitmap[Self::FIELD2_BITMAP_IDX] as usize * (self.field2_ptr.total_header_size())
    }

    fn inner_serialize<'normal>(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();
        // 1: copy in bitmap to the head of the object
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            )
        }
        let mut cur_header_off = unsafe { header_ptr.offset(Self::BITMAP_SIZE as isize) };
        let cur_dynamic_off = unsafe { header_ptr.offset(self.dynamic_header_size() as isize) };
        let cur_dynamic_off_size = offset + self.dynamic_header_offset();

        if self.has_field1() {
            ret.append(
                &mut self
                    .field1_ptr
                    .inner_serialize(cur_header_off, copy_func, 0),
            );
            cur_header_off =
                unsafe { cur_header_off.offset(CFBytes::CONSTANT_HEADER_SIZE as isize) };
        }

        if self.has_field2() {
            let mut field2_object_ref = ObjectRef(cur_header_off as _);
            field2_object_ref.write_size(self.field2_ptr.dynamic_header_size());
            field2_object_ref.write_offset(cur_dynamic_off_size);
            ret.append(&mut self.field2_ptr.inner_serialize(
                cur_dynamic_off,
                copy_func,
                cur_dynamic_off_size,
            ));
        }

        ret
    }

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        assert!(size >= Self::BITMAP_SIZE);
        self.bitmap = vec![0u8; Self::BITMAP_SIZE];
        let bitmap_slice = unsafe { slice::from_raw_parts(buf, Self::BITMAP_SIZE) };

        let mut cur_header_off = unsafe { buf.offset(Self::BITMAP_SIZE as isize) };
        if bitmap_slice[Self::FIELD1_BITMAP_IDX] == 1 {
            self.bitmap[Self::FIELD1_BITMAP_IDX] = 1;
            self.field1_ptr.inner_deserialize(
                cur_header_off,
                CFBytes::CONSTANT_HEADER_SIZE,
                relative_offset + Self::BITMAP_SIZE,
            );
            cur_header_off =
                unsafe { cur_header_off.offset(CFBytes::CONSTANT_HEADER_SIZE as isize) };
        }

        if bitmap_slice[Self::FIELD2_BITMAP_IDX] == 1 {
            self.bitmap[Self::FIELD2_BITMAP_IDX] = 1;
            let field2_object_ref = ObjectRef(cur_header_off);
            let field2_dynamic_off = field2_object_ref.get_offset() - relative_offset;
            self.field2_ptr.inner_deserialize(
                unsafe { buf.offset(field2_dynamic_off as isize) },
                field2_object_ref.get_size(),
                field2_object_ref.get_offset(),
            );
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NestedObject2<'registered> {
    bitmap: Vec<u8>,
    field1_ptr: CFBytes<'registered>,
}

pub const NESTEDOBJECT2_HEADER_SIZE: usize = 16;

impl<'registered> NestedObject2<'registered> {
    const BITMAP_SIZE: usize = 8; // align up to 1 word
    const FIELD1_BITMAP_IDX: usize = 0;

    pub fn new() -> NestedObject2<'registered> {
        NestedObject2 {
            bitmap: vec![0u8; Self::BITMAP_SIZE],
            field1_ptr: CFBytes::default(),
        }
    }

    pub fn has_field1(&self) -> bool {
        self.bitmap[Self::FIELD1_BITMAP_IDX] == 1
    }

    pub fn get_field1(&self) -> CFBytes<'registered> {
        self.field1_ptr
    }

    pub fn set_field1(&mut self, bytes: CFBytes<'registered>) {
        self.bitmap[Self::FIELD1_BITMAP_IDX] = 1;
        self.field1_ptr = bytes
    }
}

impl<'registered> HeaderRepr<'registered> for NestedObject2<'registered> {
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::FIELD1_BITMAP_IDX] as usize * (self.field1_ptr.total_header_size())
    }

    fn inner_serialize<'normal>(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();
        // 1: copy in bitmap to the head of the object
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            )
        }
        let cur_header_off = unsafe { header_ptr.offset(Self::BITMAP_SIZE as isize) };

        // field 1 is a bytes offset
        if self.has_field1() {
            let mut field1_object_ref = ObjectRef(cur_header_off as _);
            field1_object_ref.write_size(self.field1_ptr.ptr.len());
            ret.push((
                CornPtr::Registered(self.field1_ptr.ptr),
                field1_object_ref.get_offset_ptr(),
            ));
        }

        ret
    }

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        assert!(size >= Self::BITMAP_SIZE);
        self.bitmap = vec![0u8; Self::BITMAP_SIZE];
        let bitmap_slice = unsafe { slice::from_raw_parts(buf, Self::BITMAP_SIZE) };

        let cur_header_off = unsafe { buf.offset(Self::BITMAP_SIZE as isize) };
        if bitmap_slice[Self::FIELD1_BITMAP_IDX] == 1 {
            self.bitmap[Self::FIELD1_BITMAP_IDX] = 1;
            self.field1_ptr.inner_deserialize(
                cur_header_off,
                CFBytes::CONSTANT_HEADER_SIZE,
                relative_offset + Self::BITMAP_SIZE,
            );
        }
    }
}
