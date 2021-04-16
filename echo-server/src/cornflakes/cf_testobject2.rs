use byteorder::{ByteOrder, LittleEndian};
use cornflakes_codegen::utils::fixed_hdr::HeaderRepr;
use cornflakes_codegen::utils::fixed_hdr::*;
use cornflakes_libos::CornPtr;
use libc;
use std::{iter::Iterator, slice};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestObject<'registered> {
    has_header_ptr: bool,
    offset: usize,
    header_ptr: &'registered [u8],
    bitmap: [u8; 8],
    int_field: i32,
    int_list_field_ptr: List<'registered, i32>,
    bytes_list_field_ptr: VariableList<'registered, CFBytes<'registered>>,
    string_field_ptr: CFString<'registered>,
    nested_field_ptr: NestedObject1<'registered>,
    nested_list_field_ptr: VariableList<'registered, NestedObject2<'registered>>,
}

impl<'registered> Default for TestObject<'registered> {
    fn default() -> Self {
        TestObject {
            has_header_ptr: false,
            offset: 0,
            header_ptr: &[],
            bitmap: [0u8; Self::BITMAP_SIZE],
            int_field: 0,
            int_list_field_ptr: List::default(),
            bytes_list_field_ptr: VariableList::default(),
            string_field_ptr: CFString::default(),
            nested_field_ptr: NestedObject1::new(),
            nested_list_field_ptr: VariableList::default(),
        }
    }
}

impl<'registered> TestObject<'registered> {
    const BITMAP_SIZE: usize = 8; // align up to 8

    const INT_FIELD_BITMAP_IDX: usize = 0;
    const INT_FIELD_HEADER_OFFSET: usize = Self::BITMAP_SIZE;
    const INT_FIELD_HEADER_SIZE: usize = 4;

    const INT_LIST_FIELD_BITMAP_IDX: usize = 1;
    const INT_LIST_FIELD_HEADER_OFFSET: usize = Self::BITMAP_SIZE + Self::INT_FIELD_HEADER_SIZE;

    const BYTES_LIST_FIELD_BITMAP_IDX: usize = 2;
    const BYTES_LIST_FIELD_HEADER_OFFSET: usize =
        Self::BITMAP_SIZE + Self::INT_FIELD_HEADER_SIZE + List::<i32>::CONSTANT_HEADER_SIZE;

    const STRING_FIELD_BITMAP_IDX: usize = 3;
    const STRING_FIELD_HEADER_OFFSET: usize = Self::BITMAP_SIZE
        + Self::INT_FIELD_HEADER_SIZE
        + List::<i32>::CONSTANT_HEADER_SIZE
        + VariableList::<CFBytes>::CONSTANT_HEADER_SIZE;

    const NESTED_FIELD_BITMAP_IDX: usize = 4;
    const NESTED_FIELD_HEADER_OFFSET: usize = Self::BITMAP_SIZE
        + Self::INT_FIELD_HEADER_SIZE
        + List::<i32>::CONSTANT_HEADER_SIZE
        + VariableList::<CFBytes>::CONSTANT_HEADER_SIZE
        + CFString::CONSTANT_HEADER_SIZE;

    const NESTED_LIST_BITMAP_IDX: usize = 5;
    const NESTED_LIST_FIELD_HEADER_OFFSET: usize = Self::BITMAP_SIZE
        + Self::INT_FIELD_HEADER_SIZE
        + List::<i32>::CONSTANT_HEADER_SIZE
        + VariableList::<CFBytes>::CONSTANT_HEADER_SIZE
        + CFString::CONSTANT_HEADER_SIZE
        + NestedObject1::CONSTANT_HEADER_SIZE;

    // Initialize with a slice to hold bytes
    pub fn new() -> TestObject<'registered> {
        TestObject {
            has_header_ptr: false,
            header_ptr: &[],
            offset: 0,
            bitmap: [0u8; Self::BITMAP_SIZE],
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
            || (self.has_header_ptr
                && self.header_ptr[self.offset + Self::INT_FIELD_BITMAP_IDX] == 1)
    }

    pub fn get_int_field(&self) -> i32 {
        if self.bitmap[Self::INT_FIELD_BITMAP_IDX] == 1 {
            self.int_field
        } else if self.has_header_ptr
            && self.header_ptr[self.offset + Self::INT_FIELD_BITMAP_IDX] == 1
        {
            LittleEndian::read_i32(
                &self.header_ptr[self.offset + Self::INT_FIELD_HEADER_OFFSET
                    ..(self.offset + Self::INT_FIELD_HEADER_OFFSET + 4)],
            )
        } else {
            self.int_field
        }
    }

    pub fn set_int_field(&mut self, val: i32) {
        self.int_field = val;
        self.bitmap[Self::INT_FIELD_BITMAP_IDX] = 1;
    }

    pub fn has_int_list_field(&self) -> bool {
        self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] == 1
            || (self.has_header_ptr
                && self.header_ptr[self.offset + Self::INT_LIST_FIELD_BITMAP_IDX] == 1)
    }

    pub fn get_int_list_field(&self) -> List<'registered, i32> {
        if self.has_header_ptr
            && self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] != 1
            && self.header_ptr[self.offset + Self::INT_LIST_FIELD_BITMAP_IDX] == 1
        {
            List::from_buffer(
                self.header_ptr,
                self.offset + Self::INT_LIST_FIELD_HEADER_OFFSET,
            )
        } else {
            self.int_list_field_ptr.clone()
        }
    }

    pub fn get_mut_int_list_field(&mut self) -> &mut List<'registered, i32> {
        if self.has_header_ptr
            && self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] != 1
            && self.header_ptr[self.offset + Self::INT_LIST_FIELD_BITMAP_IDX] == 1
        {
            // deserialize the list
            let list = List::from_buffer(
                self.header_ptr,
                self.offset + Self::INT_LIST_FIELD_HEADER_OFFSET,
            );
            self.int_list_field_ptr = list;
            self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] = 1;
        }
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
            || (self.has_header_ptr
                && self.header_ptr[self.offset + Self::BYTES_LIST_FIELD_BITMAP_IDX] == 1)
    }

    pub fn get_bytes_list_field(&self) -> VariableList<'registered, CFBytes<'registered>> {
        if self.has_header_ptr
            && self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] != 1
            && self.header_ptr[self.offset + Self::BYTES_LIST_FIELD_BITMAP_IDX] == 1
        {
            VariableList::<CFBytes>::from_buffer(
                self.header_ptr,
                self.offset + Self::BYTES_LIST_FIELD_HEADER_OFFSET,
            )
        } else {
            self.bytes_list_field_ptr.clone()
        }
    }

    pub fn get_mut_bytes_list_field(
        &mut self,
    ) -> &mut VariableList<'registered, CFBytes<'registered>> {
        if self.has_header_ptr
            && self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] != 1
            && self.header_ptr[self.offset + Self::BYTES_LIST_FIELD_BITMAP_IDX] == 1
        {
            self.bytes_list_field_ptr = VariableList::<CFBytes>::from_buffer(
                self.header_ptr,
                self.offset + Self::BYTES_LIST_FIELD_HEADER_OFFSET,
            );
            self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] = 1;
        }
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
            || (self.has_header_ptr
                && self.header_ptr[self.offset + Self::STRING_FIELD_BITMAP_IDX] == 1)
    }

    pub fn get_string_field(&self) -> CFString<'registered> {
        if self.bitmap[Self::STRING_FIELD_BITMAP_IDX] == 1 {
            self.string_field_ptr
        } else if self.has_header_ptr
            && self.header_ptr[self.offset + Self::STRING_FIELD_BITMAP_IDX] == 1
        {
            let mut cf_string = CFString::default();
            cf_string.inner_deserialize(
                self.header_ptr,
                self.offset + Self::STRING_FIELD_HEADER_OFFSET,
            );
            cf_string
        } else {
            self.string_field_ptr
        }
    }

    pub fn set_string_field(&mut self, field: CFString<'registered>) {
        self.bitmap[Self::STRING_FIELD_BITMAP_IDX] = 1;
        self.string_field_ptr = field;
    }

    pub fn has_nested_field(&self) -> bool {
        self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] == 1
            || (self.has_header_ptr
                && self.header_ptr[self.offset + Self::NESTED_FIELD_BITMAP_IDX] == 1)
    }

    pub fn get_nested_field(&self) -> NestedObject1<'registered> {
        if self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] == 1 {
            self.nested_field_ptr
        } else if self.has_header_ptr
            && self.header_ptr[self.offset + Self::NESTED_FIELD_BITMAP_IDX] == 1
        {
            let mut nested_object = NestedObject1::new();
            nested_object.inner_deserialize(
                self.header_ptr,
                self.offset + Self::NESTED_FIELD_HEADER_OFFSET,
            );
            nested_object
        } else {
            self.nested_field_ptr
        }
    }

    pub fn get_mut_nested_field(&mut self) -> &mut NestedObject1<'registered> {
        if self.has_header_ptr
            && self.header_ptr[self.offset + Self::NESTED_FIELD_BITMAP_IDX] == 1
            && self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] != 1
        {
            let mut nested_object = NestedObject1::new();
            nested_object.inner_deserialize(
                self.header_ptr,
                self.offset + Self::NESTED_FIELD_HEADER_OFFSET,
            );
            self.nested_field_ptr = nested_object;
        }
        &mut self.nested_field_ptr
    }

    pub fn set_nested_field(&mut self, obj: NestedObject1<'registered>) {
        self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] = 1;
        self.nested_field_ptr = obj;
    }

    pub fn has_nested_list_field(&self) -> bool {
        self.bitmap[Self::NESTED_LIST_BITMAP_IDX] == 1
            || (self.has_header_ptr
                && self.header_ptr[self.offset + Self::NESTED_LIST_BITMAP_IDX] == 1)
    }

    pub fn get_nested_list_field(&self) -> VariableList<'registered, NestedObject2<'registered>> {
        if self.bitmap[Self::NESTED_LIST_BITMAP_IDX] == 1 {
            self.nested_list_field_ptr.clone()
        } else if self.has_header_ptr
            && self.header_ptr[self.offset + Self::NESTED_LIST_BITMAP_IDX] == 1
        {
            VariableList::<NestedObject2>::from_buffer(
                self.header_ptr,
                self.offset + Self::NESTED_LIST_FIELD_HEADER_OFFSET,
            )
        } else {
            self.nested_list_field_ptr.clone()
        }
    }

    pub fn get_mut_nested_list_field(
        &mut self,
    ) -> &mut VariableList<'registered, NestedObject2<'registered>> {
        if self.has_header_ptr
            && self.header_ptr[self.offset + Self::NESTED_LIST_FIELD_HEADER_OFFSET] == 1
        {
            let list = VariableList::<NestedObject2>::from_buffer(
                self.header_ptr,
                self.offset + Self::NESTED_LIST_FIELD_HEADER_OFFSET,
            );
            self.nested_list_field_ptr = list;
            self.bitmap[Self::NESTED_LIST_BITMAP_IDX] = 1;
        }

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
}

impl<'registered> HeaderRepr<'registered> for TestObject<'registered> {
    const CONSTANT_HEADER_SIZE: usize = Self::BITMAP_SIZE
        + Self::INT_FIELD_HEADER_SIZE
        + List::<i32>::CONSTANT_HEADER_SIZE
        + VariableList::<CFBytes>::CONSTANT_HEADER_SIZE
        + CFString::CONSTANT_HEADER_SIZE
        + NestedObject1::CONSTANT_HEADER_SIZE
        + VariableList::<NestedObject2>::CONSTANT_HEADER_SIZE;

    fn dynamic_header_size(&self) -> usize {
        let int_list_field = self.get_int_list_field();
        let bytes_list_field = self.get_bytes_list_field();
        let nested_field = self.get_nested_field();
        let nested_list_field = self.get_nested_list_field();
        int_list_field.dynamic_header_size()
            + bytes_list_field.dynamic_header_size()
            + nested_field.dynamic_header_size()
            + nested_list_field.dynamic_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();

        // copy the bitmap
        let final_bitmap = match self.has_header_ptr {
            true => {
                let ref_header_bitmap_slice =
                    &self.header_ptr[self.offset..(self.offset + Self::BITMAP_SIZE)];
                let bitmap_slice = &self.bitmap;
                let final_bitmap: Vec<u8> = ref_header_bitmap_slice
                    .iter()
                    .zip(bitmap_slice.iter())
                    .map(|(a, b)| a | b)
                    .collect();
                final_bitmap
            }
            false => {
                let final_bitmap: Vec<u8> = self.bitmap.iter().map(|x| *x).collect();
                final_bitmap
            }
        };

        unsafe {
            copy_func(
                constant_header_ptr as _,
                final_bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            )
        };

        let mut cur_dynamic_ptr = dynamic_header_ptr;
        let mut cur_dynamic_offset = dynamic_header_offset;

        // copy the int field
        if self.has_int_field() {
            let cur_header_ptr =
                unsafe { constant_header_ptr.offset(Self::INT_FIELD_HEADER_OFFSET as isize) };
            if self.bitmap[Self::INT_FIELD_BITMAP_IDX] == 1 {
                LittleEndian::write_i32(
                    unsafe {
                        slice::from_raw_parts_mut(cur_header_ptr as _, Self::INT_FIELD_HEADER_SIZE)
                    },
                    self.int_field,
                );
            } else {
                unsafe {
                    copy_func(
                        cur_header_ptr as _,
                        self.header_ptr
                            .as_ptr()
                            .offset((self.offset + Self::INT_FIELD_HEADER_OFFSET) as isize)
                            as _,
                        Self::INT_FIELD_HEADER_SIZE,
                    );
                }
            }
        }

        if self.has_int_list_field() {
            let cur_header_ptr =
                unsafe { constant_header_ptr.offset(Self::INT_LIST_FIELD_HEADER_OFFSET as isize) };
            let cur_header_offset = constant_header_offset + Self::INT_LIST_FIELD_HEADER_OFFSET;

            if self.bitmap[Self::INT_LIST_FIELD_BITMAP_IDX] == 1 {
                ret.append(&mut self.int_list_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
                cur_dynamic_ptr = unsafe {
                    cur_dynamic_ptr.offset(self.int_list_field_ptr.dynamic_header_size() as isize)
                };
                cur_dynamic_offset += self.int_list_field_ptr.dynamic_header_size();
            } else {
                let int_list_field_ptr = List::<i32>::from_buffer(
                    self.header_ptr,
                    self.offset + Self::INT_LIST_FIELD_HEADER_OFFSET,
                );
                ret.append(&mut int_list_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
                cur_dynamic_ptr = unsafe {
                    cur_dynamic_ptr.offset(int_list_field_ptr.dynamic_header_size() as isize)
                };
                cur_dynamic_offset += int_list_field_ptr.dynamic_header_size();
            }
        }

        if self.has_bytes_list_field() {
            let cur_header_ptr = unsafe {
                constant_header_ptr.offset(Self::BYTES_LIST_FIELD_HEADER_OFFSET as isize)
            };
            let cur_header_offset = constant_header_offset + Self::BYTES_LIST_FIELD_HEADER_OFFSET;
            if self.bitmap[Self::BYTES_LIST_FIELD_BITMAP_IDX] == 1 {
                ret.append(&mut self.bytes_list_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
                cur_dynamic_ptr = unsafe {
                    cur_dynamic_ptr.offset(self.bytes_list_field_ptr.dynamic_header_size() as isize)
                };
                cur_dynamic_offset += self.bytes_list_field_ptr.dynamic_header_size();
            } else {
                let bytes_list_field_ptr = VariableList::<CFBytes>::from_buffer(
                    self.header_ptr,
                    self.offset + Self::BYTES_LIST_FIELD_HEADER_OFFSET,
                );
                ret.append(&mut bytes_list_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
                cur_dynamic_ptr = unsafe {
                    cur_dynamic_ptr.offset(bytes_list_field_ptr.dynamic_header_size() as isize)
                };
                cur_dynamic_offset += bytes_list_field_ptr.dynamic_header_size();
            }
        }

        if self.has_string_field() {
            let cur_header_ptr =
                unsafe { constant_header_ptr.offset(Self::STRING_FIELD_HEADER_OFFSET as isize) };
            let cur_header_offset = constant_header_offset + Self::STRING_FIELD_HEADER_OFFSET;
            if self.bitmap[Self::STRING_FIELD_BITMAP_IDX] == 1 {
                ret.append(&mut self.string_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
            } else {
                let mut string_field_ptr = CFString::default();
                string_field_ptr.inner_deserialize(self.header_ptr, cur_header_offset);
                ret.append(&mut string_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
            }
        }

        if self.has_nested_field() {
            let cur_header_ptr =
                unsafe { constant_header_ptr.offset(Self::NESTED_FIELD_HEADER_OFFSET as isize) };
            let cur_header_offset = constant_header_offset + Self::NESTED_FIELD_HEADER_OFFSET;
            if self.bitmap[Self::NESTED_FIELD_BITMAP_IDX] == 1 {
                ret.append(&mut self.nested_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
                cur_dynamic_ptr = unsafe {
                    cur_dynamic_ptr.offset(self.nested_field_ptr.dynamic_header_size() as isize)
                };
                cur_dynamic_offset += self.nested_field_ptr.dynamic_header_size();
            } else {
                let mut nested_field_ptr = NestedObject1::new();
                nested_field_ptr.inner_deserialize(
                    self.header_ptr,
                    self.offset + Self::NESTED_FIELD_HEADER_OFFSET,
                );
                ret.append(&mut nested_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
                cur_dynamic_ptr = unsafe {
                    cur_dynamic_ptr.offset(nested_field_ptr.dynamic_header_size() as isize)
                };
                cur_dynamic_offset += nested_field_ptr.dynamic_header_size();
            }
        }

        if self.has_nested_list_field() {
            let cur_header_ptr = unsafe {
                constant_header_ptr.offset(Self::NESTED_LIST_FIELD_HEADER_OFFSET as isize)
            };
            let cur_header_offset = constant_header_offset + Self::NESTED_LIST_FIELD_HEADER_OFFSET;
            if self.bitmap[Self::NESTED_LIST_BITMAP_IDX] == 1 {
                ret.append(&mut self.nested_list_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
            } else {
                let nested_list_field_ptr = VariableList::<NestedObject2>::from_buffer(
                    self.header_ptr,
                    self.offset + Self::NESTED_LIST_FIELD_HEADER_OFFSET,
                );
                ret.append(&mut nested_list_field_ptr.inner_serialize(
                    cur_header_ptr,
                    cur_dynamic_ptr,
                    cur_header_offset,
                    cur_dynamic_offset,
                    copy_func,
                ));
            }
        }

        ret
    }

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        self.header_ptr = ref_buf;
        self.has_header_ptr = true;
        self.offset = constant_header_offset;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct NestedObject1<'registered> {
    has_header_ptr: bool,
    header_ptr: &'registered [u8],
    offset: usize,
    bitmap: [u8; 8],
    field1_ptr: CFBytes<'registered>,
    field2_ptr: NestedObject2<'registered>,
}

impl<'registered> Default for NestedObject1<'registered> {
    fn default() -> Self {
        NestedObject1 {
            has_header_ptr: false,
            header_ptr: &[],
            offset: 0,
            bitmap: [0u8; 8],
            field1_ptr: CFBytes::default(),
            field2_ptr: NestedObject2::new(),
        }
    }
}

impl<'registered> NestedObject1<'registered> {
    const BITMAP_SIZE: usize = 8; // align up to 1 word
    const FIELD1_BITMAP_IDX: usize = 0;
    const FIELD2_BITMAP_IDX: usize = 1;
    const FIELD1_OFFSET: usize = Self::BITMAP_SIZE;
    const FIELD2_OFFSET: usize = Self::BITMAP_SIZE + CFBytes::CONSTANT_HEADER_SIZE;

    pub fn new() -> NestedObject1<'registered> {
        NestedObject1 {
            has_header_ptr: false,
            header_ptr: &[],
            offset: 0,
            bitmap: [0u8; 8],
            field1_ptr: CFBytes::default(),
            field2_ptr: NestedObject2::new(),
        }
    }

    pub fn has_field1(&self) -> bool {
        (self.has_header_ptr && self.header_ptr[self.offset + Self::FIELD1_BITMAP_IDX] == 1)
            || self.bitmap[Self::FIELD1_BITMAP_IDX] == 1
    }

    pub fn get_field1(&self) -> CFBytes<'registered> {
        if self.bitmap[Self::FIELD1_BITMAP_IDX] == 1 {
            self.field1_ptr
        } else if self.has_header_ptr && self.header_ptr[self.offset + Self::FIELD1_BITMAP_IDX] == 1
        {
            let mut cf_bytes = CFBytes::default();
            cf_bytes.inner_deserialize(self.header_ptr, self.offset + Self::FIELD1_OFFSET);
            cf_bytes
        } else {
            self.field1_ptr
        }
    }

    pub fn set_field1(&mut self, bytes: CFBytes<'registered>) {
        self.bitmap[Self::FIELD1_BITMAP_IDX] = 1;
        self.field1_ptr = bytes;
    }

    pub fn has_field2(&self) -> bool {
        (self.has_header_ptr && self.header_ptr[self.offset + Self::FIELD2_BITMAP_IDX] == 1)
            || self.bitmap[Self::FIELD2_BITMAP_IDX] == 1
    }

    pub fn get_field2(&self) -> NestedObject2<'registered> {
        if self.bitmap[Self::FIELD2_BITMAP_IDX] == 1 {
            self.field2_ptr
        } else if self.has_header_ptr && self.header_ptr[self.offset + Self::FIELD2_BITMAP_IDX] == 1
        {
            let mut nested_obj = NestedObject2::new();
            nested_obj.inner_deserialize(self.header_ptr, self.offset + Self::FIELD2_OFFSET);
            nested_obj
        } else {
            self.field2_ptr
        }
    }

    pub fn get_field2_mut(&mut self) -> &mut NestedObject2<'registered> {
        if self.bitmap[Self::FIELD2_BITMAP_IDX] != 1
            && self.has_header_ptr
            && self.header_ptr[self.offset + Self::FIELD2_BITMAP_IDX] == 1
        {
            self.set_field2(self.get_field2())
        }
        &mut self.field2_ptr
    }

    pub fn set_field2(&mut self, obj: NestedObject2<'registered>) {
        self.bitmap[Self::FIELD2_BITMAP_IDX] = 1;
        self.field2_ptr = obj;
    }
}

impl<'registered> HeaderRepr<'registered> for NestedObject1<'registered> {
    const CONSTANT_HEADER_SIZE: usize =
        Self::BITMAP_SIZE + CFBytes::CONSTANT_HEADER_SIZE + NestedObject2::CONSTANT_HEADER_SIZE;

    fn dynamic_header_size(&self) -> usize {
        let field1 = self.get_field1();
        let field2 = self.get_field2();
        field1.dynamic_header_size() + field2.dynamic_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();
        // copy the bitmap
        let final_bitmap = match self.has_header_ptr {
            true => {
                let ref_header_bitmap_slice =
                    &self.header_ptr[self.offset..(self.offset + Self::BITMAP_SIZE)];
                let bitmap_slice = &self.bitmap;
                let final_bitmap: Vec<u8> = ref_header_bitmap_slice
                    .iter()
                    .zip(bitmap_slice.iter())
                    .map(|(a, b)| a | b)
                    .collect();
                final_bitmap
            }
            false => {
                let final_bitmap: Vec<u8> = self.bitmap.iter().map(|x| *x).collect();
                final_bitmap
            }
        };

        unsafe {
            copy_func(
                constant_header_ptr as _,
                final_bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            )
        };

        let mut cur_dynamic_ptr =
            unsafe { dynamic_header_ptr.offset(self.dynamic_header_offset() as isize) };
        let mut cur_dynamic_offset = dynamic_header_offset + self.dynamic_header_offset();

        if self.has_field1() {
            let field1 = self.get_field1();
            ret.append(&mut field1.inner_serialize(
                unsafe { constant_header_ptr.offset(Self::FIELD1_OFFSET as isize) },
                cur_dynamic_ptr,
                constant_header_offset + Self::FIELD1_OFFSET,
                cur_dynamic_offset,
                copy_func,
            ));
            cur_dynamic_ptr =
                unsafe { cur_dynamic_ptr.offset(field1.dynamic_header_size() as isize) };
            cur_dynamic_offset += field1.dynamic_header_size();
        }

        if self.has_field2() {
            let field2 = self.get_field2();
            ret.append(&mut field2.inner_serialize(
                unsafe { constant_header_ptr.offset(Self::FIELD2_OFFSET as isize) },
                cur_dynamic_ptr,
                constant_header_offset + Self::FIELD2_OFFSET,
                cur_dynamic_offset,
                copy_func,
            ));
        }

        ret
    }

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        self.header_ptr = ref_buf;
        self.has_header_ptr = true;
        self.offset = constant_header_offset;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct NestedObject2<'registered> {
    has_header_ptr: bool,
    header_ptr: &'registered [u8],
    offset: usize,
    bitmap: [u8; 8],
    field1_ptr: CFBytes<'registered>,
}

impl<'registered> Default for NestedObject2<'registered> {
    fn default() -> Self {
        NestedObject2 {
            has_header_ptr: false,
            header_ptr: &[],
            offset: 0,
            bitmap: [0u8; 8],
            field1_ptr: CFBytes::default(),
        }
    }
}

impl<'registered> NestedObject2<'registered> {
    const BITMAP_SIZE: usize = 8; // align up to 1 word
    const FIELD1_BITMAP_IDX: usize = 0;
    const FIELD1_OFFSET: usize = Self::BITMAP_SIZE;

    pub fn new() -> NestedObject2<'registered> {
        NestedObject2 {
            has_header_ptr: false,
            header_ptr: &[],
            offset: 0,
            bitmap: [0u8; 8],
            field1_ptr: CFBytes::default(),
        }
    }

    pub fn has_field1(&self) -> bool {
        (self.bitmap[Self::FIELD1_BITMAP_IDX] == 1)
            || (self.has_header_ptr && self.header_ptr[self.offset + Self::FIELD1_BITMAP_IDX] == 1)
    }

    pub fn get_field1(&self) -> CFBytes<'registered> {
        if self.bitmap[Self::FIELD1_BITMAP_IDX] == 1 {
            self.field1_ptr
        } else if self.has_header_ptr && self.header_ptr[self.offset + Self::FIELD1_BITMAP_IDX] == 1
        {
            let mut cf_bytes = CFBytes::default();
            let constant_header_offset = self.offset + Self::FIELD1_OFFSET;
            cf_bytes.inner_deserialize(self.header_ptr, constant_header_offset);
            cf_bytes
        } else {
            self.field1_ptr
        }
    }

    pub fn set_field1(&mut self, bytes: CFBytes<'registered>) {
        self.bitmap[Self::FIELD1_BITMAP_IDX] = 1;
        self.field1_ptr = bytes
    }
}

impl<'registered> HeaderRepr<'registered> for NestedObject2<'registered> {
    const CONSTANT_HEADER_SIZE: usize = Self::BITMAP_SIZE + CFBytes::CONSTANT_HEADER_SIZE;

    fn dynamic_header_size(&self) -> usize {
        let field1 = self.get_field1();
        field1.dynamic_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();
        // copy the bitmap
        let final_bitmap = match self.has_header_ptr {
            true => {
                let ref_header_bitmap_slice =
                    &self.header_ptr[self.offset..(self.offset + Self::BITMAP_SIZE)];
                let bitmap_slice = &self.bitmap;
                let final_bitmap: Vec<u8> = ref_header_bitmap_slice
                    .iter()
                    .zip(bitmap_slice.iter())
                    .map(|(a, b)| a | b)
                    .collect();
                final_bitmap
            }
            false => {
                let final_bitmap: Vec<u8> = self.bitmap.iter().map(|x| *x).collect();
                final_bitmap
            }
        };

        unsafe {
            copy_func(
                constant_header_ptr as _,
                final_bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            )
        };

        let cur_dynamic_header_ptr = dynamic_header_ptr;
        let cur_dynamic_header_off = dynamic_header_offset;

        if self.has_field1() {
            if self.bitmap[Self::FIELD1_BITMAP_IDX] == 1 {
                ret.append(&mut self.field1_ptr.inner_serialize(
                    unsafe {
                        constant_header_ptr.offset((self.offset + Self::FIELD1_OFFSET) as isize)
                    },
                    cur_dynamic_header_ptr,
                    constant_header_offset + Self::FIELD1_OFFSET,
                    cur_dynamic_header_off,
                    copy_func,
                ));
            } else {
                let field1 = self.get_field1();
                ret.append(&mut field1.inner_serialize(
                    unsafe { constant_header_ptr.offset(Self::FIELD1_OFFSET as isize) },
                    cur_dynamic_header_ptr,
                    constant_header_offset + Self::FIELD1_OFFSET,
                    cur_dynamic_header_off,
                    copy_func,
                ));
            }
        }

        ret
    }

    // Somehow is constant time
    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        self.header_ptr = ref_buf;
        self.has_header_ptr = true;
        self.offset = constant_header_offset;
    }
}
pub unsafe fn copy_func(
    dst: *mut ::std::os::raw::c_void,
    src: *const ::std::os::raw::c_void,
    n: usize,
) {
    libc::memcpy(dst, src, n);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cornflakes_libos::{CornPtr, PtrAttributes, ScatterGather};
    use cornflakes_utils::test_init;
    use libc;
    use std::{io::Write, str};
    use tracing_error::ErrorLayer;
    use tracing_subscriber;
    use tracing_subscriber::{layer::SubscriberExt, prelude::*};

    unsafe fn copy_func(
        dst: *mut ::std::os::raw::c_void,
        src: *const ::std::os::raw::c_void,
        n: usize,
    ) {
        libc::memcpy(dst, src, n);
    }

    #[test]
    fn test_nestedobject2_serialize() {
        test_init!();
        let mut object = NestedObject2::new();
        assert!(!object.has_field1());
        // just the size of the bitmap
        assert!(NestedObject2::CONSTANT_HEADER_SIZE == 8 + 8);
        let mut empty_serialization_buffer = object.init_header_buffer();
        let copy_of_header = unsafe {
            slice::from_raw_parts(
                empty_serialization_buffer.as_ptr(),
                empty_serialization_buffer.len(),
            )
        };
        assert!(
            empty_serialization_buffer.capacity() == 16 && empty_serialization_buffer.len() == 16
        );
        let cf = { object.serialize(empty_serialization_buffer.as_mut_slice(), copy_func) };
        assert!(cf.num_segments() == 1);
        assert!(*cf.get(0).unwrap() == CornPtr::Normal(copy_of_header));

        for i in 0..16 {
            assert!(cf.get(0).unwrap().as_ref()[i] == 0);
        }

        // now modify the object and see what happens
        let string_payload = "HELLO333333222222".to_string();
        object.set_field1(CFBytes::new(string_payload.as_str().as_bytes()));
        assert!(object.has_field1());
        assert!(object.get_field1() == CFBytes::new(string_payload.as_str().as_bytes()));

        let mut empty_serialization_buffer2 = object.init_header_buffer();
        let copy_of_header2 = unsafe {
            slice::from_raw_parts(
                empty_serialization_buffer2.as_ptr(),
                empty_serialization_buffer2.len(),
            )
        };
        assert!(
            empty_serialization_buffer2.capacity() == 16 && empty_serialization_buffer2.len() == 16
        );

        let cf = { object.serialize(empty_serialization_buffer2.as_mut_slice(), copy_func) };
        assert!(cf.num_segments() == 2);
        assert!(*cf.get(0).unwrap() == CornPtr::Normal(copy_of_header2));
        assert!(*cf.get(1).unwrap() == CornPtr::Registered(string_payload.as_str().as_bytes()));

        // check the payload in the header buffer
        for i in 0..8 {
            if i == 0 {
                assert!(cf.get(0).unwrap().as_ref()[i] == 1);
            } else {
                assert!(cf.get(0).unwrap().as_ref()[i] == 0);
            }
        }
        let obj_ref = unsafe { ObjectRef(empty_serialization_buffer2.as_ptr().offset(8)) };
        assert!(obj_ref.get_size() == string_payload.len());
        assert!(obj_ref.get_offset() == 16);
    }

    #[test]
    fn test_nestedobject2_deserialize() {
        test_init!();
        let string_payload = "HELLO333333222222".to_string();
        // deserialize empty object
        let buffer1_ptr = unsafe { libc::malloc(16) };
        let header_buffer1 = unsafe { slice::from_raw_parts_mut(buffer1_ptr as *mut u8, 16) };
        for i in 0..16 {
            header_buffer1[i] = 0;
        }
        assert!(header_buffer1.len() == 16);
        let mut object1 = NestedObject2::new();
        object1.deserialize(header_buffer1);
        assert!(object1.has_field1() == false);
        unsafe {
            libc::free(buffer1_ptr);
        }

        let buffer2_ptr = unsafe { libc::malloc(8 + 8 + string_payload.len()) };
        let header_buffer2 = unsafe {
            slice::from_raw_parts_mut(buffer2_ptr as *mut u8, 8 + 8 + string_payload.len())
        };
        let mut bitmap_slice = &mut header_buffer2[0..8];
        bitmap_slice.write(vec![0u8; 8].as_slice()).unwrap();
        let bitmap_slice = &mut header_buffer2[0..8];
        bitmap_slice[0] = 1;

        let mut obj_ref = ObjectRef(unsafe { (buffer2_ptr as *const u8).offset(8) });
        obj_ref.write_offset(16);
        obj_ref.write_size(string_payload.len());

        let mut payload_slice = &mut header_buffer2[16..];
        payload_slice
            .write(string_payload.as_str().as_bytes())
            .unwrap();

        let mut object2 = NestedObject2::new();
        object2.deserialize(header_buffer2);
        assert!(object2.has_field1());
        assert!(object2.get_field1().len() == string_payload.len());
        assert!(object2.get_field1().ptr == string_payload.as_str().as_bytes());
        assert!(str::from_utf8(object2.get_field1().ptr).unwrap() == string_payload.as_str());
        unsafe { libc::free(buffer2_ptr) };
    }

    fn get_nestedobject2<'registered>(payload: CFBytes<'registered>) -> NestedObject2<'registered> {
        let mut obj = NestedObject2::new();
        obj.set_field1(payload);
        obj
    }

    #[test]
    fn test_nestedobject1_serialize() {
        test_init!();
        // field 1 is CFBytes
        let payload1 = "hello11111".to_string();
        let cfbytes1 = CFBytes::new(payload1.as_str().as_bytes());
        // field 2 is nestedobject2
        let payload2 = "hello222".to_string();
        let cfbytes2 = CFBytes::new(payload2.as_str().as_bytes());
        assert!(cfbytes2.len() == payload2.len());

        let mut obj = NestedObject1::new();
        let header_buffer_empty = obj.init_header_buffer();
        assert!(header_buffer_empty.len() == 8 + 8 + 8 + 8);
        for i in 0..8 {
            assert!(header_buffer_empty[i] == 0);
        }

        assert!(!obj.has_field1());
        assert!(!obj.has_field2());

        obj.set_field2(get_nestedobject2(cfbytes2));
        assert!(!obj.has_field1());
        assert!(obj.has_field2());
        assert!(obj.get_field2().has_field1());
        assert!(obj.get_field2().get_field1() == cfbytes2);

        // serialize with only field 2 set
        let mut header_buffer_field2 = obj.init_header_buffer();
        assert!(header_buffer_field2.len() == 8 + 8 + 8 + 8);
        let header_buffer_field2_copy =
            unsafe { slice::from_raw_parts(header_buffer_field2.as_ptr(), 32) };
        let cf = obj.serialize(&mut header_buffer_field2, copy_func);
        assert!(cf.num_segments() == 2);
        // test header
        assert!(*cf.get(0).unwrap() == CornPtr::Normal(header_buffer_field2_copy));
        assert!(cf.get(0).unwrap().as_ref()[1] == 1);
        assert!(cf.get(0).unwrap().as_ref()[0] == 0);
        assert!(*cf.get(1).unwrap() == CornPtr::Registered(payload2.as_str().as_bytes()));
        assert!(cf.get(1).unwrap().buf_size() == payload2.len());
        assert!(cf.get(1).unwrap().as_ref() == payload2.as_str().as_bytes());

        // serialize with both field 1 and field 2 set
        obj.set_field1(cfbytes1);
        assert!(obj.has_field1());
        assert!(obj.has_field2());

        // serialize with both fields set
        let mut header_buffer_field3 = obj.init_header_buffer();
        assert!(header_buffer_field3.len() == 8 + 8 + 8 + 8);
        let header_buffer_field3_copy =
            unsafe { slice::from_raw_parts(header_buffer_field3.as_ptr(), 32) };
        let cf2 = obj.serialize(&mut header_buffer_field3, copy_func);
        assert!(cf2.num_segments() == 3);
        // test header
        assert!(*cf2.get(0).unwrap() == CornPtr::Normal(header_buffer_field3_copy));
        assert!(cf2.get(0).unwrap().as_ref()[0] == 1);
        assert!(cf2.get(0).unwrap().as_ref()[1] == 1);
        assert!(*cf2.get(1).unwrap() == CornPtr::Registered(payload2.as_str().as_bytes()));
        assert!(cf2.get(1).unwrap().buf_size() == payload2.len());
        assert!(cf2.get(1).unwrap().as_ref() == payload2.as_str().as_bytes());
        assert!(*cf2.get(2).unwrap() == CornPtr::Registered(payload1.as_str().as_bytes()));
        assert!(cf2.get(2).unwrap().buf_size() == payload1.len());
        assert!(cf2.get(2).unwrap().as_ref() == payload1.as_str().as_bytes());
    }

    #[test]
    fn test_nestedobject1_deserialize() {
        test_init!();
        let payload1 = "hello11111".to_string();
        let payload2 = "hello222".to_string();

        // just object 1
        let ptr1 = unsafe { libc::malloc(8 + 8 + 8 + 8 + payload1.len()) };
        let bitmap = vec![1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut bitmap_slice = unsafe { slice::from_raw_parts_mut(ptr1 as *mut u8, 8) };
        bitmap_slice.write(&bitmap).unwrap();
        let payload_ref_ptr = unsafe { (ptr1 as *const u8).offset(8) };
        let mut payload_ref = ObjectRef(payload_ref_ptr);
        payload_ref.write_size(payload1.len());
        payload_ref.write_offset(32);
        let payload_ptr = unsafe { (ptr1 as *mut u8).offset(32) };
        let mut payload_slice = unsafe { slice::from_raw_parts_mut(payload_ptr, payload1.len()) };
        payload_slice.write(payload1.as_str().as_bytes()).unwrap();

        let mut obj1_payload1 = NestedObject1::new();
        obj1_payload1
            .deserialize(unsafe { slice::from_raw_parts(ptr1 as *const u8, 32 + payload1.len()) });
        assert!(obj1_payload1.has_field1());
        assert!(!obj1_payload1.has_field2());
        assert!(obj1_payload1.get_field1().ptr == payload1.as_str().as_bytes());
        unsafe {
            libc::free(ptr1);
        }

        // just object 2
        let ptr2 = unsafe { libc::malloc(8 + 8 + 8 + 8 + payload2.len()) };
        let bitmap2 = vec![0u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut bitmap_slice2 = unsafe { slice::from_raw_parts_mut(ptr2 as *mut u8, 8) };
        bitmap_slice2.write(&bitmap2).unwrap();
        let nested_bitmap2 = vec![1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut nested_bitmap_slice2 =
            unsafe { slice::from_raw_parts_mut((ptr2 as *mut u8).offset(16), 8) };
        nested_bitmap_slice2.write(&nested_bitmap2).unwrap();
        let payload_ref_ptr2 = unsafe { (ptr2 as *const u8).offset(24) };
        let mut payload_ref2 = ObjectRef(payload_ref_ptr2);
        payload_ref2.write_size(payload2.len());
        payload_ref2.write_offset(32);
        let mut payload_slice2 =
            unsafe { slice::from_raw_parts_mut((ptr2 as *mut u8).offset(32), payload2.len()) };
        payload_slice2.write(payload2.as_str().as_bytes()).unwrap();

        let mut obj1_payload2 = NestedObject1::new();
        obj1_payload2
            .deserialize(unsafe { slice::from_raw_parts(ptr2 as *const u8, 32 + payload2.len()) });
        assert!(!obj1_payload2.has_field1());
        assert!(obj1_payload2.has_field2());
        assert!(obj1_payload2.get_field2().has_field1());
        assert!(obj1_payload2.get_field2().get_field1().ptr == payload2.as_str().as_bytes());

        unsafe {
            libc::free(ptr2);
        }

        // both object 2 and object 1
        let ptr3 = unsafe { libc::malloc(8 + 8 + 8 + 8 + payload1.len() + payload2.len()) };
        let bitmap3 = vec![1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let mut bitmap_slice3 = unsafe { slice::from_raw_parts_mut(ptr3 as *mut u8, 8) };
        bitmap_slice3.write(&bitmap3).unwrap();
        let field1_ref_ptr = unsafe { (ptr3 as *const u8).offset(8) };
        let mut field1_ref = ObjectRef(field1_ref_ptr);
        field1_ref.write_size(payload1.len());
        field1_ref.write_offset(32 + payload2.len());
        let mut payload1_slice = unsafe {
            slice::from_raw_parts_mut(
                (ptr3 as *mut u8).offset(32 + payload2.len() as isize),
                payload1.len(),
            )
        };
        payload1_slice.write(payload1.as_str().as_ref()).unwrap();
        let mut field2_bitmap_slice =
            unsafe { slice::from_raw_parts_mut((ptr3 as *mut u8).offset(16), 8) };
        let field2_bitmap = vec![1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        field2_bitmap_slice.write(&field2_bitmap).unwrap();
        let field2_field1_ptr = unsafe { (ptr3 as *const u8).offset(24) };
        let mut field2_field1_ref = ObjectRef(field2_field1_ptr);
        field2_field1_ref.write_size(payload2.len());
        field2_field1_ref.write_offset(32);
        let mut payload2_slice =
            unsafe { slice::from_raw_parts_mut((ptr3 as *mut u8).offset(32), payload2.len()) };
        payload2_slice.write(payload2.as_str().as_ref()).unwrap();
        let mut obj1_full = NestedObject1::new();
        obj1_full.deserialize(unsafe {
            slice::from_raw_parts(ptr3 as *const u8, 32 + payload1.len() + payload2.len())
        });
        assert!(obj1_full.has_field1());
        assert!(obj1_full.get_field1().ptr == payload1.as_str().as_bytes());
        assert!(obj1_full.has_field2());
        assert!(obj1_full.get_field2().has_field1());
        assert!(obj1_full.get_field2().get_field1().ptr == payload2.as_str().as_bytes());
        unsafe {
            libc::free(ptr3);
        }
    }

    fn get_nestedobject1<'a>(payload1: CFBytes<'a>, payload2: CFBytes<'a>) -> NestedObject1<'a> {
        let mut obj = NestedObject1::default();
        obj.set_field1(payload1);
        obj.set_field2(get_nestedobject2(payload2));
        obj
    }

    #[test]
    fn test_testobject_serialize() {
        test_init!();
        let int_field = 5;
        let int_list_field = vec![1i32, 2i32, 3i32, 4i32];
        let bytes_list_field = vec!["hello11111", "hello2222", "hello333", "hello44", "hello5"];
        let string_field = "hello";
        let nested_field1_payload = "nested_payload1_hello111111111111"; // size = 33
        let nested_field1_nested_payload = "nested_payload1_hello1111111"; // size = 28
        let nested_list_field = vec!["nested_payload2_hello11111", "nested_payload2_hello2222"]; // sizes 26, 25

        let mut sorted_payloads = vec![string_field.clone()];
        sorted_payloads.append(&mut bytes_list_field.clone());
        sorted_payloads.push(nested_field1_payload.clone());
        sorted_payloads.push(nested_field1_nested_payload.clone());
        sorted_payloads.append(&mut nested_list_field.clone());
        sorted_payloads.sort_by(|a, b| a.len().partial_cmp(&b.len()).unwrap());

        // serialize everything
        let mut test_object = TestObject::default();
        assert!(!test_object.has_int_field());
        assert!(!test_object.has_int_list_field());
        assert!(!test_object.has_bytes_list_field());
        assert!(!test_object.has_string_field());
        assert!(!test_object.has_nested_field());
        assert!(!test_object.has_nested_list_field());
        let dynamic_offset_so_far = TestObject::CONSTANT_HEADER_SIZE;
        let mut header_size_so_far = TestObject::CONSTANT_HEADER_SIZE;
        assert!(TestObject::CONSTANT_HEADER_SIZE == 76);

        // set the int field
        test_object.set_int_field(int_field);
        assert!(test_object.has_int_field());
        assert!(test_object.get_int_field() == int_field);
        assert!(test_object.init_header_buffer().len() == header_size_so_far);
        assert!(test_object.dynamic_header_offset() == dynamic_offset_so_far);
        let mut header_buffer_int_field = test_object.init_header_buffer();
        let cf = test_object.serialize(header_buffer_int_field.as_mut_slice(), copy_func);
        assert!(cf.num_segments() == 1);
        assert!(
            *cf.get(0).unwrap().as_ref()
                == [
                    1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                ]
        );

        // set the int list field
        let mut int_list_field_ptr: List<i32> = List::init(int_list_field.len());
        for int in int_list_field.iter() {
            int_list_field_ptr.append(*int);
        }
        test_object.set_int_list_field(int_list_field_ptr);
        assert!(test_object.has_int_list_field());
        let object_int_list_field_ptr = test_object.get_int_list_field();
        for (i, int) in int_list_field.iter().enumerate() {
            assert!(*int == object_int_list_field_ptr[i]);
        }
        header_size_so_far += int_list_field.len() * 4;
        assert!(test_object.init_header_buffer().len() == header_size_so_far);
        let mut header_buffer_int_list_field = test_object.init_header_buffer();
        let cf = test_object.serialize(header_buffer_int_list_field.as_mut_slice(), copy_func);
        assert!(cf.num_segments() == 1);
        assert!(
            *cf.get(0).unwrap().as_ref()
                == [
                    1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    76u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8,
                    0u8, 2u8, 0u8, 0u8, 0u8, 3u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8
                ]
        );

        // set the bytes list field
        let bytes_list_field_ptr: VariableList<CFBytes> =
            VariableList::init(bytes_list_field.len());
        test_object.set_bytes_list_field(bytes_list_field_ptr);
        assert!(test_object.has_bytes_list_field());
        for (i, bytes) in bytes_list_field.iter().enumerate() {
            let cfbytes = CFBytes::new(bytes.as_ref());
            test_object.get_mut_bytes_list_field().append(cfbytes);
            assert!(test_object.get_bytes_list_field().len() == i + 1);
        }
        for (i, bytes) in bytes_list_field.iter().enumerate() {
            assert!(CFBytes::new(bytes.as_ref()) == *test_object.get_bytes_list_field().get(i));
        }
        header_size_so_far += 8 * bytes_list_field.len();
        assert!(test_object.init_header_buffer().len() == header_size_so_far);
        let mut header_buffer_bytes_list_field = test_object.init_header_buffer();
        let cf = test_object.serialize(header_buffer_bytes_list_field.as_mut_slice(), copy_func);
        assert!(cf.num_segments() == 1 + bytes_list_field.len());
        assert!(
            *cf.get(0).unwrap().as_ref()
                == [
                    1u8, 1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    76u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 92u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8,
                    0u8, 2u8, 0u8, 0u8, 0u8, 3u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8, 10u8, 0u8,
                    0u8, 0u8, 162u8, 0u8, 0u8, 0u8, 9u8, 0u8, 0u8, 0u8, 153u8, 0u8, 0u8, 0u8, 8u8,
                    0u8, 0u8, 0u8, 145u8, 0u8, 0u8, 0u8, 7u8, 0u8, 0u8, 0u8, 138u8, 0u8, 0u8, 0u8,
                    6u8, 0u8, 0u8, 0u8, 132u8, 0u8, 0u8, 0u8
                ]
        );

        // set the string field
        test_object.set_string_field(CFString::new(string_field));
        assert!(test_object.has_string_field());
        assert!(test_object.get_string_field() == CFString::new(string_field));
        assert!(test_object.init_header_buffer().len() == header_size_so_far);

        let mut header_buffer_string_field = test_object.init_header_buffer();
        let cf = test_object.serialize(header_buffer_string_field.as_mut_slice(), copy_func);
        assert!(cf.num_segments() == 1 + bytes_list_field.len() + 1);
        assert!(
            *cf.get(0).unwrap().as_ref()
                == [
                    1u8, 1u8, 1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    76u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 92u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8,
                    0u8, 132u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 0u8,
                    0u8, 0u8, 2u8, 0u8, 0u8, 0u8, 3u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8, 10u8,
                    0u8, 0u8, 0u8, 167u8, 0u8, 0u8, 0u8, 9u8, 0u8, 0u8, 0u8, 158u8, 0u8, 0u8, 0u8,
                    8u8, 0u8, 0u8, 0u8, 150u8, 0u8, 0u8, 0u8, 7u8, 0u8, 0u8, 0u8, 143u8, 0u8, 0u8,
                    0u8, 6u8, 0u8, 0u8, 0u8, 137u8, 0u8, 0u8, 0u8
                ]
        );

        // set the nested object field
        test_object.set_nested_field(get_nestedobject1(
            CFBytes::new(nested_field1_payload.as_ref()),
            CFBytes::new(nested_field1_nested_payload.as_ref()),
        ));
        assert!(test_object.has_nested_field());
        assert!(
            test_object.get_nested_field()
                == get_nestedobject1(
                    CFBytes::new(nested_field1_payload.as_ref()),
                    CFBytes::new(nested_field1_nested_payload.as_ref())
                )
        );
        assert!(
            test_object.get_nested_field().has_field1()
                && test_object.get_nested_field().has_field2()
                && test_object.get_nested_field().get_field2().has_field1()
        );
        assert!(test_object.init_header_buffer().len() == header_size_so_far);
        let mut header_buffer_nested_field = test_object.init_header_buffer();
        let cf = test_object.serialize(header_buffer_nested_field.as_mut_slice(), copy_func);
        assert!(cf.num_segments() == 1 + bytes_list_field.len() + 1 + 2);
        assert!(
            *cf.get(0).unwrap().as_ref()
                == [
                    1u8, 1u8, 1u8, 1u8, 1u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    76u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 92u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8,
                    0u8, 132u8, 0u8, 0u8, 0u8, 1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 33u8, 0u8,
                    0u8, 0u8, 205u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 28u8,
                    0u8, 0u8, 0u8, 177u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
                    1u8, 0u8, 0u8, 0u8, 2u8, 0u8, 0u8, 0u8, 3u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    10u8, 0u8, 0u8, 0u8, 167u8, 0u8, 0u8, 0u8, 9u8, 0u8, 0u8, 0u8, 158u8, 0u8, 0u8,
                    0u8, 8u8, 0u8, 0u8, 0u8, 150u8, 0u8, 0u8, 0u8, 7u8, 0u8, 0u8, 0u8, 143u8, 0u8,
                    0u8, 0u8, 6u8, 0u8, 0u8, 0u8, 137u8, 0u8, 0u8, 0u8
                ]
        );

        // set the nested list field
        let nested_list_field_ptr = VariableList::<NestedObject2>::init(nested_list_field.len());
        test_object.set_nested_list_field(nested_list_field_ptr);
        assert!(test_object.has_nested_list_field());
        for (i, bytes) in nested_list_field.iter().enumerate() {
            let nested_object = get_nestedobject2(CFBytes::new(bytes.as_ref()));
            test_object
                .get_mut_nested_list_field()
                .append(nested_object);
            assert!(test_object.get_nested_list_field().len() == i + 1);
            assert!(
                *test_object.get_nested_list_field().get(i)
                    == get_nestedobject2(CFBytes::new(bytes.as_ref()))
            );
        }
        header_size_so_far += nested_list_field.len() * (8 + 8);
        assert!(test_object.init_header_buffer().len() == header_size_so_far);

        // serialize the cornflake and check all of the entries are correct
        let mut header_buffer = test_object.init_header_buffer();
        let cf = test_object.serialize(header_buffer.as_mut_slice(), copy_func);
        assert!(cf.num_segments() == 1 + bytes_list_field.len() + 1 + 2 + nested_list_field.len());
        assert!(
            *cf.get(0).unwrap().as_ref()
                == [
                    1u8, 1u8, 1u8, 1u8, 1u8, 1u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    76u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 92u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8,
                    0u8, 164u8, 0u8, 0u8, 0u8, 1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 33u8, 0u8,
                    0u8, 0u8, 32u8, 1u8, 0u8, 0u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 28u8,
                    0u8, 0u8, 0u8, 4u8, 1u8, 0u8, 0u8, 2u8, 0u8, 0u8, 0u8, 132u8, 0u8, 0u8, 0u8,
                    1u8, 0u8, 0u8, 0u8, 2u8, 0u8, 0u8, 0u8, 3u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8,
                    10u8, 0u8, 0u8, 0u8, 199u8, 0u8, 0u8, 0u8, 9u8, 0u8, 0u8, 0u8, 190u8, 0u8, 0u8,
                    0u8, 8u8, 0u8, 0u8, 0u8, 182u8, 0u8, 0u8, 0u8, 7u8, 0u8, 0u8, 0u8, 175u8, 0u8,
                    0u8, 0u8, 6u8, 0u8, 0u8, 0u8, 169u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 26u8, 0u8, 0u8, 0u8, 234u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8, 0u8,
                    0u8, 0u8, 0u8, 0u8, 25u8, 0u8, 0u8, 0u8, 209u8, 0u8, 0u8, 0u8
                ]
        );
        // check all of the pointers in further entries
        for i in 1..cf.num_segments() {
            let bytes = sorted_payloads[i - 1];
            let cf_str = str::from_utf8(cf.get(i).unwrap().as_ref()).unwrap();
            assert!(bytes == cf_str);
        }
    }

    #[test]
    fn test_testobject_deserialize() {
        test_init!();
        let int_field = 5;
        let int_list_field = vec![1i32, 2i32, 3i32, 4i32];
        let bytes_list_field = vec!["hello11111", "hello2222", "hello333", "hello44", "hello5"];
        let string_field = "hello";
        let nested_field1_payload = "nested_payload1_hello111111111111"; // size = 33
        let nested_field1_nested_payload = "nested_payload1_hello1111111"; // size = 28
        let nested_list_field = vec!["nested_payload2_hello11111", "nested_payload2_hello2222"]; // sizes 26, 25
        let mut sorted_payloads = vec![string_field.clone()];
        sorted_payloads.append(&mut bytes_list_field.clone());
        sorted_payloads.push(nested_field1_payload.clone());
        sorted_payloads.push(nested_field1_nested_payload.clone());
        sorted_payloads.append(&mut nested_list_field.clone());
        sorted_payloads.sort_by(|a, b| a.len().partial_cmp(&b.len()).unwrap());
        let total_size: usize = sorted_payloads.iter().map(|x| x.len()).sum();

        // header
        let mut buffer = vec![0u8; 164 + total_size];
        let header_buffer = vec![
            1u8, 1u8, 1u8, 1u8, 1u8, 1u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8, 76u8,
            0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 92u8, 0u8, 0u8, 0u8, 5u8, 0u8, 0u8, 0u8, 164u8, 0u8,
            0u8, 0u8, 1u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 33u8, 0u8, 0u8, 0u8, 32u8, 1u8, 0u8,
            0u8, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 28u8, 0u8, 0u8, 0u8, 4u8, 1u8, 0u8, 0u8,
            2u8, 0u8, 0u8, 0u8, 132u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8, 0u8, 2u8, 0u8, 0u8, 0u8, 3u8,
            0u8, 0u8, 0u8, 4u8, 0u8, 0u8, 0u8, 10u8, 0u8, 0u8, 0u8, 199u8, 0u8, 0u8, 0u8, 9u8, 0u8,
            0u8, 0u8, 190u8, 0u8, 0u8, 0u8, 8u8, 0u8, 0u8, 0u8, 182u8, 0u8, 0u8, 0u8, 7u8, 0u8,
            0u8, 0u8, 175u8, 0u8, 0u8, 0u8, 6u8, 0u8, 0u8, 0u8, 169u8, 0u8, 0u8, 0u8, 1u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 26u8, 0u8, 0u8, 0u8, 234u8, 0u8, 0u8, 0u8, 1u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8, 25u8, 0u8, 0u8, 0u8, 209u8, 0u8, 0u8, 0u8,
        ];
        let mut buffer_slice = &mut buffer.as_mut_slice()[0..header_buffer.len()];
        buffer_slice.write(&header_buffer).unwrap();
        let mut cur_offset = 164;
        for payload in sorted_payloads.iter() {
            let mut slice = &mut buffer.as_mut_slice()[cur_offset..(cur_offset + payload.len())];
            slice.write(&payload.as_ref()).unwrap();
            cur_offset += payload.len();
        }

        let mut test_object = TestObject::new();
        test_object.deserialize(buffer.as_slice());
        assert!(test_object.has_int_field() && test_object.get_int_field() == int_field);
        assert!(
            test_object.has_int_list_field()
                && test_object.get_int_list_field().len() == int_list_field.len()
        );
        for (i, int) in int_list_field.iter().enumerate() {
            assert!(test_object.get_int_list_field()[i] == *int);
        }

        assert!(
            test_object.has_bytes_list_field()
                && test_object.get_bytes_list_field().len() == bytes_list_field.len()
        );
        for (i, bytes) in bytes_list_field.iter().enumerate() {
            assert!(
                str::from_utf8(test_object.get_bytes_list_field().get(i).ptr).unwrap() == *bytes
            );
        }

        assert!(test_object.has_string_field());
        assert!(test_object.get_string_field().to_string() == string_field.to_string());

        assert!(test_object.has_nested_field());
        assert!(
            test_object.get_nested_field().has_field1()
                && str::from_utf8(test_object.get_nested_field().get_field1().ptr).unwrap()
                    == nested_field1_payload
        );

        assert!(
            test_object.get_nested_field().has_field2()
                && test_object.get_nested_field().get_field2().has_field1()
                && str::from_utf8(test_object.get_nested_field().get_field2().get_field1().ptr)
                    .unwrap()
                    == nested_field1_nested_payload
        );

        assert!(
            test_object.has_nested_list_field()
                && test_object.get_nested_list_field().len() == nested_list_field.len()
        );
        for (i, nested_field) in nested_list_field.iter().enumerate() {
            let mut nested_list_obj = test_object.get_nested_list_field();
            assert!(nested_list_obj.get(i).has_field1());
            assert!(
                str::from_utf8(nested_list_obj.get(i).get_field1().ptr).unwrap() == *nested_field
            );
        }
    }
}
