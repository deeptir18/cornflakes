use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use cornflakes_libos::{CornPtr, Cornflake, PtrAttributes};
use std::{
    default::Default, fmt::Debug, marker::PhantomData, mem::size_of, ops::Index, ptr, slice, str,
};

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

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct CFString<'a> {
    pub ptr: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct CFBytes<'a> {
    pub ptr: &'a [u8],
}

/// Defines the amount of space pointing to this object would take in the header.
pub trait HeaderRepr<'registered> {
    const CONSTANT_HEADER_SIZE: usize;

    fn dynamic_header_size(&self) -> usize;

    fn total_header_size(&self) -> usize {
        self.dynamic_header_size() + <Self as HeaderRepr<'registered>>::CONSTANT_HEADER_SIZE
    }

    fn dynamic_header_offset(&self) -> usize;

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
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)>;

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize);

    /// Initializes a header _buffer large enough to store the entire serialization header.
    fn init_header_buffer(&self) -> Vec<u8> {
        vec![0u8; self.total_header_size()]
    }

    fn init_header_buffer_with_padding(&self) -> Vec<u8> {
        vec![0u8; align_up(self.total_header_size(), 64)]
    }

    fn serialize<'normal>(
        &self,
        header_buffer: &'normal mut [u8],
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Cornflake<'registered, 'normal> {
        let mut cf = Cornflake::default();
        let header_ptr = header_buffer.as_mut_ptr();

        let dynamic_header_ptr = unsafe {
            header_buffer
                .as_ptr()
                .offset(self.dynamic_header_offset() as isize)
        };
        // sort all the references by size, but keep offsets the same
        let mut cornptrs = self.inner_serialize(
            header_ptr,
            dynamic_header_ptr as *mut u8,
            0,
            self.dynamic_header_offset(),
            copy_func,
        );
        cf.add_entry(CornPtr::Normal(header_buffer));
        cornptrs.sort_by(|a, b| a.0.buf_size().partial_cmp(&b.0.buf_size()).unwrap());
        let mut cur_offset = self.total_header_size();
        for (cornptr, offset) in cornptrs.into_iter() {
            let mut obj_ref = ObjectRef(offset as *const u8);
            obj_ref.write_offset(cur_offset);
            cf.add_entry(cornptr);
            cur_offset += obj_ref.get_size() as usize;
        }
        cf
    }

    fn deserialize(&mut self, buf: &'registered [u8]) {
        self.inner_deserialize(buf, 0);
    }
}

impl<'registered> CFString<'registered> {
    pub fn new(ptr: &'registered str) -> Self {
        CFString {
            ptr: ptr.as_bytes(),
        }
    }

    pub fn new_from_bytes(ptr: &'registered [u8]) -> Self {
        CFString { ptr: ptr }
    }

    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    /// Assumes that the string is utf8-encoded.
    pub fn to_string(&self) -> String {
        str::from_utf8(self.ptr).unwrap().to_string()
    }
}

impl<'registered> Default for CFString<'registered> {
    fn default() -> Self {
        CFString {
            ptr: Default::default(),
        }
    }
}

impl<'registered> HeaderRepr<'registered> for CFString<'registered> {
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        _dynamic_header_ptr: *mut u8,
        _constant_header_offset: usize,
        _dynamic_header_offset: usize,
        _copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut obj_ref = ObjectRef(constant_header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        vec![(CornPtr::Registered(self.ptr), constant_header_ptr)]
    }

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        let constant_header_ptr =
            unsafe { ref_buf.as_ptr().offset(constant_header_offset as isize) };
        let object_ref = ObjectRef(constant_header_ptr);
        let payload_buf = unsafe {
            constant_header_ptr.offset((object_ref.get_offset() - constant_header_offset) as isize)
        };
        self.ptr = unsafe { slice::from_raw_parts(payload_buf, object_ref.get_size()) };
    }
}

impl<'registered> CFBytes<'registered> {
    pub fn new(ptr: &'registered [u8]) -> Self {
        CFBytes { ptr: ptr }
    }

    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    // Note: this function copies the inner pointer and should only used to check payloads for
    // correctness.
    pub fn to_bytes_vec(&self) -> Vec<u8> {
        let mut vec: Vec<u8> = Vec::with_capacity(self.ptr.len());
        vec.extend_from_slice(self.ptr);
        vec
    }
}

impl<'registered> Default for CFBytes<'registered> {
    fn default() -> Self {
        CFBytes {
            ptr: Default::default(),
        }
    }
}

impl<'registered> HeaderRepr<'registered> for CFBytes<'registered> {
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        _dynamic_header_ptr: *mut u8,
        _constant_header_offset: usize,
        _dynamic_header_offset: usize,
        _copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut obj_ref = ObjectRef(constant_header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        vec![(CornPtr::Registered(self.ptr), constant_header_ptr)]
    }

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        let constant_header_ptr =
            unsafe { ref_buf.as_ptr().offset(constant_header_offset as isize) };
        let object_ref = ObjectRef(constant_header_ptr);
        let payload_buf = unsafe {
            constant_header_ptr.offset((object_ref.get_offset() - constant_header_offset) as isize)
        };
        self.ptr = unsafe { slice::from_raw_parts(payload_buf, object_ref.get_size()) };
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum List<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    Owned(OwnedList<'registered, T>),
    Ref(RefList<'registered, T>),
}

impl<'registered, T> Default for List<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    fn default() -> Self {
        List::Owned(OwnedList::default())
    }
}

impl<'registered, T> Index<usize> for List<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    fn index(&self, idx: usize) -> &T {
        match self {
            List::Owned(owned_list) => owned_list.index(idx),
            List::Ref(ref_list) => ref_list.index(idx),
        }
    }
}

impl<'registered, T> List<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    pub fn init(size: usize) -> List<'registered, T> {
        List::Owned(OwnedList::init(size))
    }

    pub fn from_buffer(
        ref_buf: &'registered [u8],
        constant_header_offset: usize,
    ) -> List<'registered, T> {
        let mut ref_list = RefList::default();
        ref_list.inner_deserialize(ref_buf, constant_header_offset);
        List::Ref(ref_list)
    }

    pub fn append(&mut self, val: T) {
        match self {
            List::Owned(ref mut owned_list) => owned_list.append(val),
            List::Ref(ref mut _ref_list) => {
                panic!("Should not be calling append on a ref list.")
            }
        }
    }

    pub fn replace(&mut self, idx: usize, val: T) {
        match self {
            List::Owned(ref mut owned_list) => owned_list.replace(idx, val),
            List::Ref(ref mut ref_list) => ref_list.replace(idx, val),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            List::Owned(owned_list) => owned_list.len(),
            List::Ref(ref_list) => ref_list.len(),
        }
    }
}

// TODO: use num-traits?
impl<'registered, T> HeaderRepr<'registered> for List<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        match self {
            List::Owned(owned_list) => owned_list.dynamic_header_size(),
            List::Ref(ref_list) => ref_list.dynamic_header_size(),
        }
    }

    fn dynamic_header_offset(&self) -> usize {
        8
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
        match self {
            List::Owned(owned_list) => owned_list.inner_serialize(
                constant_header_ptr,
                dynamic_header_ptr,
                constant_header_offset,
                dynamic_header_offset,
                copy_func,
            ),
            List::Ref(ref_list) => ref_list.inner_serialize(
                constant_header_ptr,
                dynamic_header_ptr,
                constant_header_offset,
                dynamic_header_offset,
                copy_func,
            ),
        }
    }

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        match self {
            List::Owned(owned_list) => {
                owned_list.inner_deserialize(ref_buf, constant_header_offset)
            }
            List::Ref(ref_list) => ref_list.inner_deserialize(ref_buf, constant_header_offset),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OwnedList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    num_space: usize,
    num_set: usize,
    list_ptr: BytesMut,
    _marker: PhantomData<&'registered [T]>,
}

impl<'registered, T> OwnedList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    pub fn init(size: usize) -> OwnedList<'registered, T> {
        let mut buf_mut = BytesMut::with_capacity(size * size_of::<T>());
        buf_mut.put(vec![0u8; size * size_of::<T>()].as_slice());
        OwnedList {
            num_space: size,
            num_set: 0,
            list_ptr: buf_mut,
            _marker: PhantomData,
        }
    }

    pub fn append(&mut self, val: T) {
        assert!(self.num_set < self.num_space);
        self.write_val(self.num_set, val);
        self.num_set += 1;
    }

    pub fn replace(&mut self, idx: usize, val: T) {
        assert!(idx < self.num_space);
        self.write_val(idx, val);
    }

    pub fn len(&self) -> usize {
        self.num_set
    }

    fn write_val(&mut self, val_idx: usize, val: T) {
        let offset = unsafe { (self.list_ptr.as_mut_ptr() as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    fn read_val(&self, val_idx: usize) -> &T {
        let offset = unsafe { (self.list_ptr.as_ptr() as *const T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts(offset, 1) };
        &t_slice[0]
    }
}

impl<'registered, T> Index<usize> for OwnedList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'registered, T> HeaderRepr<'registered> for OwnedList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.num_set * size_of::<T>()
    }

    fn dynamic_header_offset(&self) -> usize {
        8
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        _constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        // write in object ref at constant part
        let mut list_ref = ObjectRef(constant_header_ptr);
        list_ref.write_size(self.num_set);
        list_ref.write_offset(dynamic_header_offset);
        unsafe {
            copy_func(
                dynamic_header_ptr as _,
                self.list_ptr.as_ptr() as _,
                self.num_set * size_of::<T>(),
            );
        }
        Vec::default()
    }

    // Re-initializing owned list is not 0-copy. 0-copy only works for ref-list.
    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        let constant_header_ptr =
            unsafe { ref_buf.as_ptr().offset(constant_header_offset as isize) };
        let list_ref = ObjectRef(constant_header_ptr);
        self.num_set = list_ref.get_size();
        self.num_space = list_ref.get_size();
        self.list_ptr = BytesMut::with_capacity(self.num_set * size_of::<T>());
        let list_ptr = unsafe {
            constant_header_ptr.offset((list_ref.get_offset() - constant_header_offset) as isize)
        };
        // copy the bytes from the buf to the list_ptr
        self.list_ptr.chunk_mut().copy_from_slice(unsafe {
            slice::from_raw_parts(list_ptr, self.num_set * size_of::<T>())
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    ref_buf: &'registered [u8],
    constant_header_ptr: *const u8,
    constant_header_offset: usize,
    _marker: PhantomData<T>,
}

impl<'registered, T> Default for RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    fn default() -> Self {
        RefList {
            ref_buf: &[],
            constant_header_ptr: ptr::null_mut(),
            constant_header_offset: 0,
            _marker: PhantomData,
        }
    }
}

impl<'registered, T> RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    fn get_list_ref(&self) -> ObjectRef {
        ObjectRef(self.constant_header_ptr)
    }

    fn get_list_ptr(&self) -> *const u8 {
        let list_ref = self.get_list_ref();
        unsafe { self.ref_buf.as_ptr().offset(list_ref.get_offset() as isize) }
    }

    // TODO: should this even have a "replace" function
    pub fn replace(&mut self, idx: usize, val: T) {
        assert!(idx < self.get_list_ref().get_size());
        self.write_val(idx, val);
    }

    pub fn len(&self) -> usize {
        self.get_list_ref().get_size()
    }

    fn write_val(&mut self, val_idx: usize, val: T) {
        let list_ptr = self.get_list_ptr();
        let offset = unsafe { (list_ptr as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    fn read_val(&self, val_idx: usize) -> &T {
        let list_ptr = self.get_list_ptr();
        let offset = unsafe { (list_ptr as *const T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts(offset, 1) };
        &t_slice[0]
    }
}

impl<'registered, T> Index<usize> for RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        assert!(idx < self.get_list_ref().get_size());
        &self.read_val(idx)
    }
}

impl<'registered, T> HeaderRepr<'registered> for RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.get_list_ref().get_size() * size_of::<T>()
    }

    fn dynamic_header_offset(&self) -> usize {
        8
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        _constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let our_list_size = self.get_list_ref().get_size();
        let mut list_ref = ObjectRef(constant_header_ptr);
        list_ref.write_offset(dynamic_header_offset);
        list_ref.write_size(our_list_size);
        unsafe {
            copy_func(
                dynamic_header_ptr as _,
                self.get_list_ptr() as _,
                our_list_size * size_of::<T>(),
            );
        }
        Vec::default()
    }

    // CONSTANT TIME DESERIALIZATION!
    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        let constant_header_ptr =
            unsafe { ref_buf.as_ptr().offset(constant_header_offset as isize) };
        self.ref_buf = ref_buf;
        self.constant_header_ptr = constant_header_ptr;
        self.constant_header_offset = constant_header_offset;
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    Owned(OwnedVariableList<'registered, T>),
    Ref(RefVariableList<'registered, T>),
}

impl<'registered, T> Default for VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    fn default() -> VariableList<'registered, T> {
        VariableList::Owned(OwnedVariableList::default())
    }
}

impl<'registered, T> VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    pub fn init(num: usize) -> VariableList<'registered, T> {
        VariableList::Owned(OwnedVariableList::init(num))
    }

    pub fn from_buffer(
        buf: &'registered [u8],
        constant_header_offset: usize,
    ) -> VariableList<'registered, T> {
        let mut ref_list = RefVariableList::default();
        ref_list.inner_deserialize(buf, constant_header_offset);
        VariableList::Ref(ref_list)
    }

    pub fn append(&mut self, val: T) {
        match self {
            VariableList::Owned(ref mut owned) => owned.append(val),
            VariableList::Ref(_reflist) => {
                tracing::warn!("Appending to reflist is unimplemented.");
                unimplemented!();
            }
        }
    }

    pub fn replace(&mut self, idx: usize, val: T) {
        match self {
            VariableList::Owned(ref mut owned) => owned.replace(idx, val),
            VariableList::Ref(_reflist) => {
                tracing::warn!("Replacing item in reflist is unimplemented.");
                unimplemented!();
            }
        };
    }

    pub fn len(&self) -> usize {
        match self {
            VariableList::Owned(owned) => owned.len(),
            VariableList::Ref(reflist) => reflist.len(),
        }
    }

    pub fn get(&mut self, idx: usize) -> &T {
        match self {
            VariableList::Owned(owned) => owned.get(idx),
            VariableList::Ref(reflist) => reflist.get(idx),
        }
    }
}

impl<'registered, T> HeaderRepr<'registered> for VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        match self {
            VariableList::Owned(owned) => owned.dynamic_header_size(),
            VariableList::Ref(reflist) => reflist.dynamic_header_size(),
        }
    }

    fn dynamic_header_offset(&self) -> usize {
        match self {
            VariableList::Owned(owned) => owned.dynamic_header_offset(),
            VariableList::Ref(reflist) => reflist.dynamic_header_offset(),
        }
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
        match self {
            VariableList::Owned(owned) => owned.inner_serialize(
                constant_header_ptr,
                dynamic_header_ptr,
                constant_header_offset,
                dynamic_header_offset,
                copy_func,
            ),
            VariableList::Ref(reflist) => reflist.inner_serialize(
                constant_header_ptr,
                dynamic_header_ptr,
                constant_header_offset,
                dynamic_header_offset,
                copy_func,
            ),
        }
    }

    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        match self {
            VariableList::Owned(owned) => owned.inner_deserialize(ref_buf, constant_header_offset),
            VariableList::Ref(reflist) => {
                reflist.inner_deserialize(ref_buf, constant_header_offset)
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct OwnedVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    _marker: PhantomData<&'registered [u8]>,
}

impl<'registered, T> OwnedVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    pub fn init(num: usize) -> OwnedVariableList<'registered, T> {
        OwnedVariableList {
            num_space: num,
            num_set: 0,
            elts: Vec::with_capacity(num),
            _marker: PhantomData,
        }
    }

    pub fn append(&mut self, val: T) {
        assert!(self.num_set < self.num_space);
        self.elts.push(val);
        self.num_set += 1;
    }

    pub fn replace(&mut self, idx: usize, val: T) {
        assert!(idx < self.num_space);
        self.elts[idx] = val;
    }

    pub fn len(&self) -> usize {
        self.num_set
    }

    pub fn get(&self, idx: usize) -> &T {
        &self.elts[idx]
    }
}

impl<'registered, T> HeaderRepr<'registered> for OwnedVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.elts
            .iter()
            .map(|x| x.dynamic_header_size() + T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    fn dynamic_header_offset(&self) -> usize {
        8
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        _constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();
        // write in list size to the constant offset
        let mut list_ref = ObjectRef(constant_header_ptr);
        list_ref.write_size(self.elts.len());
        list_ref.write_offset(dynamic_header_offset);

        let mut cur_header_off_ptr = dynamic_header_ptr;
        let mut cur_dynamic_off_ptr =
            unsafe { dynamic_header_ptr.offset(self.dynamic_header_offset() as isize) };
        let mut cur_constant_off = dynamic_header_offset;
        let mut cur_dynamic_off = dynamic_header_offset + self.dynamic_header_offset();

        for elt in self.elts.iter() {
            ret.append(&mut elt.inner_serialize(
                cur_header_off_ptr,
                cur_dynamic_off_ptr,
                cur_constant_off,
                cur_dynamic_off,
                copy_func,
            ));
            cur_header_off_ptr =
                unsafe { cur_header_off_ptr.offset(T::CONSTANT_HEADER_SIZE as isize) };
            cur_constant_off += T::CONSTANT_HEADER_SIZE;
            cur_dynamic_off_ptr =
                unsafe { cur_dynamic_off_ptr.offset(elt.dynamic_header_size() as isize) };
            cur_dynamic_off += elt.dynamic_header_size();
        }
        ret
    }

    // Not constant time deserialization.
    // Because of object representation.
    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        let constant_header_ptr =
            unsafe { ref_buf.as_ptr().offset(constant_header_offset as isize) };
        let list_ref = ObjectRef(constant_header_ptr);
        let mut elts: Vec<T> = Vec::with_capacity(list_ref.get_size());
        let mut cur_offset = list_ref.get_offset();
        for _i in 0..list_ref.get_size() {
            let mut elt = T::default();
            elt.inner_deserialize(ref_buf, cur_offset);
            cur_offset += T::CONSTANT_HEADER_SIZE;
            elts.push(elt);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RefVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    ref_buf: &'registered [u8],
    constant_header_ptr: *const u8,
    constant_header_offset: usize,
    elts: Vec<T>,
    filled_in: Vec<bool>,
}

impl<'registered, T> Default for RefVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    fn default() -> RefVariableList<'registered, T> {
        RefVariableList {
            ref_buf: &[],
            constant_header_ptr: ptr::null_mut(),
            constant_header_offset: 0,
            elts: Vec::default(),
            filled_in: Vec::default(),
        }
    }
}

impl<'registered, T> RefVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    fn get_list_ref(&self) -> ObjectRef {
        let list_ref = ObjectRef(self.constant_header_ptr);
        list_ref
    }

    pub fn get(&mut self, idx: usize) -> &T {
        if self.filled_in[idx] {
            &self.elts[idx]
        } else {
            let list_ref = self.get_list_ref();
            assert!(idx < list_ref.get_size());
            let dynamic_offset = self.get_list_ref().get_offset() + idx * T::CONSTANT_HEADER_SIZE;
            let mut obj = T::default();
            obj.inner_deserialize(self.ref_buf, dynamic_offset);
            self.elts[idx] = obj;
            self.filled_in[idx] = true;
            &self.elts[idx]
        }
    }

    pub fn len(&self) -> usize {
        self.get_list_ref().get_size()
    }
}

impl<'registered, T> HeaderRepr<'registered> for RefVariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        let list_ref = self.get_list_ref();
        let mut ret = 0;
        for idx in 0..list_ref.get_size() {
            let dynamic_offset = self.get_list_ref().get_offset() + idx * T::CONSTANT_HEADER_SIZE;
            let mut obj = T::default();
            obj.inner_deserialize(self.ref_buf, dynamic_offset);
            ret += T::CONSTANT_HEADER_SIZE + obj.dynamic_header_size();
        }
        ret
    }

    fn dynamic_header_offset(&self) -> usize {
        8
    }

    fn inner_serialize<'normal>(
        &self,
        constant_header_ptr: *mut u8,
        dynamic_header_ptr: *mut u8,
        _constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        // write in list_ref to constant header ptr
        unsafe {
            copy_func(
                constant_header_ptr as _,
                self.constant_header_ptr as _,
                Self::CONSTANT_HEADER_SIZE,
            )
        };

        let mut ret: Vec<(CornPtr<'registered, 'normal>, *mut u8)> = Vec::default();
        let mut cur_header_ptr = dynamic_header_ptr;
        let mut cur_header_off = dynamic_header_offset;
        let our_dynamic_header_off = self.dynamic_header_offset();
        let mut cur_dynamic_ptr = unsafe { cur_header_ptr.offset(our_dynamic_header_off as isize) };
        let mut cur_dynamic_off = dynamic_header_offset + our_dynamic_header_off;
        let list_ref = self.get_list_ref();

        for idx in 0..list_ref.get_size() {
            let dynamic_offset = self.get_list_ref().get_offset() + idx * T::CONSTANT_HEADER_SIZE;
            let mut elt = T::default();
            elt.inner_deserialize(self.ref_buf, dynamic_offset);
            ret.append(&mut elt.inner_serialize(
                cur_header_ptr,
                cur_header_off as _,
                cur_dynamic_ptr as _,
                cur_dynamic_off,
                copy_func,
            ));
            cur_header_ptr = unsafe { cur_header_ptr.offset(T::CONSTANT_HEADER_SIZE as isize) };
            cur_header_off += T::CONSTANT_HEADER_SIZE;
            cur_dynamic_ptr = unsafe { cur_dynamic_ptr.offset(elt.dynamic_header_size() as isize) };
            cur_dynamic_off += elt.dynamic_header_size();
        }
        ret
    }

    // CONSTANT TIME DESERIALIZATION
    fn inner_deserialize(&mut self, ref_buf: &'registered [u8], constant_header_offset: usize) {
        let constant_header_ptr =
            unsafe { ref_buf.as_ptr().offset(constant_header_offset as isize) };
        self.ref_buf = ref_buf;
        self.constant_header_ptr = constant_header_ptr;
        self.constant_header_offset = constant_header_offset;
        let size = self.get_list_ref().get_size();
        self.elts = vec![T::default(); size];
        self.filled_in = vec![false; size];
    }
}
pub struct ObjectRef(pub *const u8);

impl ObjectRef {
    pub fn get_size(&self) -> usize {
        unsafe { LittleEndian::read_u32(slice::from_raw_parts(self.0, 4)) as usize }
    }

    pub fn get_offset(&self) -> usize {
        unsafe { LittleEndian::read_u32(slice::from_raw_parts(self.0.offset(4), 4)) as usize }
    }

    pub fn write_size(&mut self, size: usize) {
        unsafe {
            LittleEndian::write_u32(slice::from_raw_parts_mut(self.0 as _, 4), size as u32);
        }
    }

    pub fn write_offset(&mut self, offset: usize) {
        unsafe {
            LittleEndian::write_u32(
                slice::from_raw_parts_mut(self.0.offset(4) as _, 4),
                offset as u32,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cornflakes_libos::{CornPtr, PtrAttributes, ScatterGather};
    use cornflakes_utils::test_init;
    use libc;
    use std::{io::Write, slice, str};
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
    fn encode_single_string_ptr() {
        test_init!();
        let string = "HELLO".to_string();
        let cf_string = CFString::new(string.as_str());
        let ptr = unsafe { libc::malloc(SIZE_FIELD + OFFSET_FIELD) };
        let header_buffer =
            unsafe { slice::from_raw_parts_mut(ptr as *mut u8, SIZE_FIELD + OFFSET_FIELD) };

        let cf = cf_string.serialize(header_buffer, copy_func);
        assert!(cf.num_segments() == 2);

        // check object ref in the first scatter-gather entry
        let first_entry = cf.get(0).unwrap();
        assert!(first_entry.buf_size() == 8);
        assert!(
            *first_entry
                == CornPtr::Normal(unsafe {
                    slice::from_raw_parts(ptr as *const u8, SIZE_FIELD + OFFSET_FIELD)
                })
        );
        // check payload in first cornflake (header)
        let header_slice = ObjectRef(first_entry.as_ref().as_ptr());
        // length of HELLO
        assert!(header_slice.get_size() == 5);
        // offset it should have
        assert!(header_slice.get_offset() == 8);

        // check 2nd cornflake
        let second_entry = cf.get(1).unwrap();
        assert!(second_entry.buf_size() == 5);
        // check payload in second cornflake
        let str_ref = str::from_utf8(second_entry.as_ref()).unwrap();
        assert!(str_ref == string.as_str());
        unsafe {
            libc::free(ptr);
        }
    }

    #[test]
    fn decode_single_string_ptr() {
        test_init!();
        let string = "HELLO".to_string();
        let ptr = unsafe { libc::malloc(SIZE_FIELD + OFFSET_FIELD + string.len()) };
        let mut object_ref = ObjectRef(ptr as _);
        object_ref.write_size(5);
        object_ref.write_offset(8);

        let header_buffer = unsafe {
            slice::from_raw_parts_mut(ptr as *mut u8, SIZE_FIELD + OFFSET_FIELD + string.len())
        };
        let mut header_buffer_slice = &mut header_buffer[8..];
        header_buffer_slice
            .write(string.as_str().as_bytes())
            .unwrap();
        let final_buffer = unsafe {
            slice::from_raw_parts(ptr as *const u8, SIZE_FIELD + OFFSET_FIELD + string.len())
        };

        let mut cf_string = CFString::default();
        cf_string.deserialize(final_buffer);

        assert!(cf_string.ptr.len() == 5);
        let str_ref = str::from_utf8(cf_string.ptr).unwrap();
        assert!(str_ref == string.as_str());
        unsafe {
            libc::free(ptr);
        }
    }

    #[test]
    fn encode_single_bytes_ptr() {
        test_init!();
        let string = "HELLO".to_string();
        let cf_bytes = CFBytes::new(string.as_str().as_bytes());
        let ptr = unsafe { libc::malloc(SIZE_FIELD + OFFSET_FIELD) };
        let header_buffer =
            unsafe { slice::from_raw_parts_mut(ptr as *mut u8, SIZE_FIELD + OFFSET_FIELD) };

        let cf = cf_bytes.serialize(header_buffer, copy_func);
        assert!(cf.num_segments() == 2);

        // check object ref in the first scatter-gather entry
        let first_entry = cf.get(0).unwrap();
        assert!(first_entry.buf_size() == 8);
        assert!(
            *first_entry
                == CornPtr::Normal(unsafe {
                    slice::from_raw_parts(ptr as *const u8, SIZE_FIELD + OFFSET_FIELD)
                })
        );
        // check payload in first cornflake (header)
        let header_slice = ObjectRef(first_entry.as_ref().as_ptr());
        // length of HELLO
        assert!(header_slice.get_size() == 5);
        // offset it should have
        assert!(header_slice.get_offset() == 8);

        // check 2nd cornflake
        let second_entry = cf.get(1).unwrap();
        assert!(second_entry.buf_size() == 5);
        // check payload in second cornflake
        assert!(second_entry.as_ref() == string.as_str().as_bytes());
        assert!(str::from_utf8(second_entry.as_ref()).unwrap() == string.as_str());
        unsafe {
            libc::free(ptr);
        }
    }

    #[test]
    fn decode_single_bytes_ptr() {
        test_init!();
        let string = "HELLO".to_string();
        let ptr = unsafe { libc::malloc(SIZE_FIELD + OFFSET_FIELD + string.len()) };
        let mut object_ref = ObjectRef(ptr as _);
        object_ref.write_size(5);
        object_ref.write_offset(8);

        let header_buffer = unsafe {
            slice::from_raw_parts_mut(ptr as *mut u8, SIZE_FIELD + OFFSET_FIELD + string.len())
        };
        let mut header_buffer_slice = &mut header_buffer[8..];
        header_buffer_slice
            .write(string.as_str().as_bytes())
            .unwrap();
        let final_buffer = unsafe {
            slice::from_raw_parts(ptr as *const u8, SIZE_FIELD + OFFSET_FIELD + string.len())
        };

        let mut cf_bytes = CFBytes::default();
        cf_bytes.deserialize(final_buffer);

        assert!(cf_bytes.ptr.len() == 5);
        let str_ref = str::from_utf8(cf_bytes.ptr).unwrap();
        assert!(str_ref == string.as_str());
        unsafe {
            libc::free(ptr);
        }
    }

    #[test]
    fn test_int32_list() {
        test_init!();
        let mut list = List::<i32>::init(3);
        list.append(10);
        assert!(list[0] == 10);
        assert!(list.len() == 1);
        list.append(11);
        assert!(list[1] == 11);
        assert!(list.len() == 2);
        list.append(12);
        assert!(list[2] == 12);
        list.replace(0, 15);
        assert!(list[0] == 15);
        assert!(list.len() == 3);

        // serialize the int list
        let ptr = unsafe { libc::malloc(20) };
        let header_buffer = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, 20) };
        let cf = list.serialize(header_buffer, copy_func);

        assert!(cf.num_segments() == 1);
        let header_entry = cf.get(0).unwrap();
        assert!(
            *header_entry == CornPtr::Normal(unsafe { slice::from_raw_parts(ptr as *mut u8, 20) })
        );

        // check that the list ref at the head is correct
        let list_ref = ObjectRef(ptr as *const u8);
        assert!(list_ref.get_size() == 3);
        assert!(list_ref.get_offset() == 8);

        // check the contents of the int list in serialized format
        let first_int = LittleEndian::read_i32(unsafe {
            slice::from_raw_parts((ptr as *const u8).offset(8), 4)
        });
        assert!(first_int == 15);

        let second_int = LittleEndian::read_i32(unsafe {
            slice::from_raw_parts((ptr as *const u8).offset(12), 4)
        });
        assert!(second_int == 11);
        let third_int = LittleEndian::read_i32(unsafe {
            slice::from_raw_parts((ptr as *const u8).offset(16), 4)
        });
        assert!(third_int == 12);

        // deserialize the int list
        let deserialized_list =
            List::<i32>::from_buffer(unsafe { slice::from_raw_parts(ptr as *const u8, 20) }, 0);
        assert!(deserialized_list.len() == 3);
        assert!(deserialized_list[0] == 15);
        assert!(deserialized_list[1] == 11);
        assert!(deserialized_list[2] == 12);

        let mut deserialized_list_owned = List::<i32>::default();
        deserialized_list_owned.deserialize(unsafe { slice::from_raw_parts(ptr as *const u8, 24) });
        assert!(deserialized_list.len() == 3);
        assert!(deserialized_list[0] == 15);
        assert!(deserialized_list[1] == 11);
        assert!(deserialized_list[2] == 12);
        unsafe {
            libc::free(ptr);
        }
    }

    #[test]
    fn test_u64_list() {
        test_init!();
        let mut list = List::<u64>::init(3);
        list.append(100000);
        assert!(list[0] == 100000);
        assert!(list.len() == 1);
        list.append(110000);
        assert!(list[1] == 110000);
        assert!(list.len() == 2);
        list.append(120000);
        assert!(list[2] == 120000);
        list.replace(0, 150000);
        assert!(list[0] == 150000);
        assert!(list.len() == 3);

        // serialize the int list
        let ptr = unsafe { libc::malloc(32) };
        let header_buffer = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, 32) };
        let cf = list.serialize(header_buffer, copy_func);

        assert!(cf.num_segments() == 1);
        let header_entry = cf.get(0).unwrap();
        assert!(
            *header_entry == CornPtr::Normal(unsafe { slice::from_raw_parts(ptr as *mut u8, 32) })
        );

        // check list_ref at front
        let list_ref = ObjectRef(ptr as _);
        assert!(list_ref.get_size() == 3);
        assert!(list_ref.get_offset() == 8);

        // check the contents of the int list in serialized format
        let first_int = LittleEndian::read_u64(unsafe {
            slice::from_raw_parts((ptr as *const u8).offset(8), 8)
        });
        assert!(first_int == 150000);

        let second_int = LittleEndian::read_u64(unsafe {
            slice::from_raw_parts((ptr as *const u8).offset(16), 8)
        });
        assert!(second_int == 110000);
        let third_int = LittleEndian::read_u64(unsafe {
            slice::from_raw_parts((ptr as *const u8).offset(24), 8)
        });
        assert!(third_int == 120000);

        // deserialize the int list
        let deserialized_list =
            List::<u64>::from_buffer(unsafe { slice::from_raw_parts(ptr as *const u8, 24) }, 0);
        assert!(deserialized_list.len() == 3);
        assert!(deserialized_list[0] == 150000);
        assert!(deserialized_list[1] == 110000);
        assert!(deserialized_list[2] == 120000);

        let mut deserialized_list_owned = List::<u64>::default();
        deserialized_list_owned.deserialize(unsafe { slice::from_raw_parts(ptr as *const u8, 24) });
        assert!(deserialized_list.len() == 3);
        assert!(deserialized_list[0] == 150000);
        assert!(deserialized_list[1] == 110000);
        assert!(deserialized_list[2] == 120000);

        unsafe {
            libc::free(ptr);
        }
    }

    #[test]
    fn test_bytes_list() {
        test_init!();
        let string1 = "HELLO1".to_string(); // first sorted
        let string2 = "HELLO22".to_string(); // second sorted
        let string3 = "HELLO333".to_string(); // third sorted
        let string4 = "HELLO2222".to_string(); // last sorted
        let mut bytes_list = OwnedVariableList::<CFBytes>::init(3);

        bytes_list.append(CFBytes::new(string1.as_str().as_bytes()));
        assert!(bytes_list.len() == 1);
        bytes_list.append(CFBytes::new(string2.as_str().as_bytes()));
        assert!(bytes_list.len() == 2);
        bytes_list.append(CFBytes::new(string3.as_str().as_bytes()));

        assert!(*bytes_list.get(0) == CFBytes::new(string1.as_str().as_bytes()));
        assert!(*bytes_list.get(1) == CFBytes::new(string2.as_str().as_bytes()));
        assert!(*bytes_list.get(2) == CFBytes::new(string3.as_str().as_bytes()));
        bytes_list.replace(1, CFBytes::new(string4.as_str().as_bytes()));

        // serialize
        let header_size =
            3 * CFBytes::CONSTANT_HEADER_SIZE + OwnedVariableList::<CFBytes>::CONSTANT_HEADER_SIZE;
        let ptr = unsafe { libc::malloc(header_size) };
        let header_buffer = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, header_size) };
        let cf = bytes_list.serialize(header_buffer, copy_func);

        assert!(cf.num_segments() == 4);
        // check the header
        let header_ptr = cf.get(0).unwrap();
        assert!(
            *header_ptr
                == CornPtr::Normal(unsafe { slice::from_raw_parts(ptr as *const u8, header_size) })
        );

        // check the list ref in the header is correct
        let list_ref = ObjectRef(ptr as *const u8);
        assert!(list_ref.get_size() == 3);
        assert!(list_ref.get_offset() == 8);

        for i in 0..3 {
            let offset: isize = (CFBytes::CONSTANT_HEADER_SIZE * i) as isize
                + OwnedVariableList::<CFBytes>::CONSTANT_HEADER_SIZE as isize;
            let buf = unsafe { (ptr as *const u8).offset(offset) };
            let object_ref = ObjectRef(buf);
            if i == 0 {
                // HELLO1
                assert!(object_ref.get_size() == string1.len());
                assert!(object_ref.get_offset() == 32);
            } else if i == 1 {
                // HELLO2222
                assert!(object_ref.get_size() == string4.len());
                assert!(object_ref.get_offset() == 46);
            } else if i == 2 {
                // HELLO333
                assert!(object_ref.get_size() == string3.len());
                assert!(object_ref.get_offset() == 38);
            } else {
                unreachable!();
            }
        }

        // check each of the following CornPtrs
        assert!(*cf.get(1).unwrap() == CornPtr::Registered(string1.as_str().as_bytes()));
        assert!(*cf.get(2).unwrap() == CornPtr::Registered(string3.as_str().as_bytes()));
        assert!(*cf.get(3).unwrap() == CornPtr::Registered(string4.as_str().as_bytes()));

        // now DESERIALIZE a string
        let total_serialized_length = OwnedVariableList::<CFBytes>::CONSTANT_HEADER_SIZE
            + CFBytes::CONSTANT_HEADER_SIZE * 4
            + string1.len()
            + string2.len()
            + string3.len()
            + string4.len();
        let ptr_to_deserialize = unsafe { libc::malloc(total_serialized_length) };

        let mut list_ref = ObjectRef(ptr_to_deserialize as _);
        list_ref.write_size(4);
        list_ref.write_offset(8);

        // fill in the stuff that should be in the serialized version
        for i in 0..4 {
            let header_off = unsafe {
                (ptr_to_deserialize as *mut u8)
                    .offset(8 + i * CFBytes::CONSTANT_HEADER_SIZE as isize)
            };
            let mut object_ref = ObjectRef(header_off as *const u8);
            if i == 0 {
                let offset = 8 + 4 * CFBytes::CONSTANT_HEADER_SIZE;
                object_ref.write_size(string1.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string1.len(),
                    )
                };
                data_slice.write(string1.as_str().as_bytes()).unwrap();
            } else if i == 1 {
                let offset = 8 + 4 * CFBytes::CONSTANT_HEADER_SIZE + string1.len();
                object_ref.write_size(string2.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string2.len(),
                    )
                };
                data_slice.write(string2.as_str().as_bytes()).unwrap();
            } else if i == 2 {
                let offset = 8 + 4 * CFBytes::CONSTANT_HEADER_SIZE + string1.len() + string2.len();
                object_ref.write_size(string3.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string3.len(),
                    )
                };
                data_slice.write(string3.as_str().as_bytes()).unwrap();
            } else if i == 3 {
                let offset = 8
                    + 4 * CFBytes::CONSTANT_HEADER_SIZE
                    + string1.len()
                    + string2.len()
                    + string3.len();
                object_ref.write_size(string4.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string4.len(),
                    )
                };
                data_slice.write(string4.as_str().as_bytes()).unwrap();
            }
        }

        let mut deserialized_bytes_list = RefVariableList::<CFBytes>::default();
        deserialized_bytes_list.inner_deserialize(
            unsafe {
                slice::from_raw_parts(ptr_to_deserialize as *const u8, total_serialized_length)
            },
            0,
        );

        assert!(deserialized_bytes_list.len() == 4);

        let first = deserialized_bytes_list.get(0);
        assert!(first.len() == string1.len());
        assert!(first.ptr == string1.as_str().as_bytes());

        let second = deserialized_bytes_list.get(1);
        assert!(second.len() == string2.len());
        assert!(second.ptr == string2.as_str().as_bytes());

        let third = deserialized_bytes_list.get(2);
        assert!(third.len() == string3.len());
        assert!(third.ptr == string3.as_str().as_bytes());

        let fourth = deserialized_bytes_list.get(3);
        assert!(fourth.len() == string4.len());
        assert!(fourth.ptr == string4.as_str().as_bytes());

        unsafe {
            libc::free(ptr);
            libc::free(ptr_to_deserialize);
        }
    }

    #[test]
    fn test_string_list() {
        test_init!();
        let string1 = "HELLO1".to_string(); // first sorted
        let string2 = "HELLO22".to_string(); // second sorted
        let string3 = "HELLO333".to_string(); // third sorted
        let string4 = "HELLO2222".to_string(); // last sorted
        let mut string_list = OwnedVariableList::<CFString>::init(3);

        string_list.append(CFString::new(string1.as_str()));
        assert!(string_list.len() == 1);
        string_list.append(CFString::new(string2.as_str()));
        assert!(string_list.len() == 2);
        string_list.append(CFString::new(string3.as_str()));

        assert!(*string_list.get(0) == CFString::new(string1.as_str()));
        assert!(*string_list.get(1) == CFString::new(string2.as_str()));
        assert!(*string_list.get(2) == CFString::new(string3.as_str()));
        string_list.replace(1, CFString::new(string4.as_str()));

        // serialize
        let header_size = 3 * CFString::CONSTANT_HEADER_SIZE
            + OwnedVariableList::<CFString>::CONSTANT_HEADER_SIZE;
        let ptr = unsafe { libc::malloc(header_size) };
        let header_buffer = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, header_size) };
        let cf = string_list.serialize(header_buffer, copy_func);

        assert!(cf.num_segments() == 4);
        // check the header
        let header_ptr = cf.get(0).unwrap();
        assert!(
            *header_ptr
                == CornPtr::Normal(unsafe { slice::from_raw_parts(ptr as *const u8, header_size) })
        );

        // check the list ref
        let list_ref = ObjectRef(ptr as _);
        assert!(list_ref.get_size() == 3);
        assert!(list_ref.get_offset() == 8);

        for i in 0..3 {
            let offset: isize = (CFString::CONSTANT_HEADER_SIZE * i + 8) as isize;
            let buf = unsafe { (ptr as *const u8).offset(offset) };
            let object_ref = ObjectRef(buf);
            if i == 0 {
                // HELLO1
                assert!(object_ref.get_size() == string1.len());
                assert!(object_ref.get_offset() == 32);
            } else if i == 1 {
                // HELLO2222
                assert!(object_ref.get_size() == string4.len());
                assert!(object_ref.get_offset() == 46);
            } else if i == 2 {
                // HELLO333
                assert!(object_ref.get_size() == string3.len());
                assert!(object_ref.get_offset() == 38);
            } else {
                unreachable!();
            }
        }

        // check each of the following CornPtrs
        assert!(*cf.get(1).unwrap() == CornPtr::Registered(string1.as_str().as_bytes()));
        assert!(*cf.get(2).unwrap() == CornPtr::Registered(string3.as_str().as_bytes()));
        assert!(*cf.get(3).unwrap() == CornPtr::Registered(string4.as_str().as_bytes()));

        // now DESERIALIZE a string
        let total_serialized_length = OwnedVariableList::<CFString>::CONSTANT_HEADER_SIZE
            + CFString::CONSTANT_HEADER_SIZE * 4
            + string1.len()
            + string2.len()
            + string3.len()
            + string4.len();
        let ptr_to_deserialize = unsafe {
            libc::malloc(
                OwnedVariableList::<CFString>::CONSTANT_HEADER_SIZE
                    + CFString::CONSTANT_HEADER_SIZE * 4
                    + string1.len()
                    + string2.len()
                    + string3.len()
                    + string4.len(),
            )
        };

        let mut list_ref = ObjectRef(ptr_to_deserialize as _);
        list_ref.write_offset(8);
        list_ref.write_size(4);

        // fill in the stuff that should be in the serialized version
        for i in 0..4 {
            let header_off = unsafe {
                (ptr_to_deserialize as *mut u8)
                    .offset(8 + i * CFString::CONSTANT_HEADER_SIZE as isize)
            };
            let mut object_ref = ObjectRef(header_off as *const u8);
            if i == 0 {
                let offset = 8 + 4 * CFString::CONSTANT_HEADER_SIZE;
                object_ref.write_size(string1.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string1.len(),
                    )
                };
                data_slice.write(string1.as_str().as_bytes()).unwrap();
            } else if i == 1 {
                let offset = 8 + 4 * CFString::CONSTANT_HEADER_SIZE + string1.len();
                object_ref.write_size(string2.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string2.len(),
                    )
                };
                data_slice.write(string2.as_str().as_bytes()).unwrap();
            } else if i == 2 {
                let offset = 8 + 4 * CFString::CONSTANT_HEADER_SIZE + string1.len() + string2.len();
                object_ref.write_size(string3.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string3.len(),
                    )
                };
                data_slice.write(string3.as_str().as_bytes()).unwrap();
            } else if i == 3 {
                let offset = 8
                    + 4 * CFString::CONSTANT_HEADER_SIZE
                    + string1.len()
                    + string2.len()
                    + string3.len();
                object_ref.write_size(string4.len());
                object_ref.write_offset(offset);
                let mut data_slice = unsafe {
                    slice::from_raw_parts_mut(
                        (ptr_to_deserialize as *mut u8).offset(offset as isize),
                        string4.len(),
                    )
                };
                data_slice.write(string4.as_str().as_bytes()).unwrap();
            }
        }

        let mut deserialized_string_list = VariableList::<CFString>::from_buffer(
            unsafe {
                slice::from_raw_parts(ptr_to_deserialize as *const u8, total_serialized_length)
            },
            0,
        );

        assert!(deserialized_string_list.len() == 4);

        let first = deserialized_string_list.get(0);
        assert!(first.len() == string1.len());
        assert!(first.to_string() == string1);

        let second = deserialized_string_list.get(1);
        assert!(second.len() == string2.len());
        assert!(second.to_string() == string2);

        let third = deserialized_string_list.get(2);
        assert!(third.len() == string3.len());
        assert!(third.to_string() == string3);

        let fourth = deserialized_string_list.get(3);
        assert!(fourth.len() == string4.len());
        assert!(fourth.to_string() == string4);

        unsafe {
            libc::free(ptr);
            libc::free(ptr_to_deserialize);
        }
    }
}
