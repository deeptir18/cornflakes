use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use cornflakes_libos::{CornPtr, Cornflake, PtrAttributes};
use std::{
    fmt::Debug,
    marker::PhantomData,
    mem::{size_of, transmute},
    ops::Index,
    ptr, slice,
};

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;

#[derive(Debug, Default, Clone, PartialEq, Eq, Copy)]
pub struct CFString<'a> {
    pub ptr: &'a [u8],
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Copy)]
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

    fn inner_serialize<'normal>(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)>;

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize);

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
        // sort all the references by size, but keep offsets the same
        let mut cornptrs = self.inner_serialize(header_ptr, copy_func, 0);
        cf.add_entry(CornPtr::Normal(header_buffer));
        cornptrs.sort_by(|a, b| a.0.buf_size().partial_cmp(&b.0.buf_size()).unwrap());
        let mut cur_offset = self.dynamic_header_size();
        for (cornptr, offset) in cornptrs.into_iter() {
            let mut obj_ref = ObjectRef(offset as *const u8);
            obj_ref.write_offset(cur_offset);
            cf.add_entry(cornptr);
            cur_offset += obj_ref.get_size() as usize;
        }
        cf
    }

    fn deserialize(&mut self, buf: &'registered [u8], size: usize) {
        self.inner_deserialize(buf.as_ptr(), size, 0)
    }
}

impl<'registered> HeaderRepr<'registered> for CFString<'registered> {
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn inner_serialize<'normal>(
        &self,
        header_ptr: *mut u8,
        _copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut obj_ref = ObjectRef(header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        vec![(CornPtr::Registered(self.ptr), obj_ref.get_offset_ptr())]
    }

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        assert!(size == Self::CONSTANT_HEADER_SIZE);
        let object_ref = ObjectRef(buf);
        let payload_buf =
            unsafe { buf.offset((object_ref.get_offset() - relative_offset) as isize) };
        self.ptr = unsafe { slice::from_raw_parts(payload_buf, object_ref.get_size()) };
    }
}

impl<'registered> HeaderRepr<'registered> for CFBytes<'registered> {
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn inner_serialize<'normal>(
        &self,
        header_ptr: *mut u8,
        _copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Vec<(CornPtr<'registered, 'normal>, *mut u8)> {
        let mut obj_ref = ObjectRef(header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        vec![(CornPtr::Registered(self.ptr), obj_ref.get_offset_ptr())]
    }

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        assert!(size == Self::CONSTANT_HEADER_SIZE);
        let object_ref = ObjectRef(buf);
        let payload_buf =
            unsafe { buf.offset((object_ref.get_offset() - relative_offset) as isize) };
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

impl<'registered, T> List<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    pub fn init(size: usize) -> List<'registered, T> {
        List::Owned(OwnedList::init(size))
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

    pub fn size(&self) -> usize {
        match self {
            List::Owned(owned_list) => owned_list.size(),
            List::Ref(ref_list) => ref_list.size(),
        }
    }
}

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
        match self {
            List::Owned(owned_list) => owned_list.inner_serialize(header_ptr, copy_func, offset),
            List::Ref(ref_list) => ref_list.inner_serialize(header_ptr, copy_func, offset),
        }
    }

    // TODO: figure out the correct layout for list. It can't be vec because you can't deserialize
    // zero-copy. Needs to be some custom implementation of sorts.
    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        match self {
            List::Owned(owned_list) => owned_list.inner_deserialize(buf, size, relative_offset),
            List::Ref(ref_list) => ref_list.inner_deserialize(buf, size, relative_offset),
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
        let buf_mut = BytesMut::with_capacity(size * size_of::<T>());
        OwnedList {
            num_space: size,
            num_set: 0,
            list_ptr: buf_mut,
            _marker: PhantomData,
        }
    }

    pub fn append(&mut self, val: T) {
        assert!(self.num_set < self.num_space);
        self.write_val(self.num_set + 1, val);
        self.num_set += 1;
    }

    pub fn replace(&mut self, idx: usize, val: T) {
        self.write_val(idx, val);
    }

    pub fn size(&self) -> usize {
        self.num_set
    }

    fn write_val(&mut self, val_idx: usize, val: T) {
        let offset = unsafe {
            self.list_ptr
                .as_mut_ptr()
                .offset((val_idx * size_of::<T>()) as isize)
        };
        let slice = unsafe { slice::from_raw_parts_mut(offset, size_of::<T>()) };
        let t_slice = unsafe { transmute::<&mut [u8], &mut [T]>(slice) };
        t_slice[0] = val;
    }

    fn read_val(&self, val_idx: usize) -> &T {
        let offset = unsafe {
            self.list_ptr
                .as_ptr()
                .offset((val_idx * size_of::<T>()) as isize)
        };
        let slice = unsafe { slice::from_raw_parts(offset, size_of::<T>()) };
        let t_slice = unsafe { transmute::<&[u8], &[T]>(slice) };
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
        unsafe {
            copy_func(
                header_ptr as _,
                self.list_ptr.as_ptr() as _,
                self.num_set * size_of::<T>(),
            );
        }
        Vec::default()
    }

    // Re-initializing owned list is not 0-copy. 0-copy only works for ref-list.
    fn inner_deserialize(&mut self, buf: *const u8, size: usize, _relative_offset: usize) {
        self.num_set = size;
        self.num_space = size;
        self.list_ptr = BytesMut::with_capacity(size * size_of::<T>());
        // copy the bytes from the buf to the list_ptr
        self.list_ptr
            .copy_from_slice(unsafe { slice::from_raw_parts(buf, size * size_of::<T>()) });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    num_space: usize,
    list_ptr: *mut u8,
    _marker: PhantomData<&'registered [T]>,
}

impl<'registered, T> Default for RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    fn default() -> Self {
        RefList {
            num_space: 0,
            list_ptr: ptr::null_mut(),
            _marker: PhantomData,
        }
    }
}

impl<'registered, T> RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    pub fn replace(&mut self, idx: usize, val: T) {
        self.write_val(idx, val);
    }

    pub fn size(&self) -> usize {
        self.num_space
    }

    fn write_val(&self, val_idx: usize, val: T) {
        let offset =
            unsafe { (self.list_ptr as *mut u8).offset((val_idx * size_of::<T>()) as isize) };
        // TODO: this is extremely sketchy.
        let slice = unsafe { slice::from_raw_parts_mut(offset, size_of::<T>()) };
        let t_slice = unsafe { transmute::<&mut [u8], &mut [T]>(slice) };
        t_slice[0] = val;
    }

    fn read_val(&self, val_idx: usize) -> &T {
        let offset = unsafe { self.list_ptr.offset((val_idx * size_of::<T>()) as isize) };
        let slice = unsafe { slice::from_raw_parts(offset, size_of::<T>()) };
        let t_slice = unsafe { transmute::<&[u8], &[T]>(slice) };
        &t_slice[0]
    }
}

impl<'registered, T> Index<usize> for RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'registered, T> HeaderRepr<'registered> for RefList<'registered, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.num_space * size_of::<T>()
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
        unsafe {
            copy_func(
                header_ptr as _,
                self.list_ptr as _,
                self.num_space * size_of::<T>(),
            );
        }
        Vec::default()
    }

    fn inner_deserialize(&mut self, buf: *const u8, size: usize, _relative_offset: usize) {
        self.num_space = size;
        self.list_ptr = buf as _;
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    _marker: PhantomData<&'registered [u8]>,
}

impl<'registered, T> VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    pub fn init(num: usize) -> VariableList<'registered, T> {
        VariableList {
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

    pub fn size(&self) -> usize {
        self.num_set
    }

    pub fn dynamic_header_offset(&self) -> usize {
        self.elts.iter().map(|_x| T::CONSTANT_HEADER_SIZE).sum()
    }
}
impl<'registered, T> Index<usize> for VariableList<'registered, T>
where
    T: HeaderRepr<'registered> + Debug + Default + PartialEq + Eq + Clone,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        assert!(idx < self.num_space);
        &self.elts[idx]
    }
}

impl<'registered, T> HeaderRepr<'registered> for VariableList<'registered, T>
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
        let mut cur_dynamic_off = unsafe { header_ptr.offset(self.dynamic_header_size() as isize) };
        let mut cur_dynamic_off_usize = offset + self.dynamic_header_offset();

        for elt in self.elts.iter() {
            if elt.dynamic_header_size() == 0 {
                ret.append(&mut elt.inner_serialize(cur_header_off, copy_func, 0));
                cur_header_off = unsafe { cur_header_off.offset(T::CONSTANT_HEADER_SIZE as isize) };
            } else {
                // write header offset to element header and recursively serialize
                let mut object_ref = ObjectRef(cur_header_off as _);
                object_ref.write_size(elt.dynamic_header_size());
                object_ref.write_offset(cur_dynamic_off_usize);
                ret.append(&mut elt.inner_serialize(
                    cur_dynamic_off,
                    copy_func,
                    cur_dynamic_off_usize,
                ));
                cur_header_off = unsafe { cur_header_off.offset(T::CONSTANT_HEADER_SIZE as isize) };
                cur_dynamic_off =
                    unsafe { cur_dynamic_off.offset(elt.dynamic_header_size() as isize) };
                cur_dynamic_off_usize += elt.dynamic_header_size();
            }
        }
        ret
    }

    // TODO: should offsets be written RELATIVE to the object that is being deserialized?
    fn inner_deserialize(&mut self, buf: *const u8, size: usize, relative_offset: usize) {
        let mut elts: Vec<T> = Vec::with_capacity(size);
        let mut cur_buf_ptr = buf;
        let mut total_constant_size_so_far = 0;
        // TODO: this entire thing is kind of sketchy. is there a cleaner way to do this?
        for _i in 0..size {
            // TODO: how do you know if it's a nested object or just a list of bytes?
            let mut elt = T::default();
            if elt.dynamic_header_size() == 0 {
                elt.inner_deserialize(
                    cur_buf_ptr,
                    T::CONSTANT_HEADER_SIZE,
                    relative_offset + total_constant_size_so_far,
                );
                elts.push(elt);
                total_constant_size_so_far += T::CONSTANT_HEADER_SIZE;
            } else {
                let object_ref = ObjectRef(cur_buf_ptr);
                let cur_dynamic_off = object_ref.get_offset() - relative_offset;
                let cur_dynamic_ptr = unsafe { buf.offset(cur_dynamic_off as isize) };
                let mut elt = T::default();
                elt.inner_deserialize(
                    cur_dynamic_ptr,
                    object_ref.get_size(),
                    object_ref.get_offset(),
                );
                elts.push(elt);
            }
            cur_buf_ptr = unsafe { cur_buf_ptr.offset(T::CONSTANT_HEADER_SIZE as isize) };
        }
        self.num_space = size;
        self.num_set = size;
        self.elts = elts;
        self._marker = PhantomData;
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

    pub fn get_offset_ptr(&self) -> *mut u8 {
        unsafe { self.0.offset(4) as *mut u8 }
    }
}
