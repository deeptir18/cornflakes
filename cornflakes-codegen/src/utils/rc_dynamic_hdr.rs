use super::ObjectRef;
use bytes::{BufMut, BytesMut};
use color_eyre::eyre::{ensure, Result, WrapErr};
use cornflakes_libos::{
    CfBuf, CornType, Datapath, PtrAttributes, RcCornPtr, RcCornflake, ReceivedPkt,
};
use std::{
    cmp::Ordering, default::Default, fmt::Debug, marker::PhantomData, mem::size_of, ops::Index,
    ptr, slice, str,
};

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;

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

pub trait HeaderRepr<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize;

    fn dynamic_header_size(&self) -> usize;

    fn dynamic_header_offset(&self) -> usize;

    fn total_header_size(&self) -> usize {
        self.dynamic_header_size() + <Self as HeaderRepr<'a, D>>::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>>;

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    /// TODO: for safety -- this should consume the received packet?
    /// Parameters:
    ///     * pkt -> Incoming packet to deserialize. Could potentially be a scatter-gather array.
    ///     * relative_offset -> Offset into the packet (as if it were a flat array) where object
    ///     is.
    ///     * size -> In the case of a fixed-sized list, the number of elements to deserialize.
    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()>;

    /// Initializes a header _buffer large enough to store the entire serialization header.
    fn init_header_buffer(&self) -> Vec<u8> {
        vec![0u8; self.dynamic_header_size()]
    }

    fn init_header_buffer_with_padding(&self) -> Vec<u8> {
        vec![0u8; align_up(self.dynamic_header_size(), 64)]
    }

    fn serialize(
        &self,
        header_buffer: &'a mut [u8],
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Result<RcCornflake<'a, D>> {
        let mut rc_cornflake = RcCornflake::new();
        // assert that the header buffer size is big enough
        assert!(header_buffer.len() >= self.dynamic_header_size());
        let header_ptr = header_buffer.as_mut_ptr();

        // get all the pointers, and fill in the header
        let mut rc_cornptrs = self
            .inner_serialize(header_ptr, copy_func, 0)
            .wrap_err("Failed to call inner_serialize on top-level object.")?;

        // sorting heuristic
        // if both are in normal memory, they anyway need to be copied
        // if both are in registered memory, compare sizes
        // if one is normal and one is in registered, register one should go later
        // TODO/design goal: how do we make it such that the the serialization library does not
        // necessarily know if a certain memory is registered or not?
        rc_cornptrs.sort_by(|a, b| match (a.0.buf_type(), b.0.buf_type()) {
            (CornType::Normal, CornType::Normal) => Ordering::Equal,
            (CornType::Normal, CornType::Registered) => Ordering::Less,
            (CornType::Registered, CornType::Normal) => Ordering::Greater,
            (CornType::Registered, CornType::Registered) => {
                a.0.buf_size().partial_cmp(&b.0.buf_size()).unwrap()
            }
        });

        let mut cur_offset = self.dynamic_header_size();
        for (rc_cornptr, offset) in rc_cornptrs.into_iter() {
            let mut obj_ref = ObjectRef(offset as *const u8);
            obj_ref.write_offset(cur_offset);
            rc_cornflake.add_entry(rc_cornptr);
            cur_offset += obj_ref.get_size() as usize;
        }

        Ok(rc_cornflake)
    }

    /// Deserializes the packet into this object.
    /// Received packet could be a scatter-gather array.
    fn deserialize(&mut self, buf: ReceivedPkt<D>) -> Result<()> {
        self.inner_deserialize(&buf, 0, buf.data_len())
            .wrap_err("Failed to call inner_deserialize on top level object")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct CFString<'a> {
    pub ptr: &'a [u8],
}

impl<'a> CFString<'a> {
    pub fn new(ptr: &'a str) -> Self {
        CFString {
            ptr: ptr.as_bytes(),
        }
    }

    pub fn new_from_bytes(ptr: &'a [u8]) -> Self {
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

impl<'a> Default for CFString<'a> {
    fn default() -> Self {
        CFString {
            ptr: Default::default(),
        }
    }
}

impl<'a, D> HeaderRepr<'a, D> for CFString<'a>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        _copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut obj_ref = ObjectRef(header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        Ok(vec![(RcCornPtr::RawRef(self.ptr), header_ptr)])
    }

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        _size: usize,
    ) -> Result<()> {
        let (header_seg, off_within_header_seg) = pkt.index_at_offset(relative_offset);
        let header_segment = pkt.index(header_seg);
        // TODO: have better error handling here -- error out if there is not if length in the
        // buffer

        ensure!(
            (header_segment.buf_size() - off_within_header_seg) >= (SIZE_FIELD + OFFSET_FIELD),
            "Not enough space in header segment for CfString header info."
        );
        let header_buf = unsafe {
            header_segment
                .as_ref()
                .as_ptr()
                .offset(off_within_header_seg as isize)
        };
        let object_ref = ObjectRef(header_buf);
        let payload_offset = object_ref.get_offset();
        let payload_size = object_ref.get_size();

        let (payload_seg, off_within_payload_seg) = pkt.index_at_offset(payload_offset);
        let payload_segment = pkt.index(payload_seg);
        ensure!(
            (payload_segment.buf_size() - off_within_payload_seg) >= payload_size,
            "Not enough space in payload segment for CfString payload."
        );
        let payload_buf = unsafe {
            payload_segment
                .as_ref()
                .as_ptr()
                .offset(off_within_payload_seg as isize)
        };
        self.ptr = unsafe { slice::from_raw_parts(payload_buf, payload_size) };
        Ok(())
    }
}

/// Ref counted bytes buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RcBytes<D>
where
    D: Datapath,
{
    pub ptr: CfBuf<D>,
}

impl<D> RcBytes<D>
where
    D: Datapath,
{
    pub fn new(ptr: CfBuf<D>) -> Self {
        RcBytes { ptr: ptr }
    }

    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    pub fn to_bytes_vec(&self) -> Vec<u8> {
        let mut vec: Vec<u8> = Vec::with_capacity(self.ptr.len());
        vec.extend_from_slice(self.ptr.as_ref());
        vec
    }
}

impl<D> Default for RcBytes<D>
where
    D: Datapath,
{
    fn default() -> Self {
        RcBytes {
            ptr: CfBuf::default(),
        }
    }
}

impl<'a, D> HeaderRepr<'a, D> for RcBytes<D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        _copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut obj_ref = ObjectRef(header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        Ok(vec![(RcCornPtr::RefCounted(self.ptr.clone()), header_ptr)])
    }

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        _size: usize,
    ) -> Result<()> {
        let (header_seg, off_within_header_seg) = pkt.index_at_offset(relative_offset);
        let header_segment = pkt.index(header_seg);
        ensure!(
            (header_segment.buf_size() - off_within_header_seg) >= (SIZE_FIELD + OFFSET_FIELD),
            "Not enough space in header segment for RcBytes header info."
        );
        let header_buf = unsafe {
            header_segment
                .as_ref()
                .as_ptr()
                .offset(off_within_header_seg as isize)
        };
        let object_ref = ObjectRef(header_buf);
        let payload_offset = object_ref.get_offset();
        let payload_size = object_ref.get_size();

        let (payload_seg, off_within_payload_seg) = pkt.index_at_offset(payload_offset);
        let payload_segment = pkt.index(payload_seg);
        ensure!(
            (payload_segment.buf_size() - off_within_payload_seg) >= payload_size,
            "Not enough space in payload segment for RcBytes payload."
        );
        // create reference to underlying DatapathPkt that increments the reference count
        let payload_buf =
            CfBuf::new_with_bounds(payload_segment, off_within_payload_seg, payload_size);
        self.ptr = payload_buf;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    Owned(OwnedList<'a, T, D>),
    Ref(RefList<'a, T, D>),
}

impl<'a, T, D> Default for List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    fn default() -> Self {
        List::Owned(OwnedList::default())
    }
}

impl<'a, T, D> Index<usize> for List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &T {
        match self {
            List::Owned(owned_list) => owned_list.index(idx),
            List::Ref(ref_list) => ref_list.index(idx),
        }
    }
}

impl<'a, T, D> List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    pub fn init(size: usize) -> List<'a, T, D> {
        List::Owned(OwnedList::init(size))
    }

    pub fn init_ref() -> List<'a, T, D> {
        List::Ref(RefList::default())
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
impl<'a, T, D> HeaderRepr<'a, D> for List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        match self {
            List::Owned(owned_list) => owned_list.dynamic_header_size(),
            List::Ref(ref_list) => ref_list.dynamic_header_size(),
        }
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        match self {
            List::Owned(owned_list) => owned_list.inner_serialize(header_ptr, copy_func, offset),
            List::Ref(ref_list) => ref_list.inner_serialize(header_ptr, copy_func, offset),
        }
    }

    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        match self {
            List::Owned(owned_list) => owned_list.inner_deserialize(pkt, relative_offset, size),
            List::Ref(ref_list) => ref_list.inner_deserialize(pkt, relative_offset, size),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    num_space: usize,
    num_set: usize,
    list_ptr: BytesMut,
    _marker: PhantomData<&'a [(T, D)]>,
}

impl<'a, T, D> Default for OwnedList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    fn default() -> Self {
        OwnedList {
            num_space: 0,
            num_set: 0,
            list_ptr: BytesMut::default(),
            _marker: PhantomData,
        }
    }
}

impl<'a, T, D> OwnedList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    pub fn init(size: usize) -> OwnedList<'a, T, D> {
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

impl<'a, T, D> Index<usize> for OwnedList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'a, T, D> HeaderRepr<'a, D> for OwnedList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.num_set * size_of::<T>()
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        unsafe {
            copy_func(
                header_ptr as _,
                self.list_ptr.as_ptr() as _,
                self.num_set * size_of::<T>(),
            );
        }
        Ok(Vec::default())
    }

    // Re-initializing owned list is not 0-copy. 0-copy only works for ref-list.
    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        self.num_set = size;
        self.num_space = size;
        self.list_ptr = BytesMut::with_capacity(size * size_of::<T>());
        // copy the bytes from the buf to the list_ptr
        let (payload_seg, payload_off) = pkt.index_at_offset(relative_offset);
        let payload_segment = pkt.index(payload_seg);

        // For now, assume that the list must be contiguous within a segment
        ensure!((payload_segment.buf_size() - payload_off) >= size * (size_of::<T>()), format!("Not enough space in payload segment {} with length {} (at offset {}), for {:?} list of length {}.", payload_seg, payload_segment.buf_size(), payload_off, std::any::type_name::<T>(), size));

        let payload_ptr = unsafe {
            payload_segment
                .as_ref()
                .as_ptr()
                .offset(payload_off as isize)
        };
        self.list_ptr
            .chunk_mut()
            .copy_from_slice(unsafe { slice::from_raw_parts(payload_ptr, size * size_of::<T>()) });

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    num_space: usize,
    list_ptr: *mut u8,
    _marker: PhantomData<&'a [(T, D)]>,
}

impl<'a, T, D> Default for RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    fn default() -> Self {
        RefList {
            num_space: 0,
            list_ptr: ptr::null_mut(),
            _marker: PhantomData,
        }
    }
}

impl<'a, T, D> RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    pub fn replace(&mut self, idx: usize, val: T) {
        assert!(idx < self.num_space);
        self.write_val(idx, val);
    }

    pub fn len(&self) -> usize {
        self.num_space
    }

    fn write_val(&mut self, val_idx: usize, val: T) {
        let offset = unsafe { (self.list_ptr as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    fn read_val(&self, val_idx: usize) -> &T {
        let offset = unsafe { (self.list_ptr as *const T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts(offset, 1) };
        &t_slice[0]
    }
}

impl<'a, T, D> Index<usize> for RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'a, T, D> HeaderRepr<'a, D> for RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.num_space * size_of::<T>()
    }

    fn dynamic_header_offset(&self) -> usize {
        0
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        unsafe {
            copy_func(
                header_ptr as _,
                self.list_ptr as _,
                self.num_space * size_of::<T>(),
            );
        }
        Ok(Vec::default())
    }

    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        self.num_space = size;
        let (payload_seg, payload_off) = pkt.index_at_offset(relative_offset);
        // payload must be contiguous in the packet: check there's enough space
        // For now, assume that the list must be contiguous within a segment
        let payload_segment = pkt.index(payload_seg);
        ensure!((payload_segment.buf_size() - payload_off) >= size * (size_of::<T>()), format!("Not enough space in payload segment {} with length {} (at offset {}), for {:?} list of length {}.", payload_seg, payload_segment.buf_size(), payload_off, std::any::type_name::<T>(), size));

        let buf = unsafe {
            payload_segment
                .as_ref()
                .as_ptr()
                .offset(payload_off as isize)
        };
        self.list_ptr = buf as _;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct VariableList<'a, T, D>
where
    T: HeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    _marker: PhantomData<(&'a [u8], D)>,
}

impl<'a, T, D> Default for VariableList<'a, T, D>
where
    T: HeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    fn default() -> Self {
        VariableList {
            num_space: 0,
            num_set: 0,
            elts: Vec::default(),
            _marker: PhantomData,
        }
    }
}
impl<'a, T, D> VariableList<'a, T, D>
where
    T: HeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    pub fn init(num: usize) -> VariableList<'a, T, D> {
        VariableList {
            num_space: num,
            num_set: 0,
            elts: Vec::with_capacity(num),
            _marker: PhantomData,
        }
    }

    pub fn append(&mut self, val: T) {
        assert!(self.num_set < self.num_space);
        tracing::debug!("Appending to the list");
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
}
impl<'a, T, D> Index<usize> for VariableList<'a, T, D>
where
    T: HeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        assert!(idx < self.num_space);
        &self.elts[idx]
    }
}

impl<'a, T, D> HeaderRepr<'a, D> for VariableList<'a, T, D>
where
    T: HeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.elts
            .iter()
            .map(|x| x.dynamic_header_size() + T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    fn dynamic_header_offset(&self) -> usize {
        self.elts.iter().map(|_x| T::CONSTANT_HEADER_SIZE).sum()
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        let mut cur_header_off = header_ptr;
        let mut cur_dynamic_off =
            unsafe { header_ptr.offset(self.dynamic_header_offset() as isize) };
        let mut cur_dynamic_off_usize = offset + self.dynamic_header_offset();

        tracing::debug!(num_elts = self.elts.len(), "Info about list items");
        for elt in self.elts.iter() {
            if elt.dynamic_header_size() == 0 {
                ret.append(&mut elt.inner_serialize(cur_header_off, copy_func, 0)?);
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
                )?);
                cur_header_off = unsafe { cur_header_off.offset(T::CONSTANT_HEADER_SIZE as isize) };
                cur_dynamic_off =
                    unsafe { cur_dynamic_off.offset(elt.dynamic_header_size() as isize) };
                cur_dynamic_off_usize += elt.dynamic_header_size();
            }
        }
        Ok(ret)
    }

    fn inner_deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        let mut elts: Vec<T> = Vec::with_capacity(size);
        let mut cur_header_offset = relative_offset;

        // TODO: this entire thing is kind of sketchy. is there a cleaner way to do this?
        for _i in 0..size {
            let mut elt = T::default();
            if elt.dynamic_header_size() == 0 {
                elt.inner_deserialize(pkt, T::CONSTANT_HEADER_SIZE, cur_header_offset)?;
                elts.push(elt);
            } else {
                let (header_seg, header_off) = pkt.index_at_offset(cur_header_offset);
                let header_segment = pkt.index(header_seg);
                ensure!(
                    (header_segment.buf_size() - header_off) >= T::CONSTANT_HEADER_SIZE,
                    format!("Not enough space in header segment {} with length {} (at off {}) for constant header portion of Type {}, sized {}", header_seg, header_segment.buf_size(), header_off, std::any::type_name::<T>(), T::CONSTANT_HEADER_SIZE)
                );

                let cur_header_ptr =
                    unsafe { header_segment.as_ref().as_ptr().offset(header_off as isize) };
                let object_ref = ObjectRef(cur_header_ptr);

                let (payload_seg, payload_off) = pkt.index_at_offset(object_ref.get_offset());
                let payload_segment = pkt.index(payload_seg);
                ensure!(
                    (payload_segment.buf_size() - payload_off) >= object_ref.get_size(),
                    format!("Not enough space in payload segment {} with length {} (at off {}) for dynamic object {}, sized {}", payload_seg, payload_segment.buf_size(), payload_off, std::any::type_name::<T>(), T::CONSTANT_HEADER_SIZE)
                );

                let mut elt = T::default();
                elt.inner_deserialize(pkt, object_ref.get_size(), object_ref.get_offset())?;
                elts.push(elt);
            }
            cur_header_offset += T::CONSTANT_HEADER_SIZE;
        }
        self.num_space = size;
        self.num_set = size;
        self.elts = elts;
        self._marker = PhantomData;
        Ok(())
    }
}
