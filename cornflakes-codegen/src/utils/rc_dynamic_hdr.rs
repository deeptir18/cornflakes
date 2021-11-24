use super::ObjectRef;
use bytes::{BufMut, BytesMut};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{
    CfBuf, CornType, Datapath, PtrAttributes, RcCornPtr, RcCornflake, ReceivedPkt, ScatterGather,
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
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
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

    /// Serializes cornflake into the given buffer.
    fn serialize_into_buffer(
        &self,
        buf: &mut [u8],
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Result<usize> {
        ensure!(
            buf.len() >= self.dynamic_header_size(),
            "Not enough space in buffer for entire object"
        );

        let cf = self.serialize_with_context(buf, copy_func)?;
        let mut buf_offset = self.dynamic_header_size();
        for i in 1..cf.num_segments() {
            let entry = cf.index(i);
            unsafe {
                copy_func(
                    buf.as_mut_ptr().offset(buf_offset as isize) as _,
                    entry.as_ref().as_ptr() as _,
                    entry.buf_size(),
                );
            }
            buf_offset += entry.buf_size();
        }

        Ok(buf_offset)
    }

    fn serialize(
        &self,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
    ) -> Result<(Vec<u8>, RcCornflake<'a, D>)> {
        let mut header_buffer_vec = self.init_header_buffer();
        let cf = self.serialize_with_context(header_buffer_vec.as_mut_slice(), copy_func)?;
        Ok((header_buffer_vec, cf))
    }

    fn serialize_with_context(
        &self,
        header_buffer: &mut [u8],
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

        // put in a filler buffer at the front
        rc_cornflake.add_entry(RcCornPtr::RawRef(&[]));

        // get all the pointers, and fill in the header
        let mut rc_cornptrs = self
            .inner_serialize(header_ptr, copy_func, 0)
            .wrap_err("Failed to call inner_serialize on top-level object.")?;
        tracing::debug!("# of cornptrs: {}", rc_cornptrs.len());

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
    fn deserialize(&mut self, pkt: &'a ReceivedPkt<D>, buffer_offset: usize) -> Result<()> {
        self.inner_deserialize(pkt, buffer_offset, 0, pkt.data_len())
            .wrap_err("Failed to call inner_deserialize on top level object")?;
        Ok(())
    }
}

pub enum CFString<'a, D>
where
    D: Datapath,
{
    Rc(CfBuf<D>),
    Raw(&'a [u8]),
}

impl<'a, D> std::cmp::PartialEq for CFString<'a, D>
where
    D: Datapath,
{
    fn eq(&self, other: &Self) -> bool {
        if let Some(rc_ref) = other.get_rc() {
            if let Some(our_rc_ref) = self.get_rc() {
                return rc_ref == our_rc_ref;
            }
        }
        if let Some(raw) = other.get_raw() {
            if let Some(our_raw) = self.get_raw() {
                return raw == our_raw;
            }
        }
        return false;
    }
}

impl<'a, D> std::fmt::Debug for CFString<'a, D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CFString::Rc(ptr) => write!(f, "Ref counted string: {:?}", ptr),
            CFString::Raw(ptr) => write!(f, "Raw string: {:?}", ptr),
        }
    }
}

impl<'a, D> std::cmp::Eq for CFString<'a, D> where D: Datapath {}

impl<'a, D> Clone for CFString<'a, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            CFString::Rc(cfbuf) => CFString::Rc(cfbuf.clone()),
            CFString::Raw(bytes) => CFString::Raw(bytes),
        }
    }
}

impl<'a, D> AsRef<[u8]> for CFString<'a, D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            CFString::Raw(ptr) => {
                return ptr;
            }
            CFString::Rc(ptr) => {
                return ptr.as_ref();
            }
        }
    }
}

impl<'a, D> CFString<'a, D>
where
    D: Datapath,
{
    pub fn new(ptr: &'a str) -> Self {
        CFString::Raw(ptr.as_bytes())
    }

    pub fn new_from_bytes(ptr: &'a [u8]) -> Self {
        CFString::Raw(ptr)
    }

    pub fn len(&self) -> usize {
        match self {
            CFString::Rc(ptr) => ptr.len(),
            CFString::Raw(buf) => buf.len(),
        }
    }

    pub fn to_str(&self) -> Result<&str> {
        match self {
            CFString::Raw(ptr) => str::from_utf8(ptr).wrap_err("Could not turn CFString into str"),
            CFString::Rc(ptr) => {
                let s =
                    str::from_utf8(ptr.as_ref()).wrap_err("Could not turn CFString into str")?;
                Ok(s)
            }
        }
    }

    /// Assumes that the string is utf8-encoded.
    pub fn to_string(&self) -> Result<String> {
        let s = self.to_str()?;
        Ok(s.to_string())
    }

    pub fn get_rc(&self) -> Option<CfBuf<D>> {
        match self {
            CFString::Rc(ptr) => Some(ptr.clone()),
            CFString::Raw(_) => None,
        }
    }

    pub fn get_raw(&self) -> Option<&'a [u8]> {
        match self {
            CFString::Rc(_) => None,
            CFString::Raw(buf) => Some(buf),
        }
    }

    pub fn get_inner(&self) -> Result<&'a [u8]> {
        match self {
            CFString::Raw(ptr) => Ok(ptr),
            CFString::Rc(_) => {
                bail!("Cannot return raw ref from Rc variant of CFString.")
            }
        }
    }
}

impl<'a, D> Default for CFString<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        CFString::Raw(&[])
    }
}

impl<'a, D> HeaderRepr<'a, D> for CFString<'a, D>
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
        match self {
            CFString::Rc(ptr) => {
                obj_ref.write_size(ptr.len());
                Ok(vec![(RcCornPtr::RefCounted(ptr.clone()), header_ptr)])
            }
            CFString::Raw(bytes) => {
                obj_ref.write_size(bytes.len());
                Ok(vec![(RcCornPtr::RawRef(bytes), header_ptr)])
            }
        }
    }

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    fn inner_deserialize(
        &mut self,
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        tracing::debug!(
            buffer_offset,
            relative_offset,
            size,
            "Deserializing cf bytes"
        );
        let header_slice = pkt.contiguous_slice(relative_offset + buffer_offset, size)?;
        let object_ref = ObjectRef(header_slice.as_ptr());
        let payload_offset = object_ref.get_offset() + buffer_offset;
        let payload_size = object_ref.get_size();

        let (payload_seg, off_within_payload_seg) = pkt.index_at_offset(payload_offset);
        tracing::debug!(
            "Payload seg: {}, off: {}",
            payload_seg,
            off_within_payload_seg
        );
        let payload_segment = pkt.index(payload_seg);
        ensure!(
            (payload_segment.buf_size() - off_within_payload_seg) >= payload_size,
            "Not enough space in payload segment for CfBuf payload."
        );
        // create reference to underlying DatapathPkt that increments the reference count
        let payload_buf =
            CfBuf::new_with_bounds(payload_segment, off_within_payload_seg, payload_size);

        // assume underlying packet is in REGISTERED MEMORY
        tracing::debug!(
            "Switching from raw ref to rc version of CFString for payload: {}",
            payload_size
        );
        *self = CFString::Rc(payload_buf);
        Ok(())
    }
}

/// Ref counted bytes buffer.
pub enum CFBytes<'a, D>
where
    D: Datapath,
{
    Rc(CfBuf<D>),
    Raw(&'a [u8]),
}

impl<'a, D> std::fmt::Debug for CFBytes<'a, D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CFBytes::Rc(ptr) => write!(f, "Ref counted: {:?}", ptr),
            CFBytes::Raw(ptr) => write!(f, "Raw: {:?}", ptr),
        }
    }
}

impl<'a, D> PartialEq for CFBytes<'a, D>
where
    D: Datapath,
{
    fn eq(&self, other: &Self) -> bool {
        if let Some(rc_ref) = other.get_rc() {
            if let Some(our_rc_ref) = self.get_rc() {
                return rc_ref == our_rc_ref;
            }
        }
        if let Some(raw) = other.get_raw() {
            if let Some(our_raw) = self.get_raw() {
                return raw == our_raw;
            }
        }
        return false;
    }
}

impl<'a, D> Eq for CFBytes<'a, D> where D: Datapath {}

impl<'a, D> Clone for CFBytes<'a, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            CFBytes::Rc(cfbuf) => CFBytes::Rc(cfbuf.clone()),
            CFBytes::Raw(bytes) => CFBytes::Raw(bytes),
        }
    }
}

impl<'a, D> CFBytes<'a, D>
where
    D: Datapath,
{
    pub fn new_rc(ptr: CfBuf<D>) -> Self {
        CFBytes::Rc(ptr)
    }

    pub fn new_raw(buf: &'a [u8]) -> Self {
        CFBytes::Raw(buf)
    }

    pub fn len(&self) -> usize {
        match self {
            CFBytes::Rc(ptr) => ptr.len(),
            CFBytes::Raw(buf) => buf.len(),
        }
    }

    pub fn get_rc(&self) -> Option<CfBuf<D>> {
        match self {
            CFBytes::Rc(ptr) => Some(ptr.clone()),
            CFBytes::Raw(_) => None,
        }
    }

    pub fn get_raw(&self) -> Option<&'a [u8]> {
        match self {
            CFBytes::Rc(_) => None,
            CFBytes::Raw(buf) => Some(buf),
        }
    }

    pub fn get_inner(&self) -> Result<&'a [u8]> {
        match self {
            CFBytes::Raw(ptr) => Ok(ptr),
            CFBytes::Rc(_) => {
                bail!("Cannot return raw ref from Rc variant of CFBytes.")
            }
        }
    }

    pub fn get_inner_rc(&self) -> Result<CfBuf<D>> {
        match self {
            CFBytes::Rc(ptr) => Ok(ptr.clone()),
            CFBytes::Raw(_) => {
                bail!("Cannot return Rc variant from raw buf variant of CFBytes.")
            }
        }
    }
    pub fn to_bytes_vec(&self) -> Vec<u8> {
        match self {
            CFBytes::Rc(ptr) => {
                let mut vec: Vec<u8> = Vec::with_capacity(ptr.len());
                vec.extend_from_slice(ptr.as_ref());
                vec
            }
            CFBytes::Raw(buf) => {
                let mut vec: Vec<u8> = Vec::with_capacity(buf.len());
                vec.extend_from_slice(buf);
                vec
            }
        }
    }
}

impl<'a, D> Default for CFBytes<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        CFBytes::Raw(&[])
    }
}

impl<'a, D> HeaderRepr<'a, D> for CFBytes<'a, D>
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
        match self {
            CFBytes::Rc(ptr) => {
                obj_ref.write_size(ptr.len());
                Ok(vec![(RcCornPtr::RefCounted(ptr.clone()), header_ptr)])
            }
            CFBytes::Raw(bytes) => {
                obj_ref.write_size(bytes.len());
                Ok(vec![(RcCornPtr::RawRef(bytes), header_ptr)])
            }
        }
    }

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    fn inner_deserialize(
        &mut self,
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        tracing::debug!(
            buffer_offset,
            relative_offset,
            size,
            "Deserializing cf bytes"
        );
        let header_slice = pkt.contiguous_slice(relative_offset + buffer_offset, size)?;
        let object_ref = ObjectRef(header_slice.as_ptr());
        let payload_offset = object_ref.get_offset() + buffer_offset;
        let payload_size = object_ref.get_size();

        let (payload_seg, off_within_payload_seg) = pkt.index_at_offset(payload_offset);
        tracing::debug!(
            "Payload seg: {}, off: {}",
            payload_seg,
            off_within_payload_seg
        );
        let payload_segment = pkt.index(payload_seg);
        ensure!(
            (payload_segment.buf_size() - off_within_payload_seg) >= payload_size,
            "Not enough space in payload segment for CfBuf payload."
        );
        // create reference to underlying DatapathPkt that increments the reference count
        let payload_buf =
            CfBuf::new_with_bounds(payload_segment, off_within_payload_seg, payload_size);

        // assume underlying packet is in REGISTERED MEMORY
        tracing::debug!(
            "Switching from raw ref to rc version of CFBytes for payload: {}",
            payload_size
        );
        *self = CFBytes::Rc(payload_buf);
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
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        match self {
            List::Owned(owned_list) => {
                owned_list.inner_deserialize(pkt, buffer_offset, relative_offset, size)
            }
            List::Ref(ref_list) => {
                ref_list.inner_deserialize(pkt, buffer_offset, relative_offset, size)
            }
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
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        self.num_set = size;
        self.num_space = size;
        self.list_ptr = BytesMut::with_capacity(size * size_of::<T>());
        let payload_slice =
            pkt.contiguous_slice(relative_offset + buffer_offset, size * size_of::<T>())?;
        self.list_ptr.chunk_mut().copy_from_slice(payload_slice);

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
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        self.num_space = size;
        let payload_slice =
            pkt.contiguous_slice(relative_offset + buffer_offset, size * (size_of::<T>()))?;
        self.list_ptr = payload_slice.as_ptr() as _;
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
        pkt: &'a ReceivedPkt<D>,
        buffer_offset: usize,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        tracing::debug!(
            buffer_offset = buffer_offset,
            relative_offset = relative_offset,
            size = size,
            "Inner deserialize for Variable List"
        );
        let mut elts: Vec<T> = Vec::with_capacity(size);
        let mut cur_header_offset = relative_offset;

        // TODO: this entire thing is kind of sketchy. is there a cleaner way to do this?
        for _i in 0..size {
            let mut elt = T::default();
            if elt.dynamic_header_size() == 0 {
                tracing::debug!("Calling inner serialize with off {}", cur_header_offset);
                elt.inner_deserialize(
                    pkt,
                    buffer_offset,
                    cur_header_offset,
                    T::CONSTANT_HEADER_SIZE,
                )?;
                elts.push(elt);
            } else {
                let header_slice = pkt
                    .contiguous_slice(cur_header_offset + buffer_offset, T::CONSTANT_HEADER_SIZE)?;
                let object_ref = ObjectRef(header_slice.as_ptr());

                let mut elt = T::default();
                elt.inner_deserialize(
                    pkt,
                    buffer_offset,
                    object_ref.get_offset(),
                    object_ref.get_size(),
                )?;
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
