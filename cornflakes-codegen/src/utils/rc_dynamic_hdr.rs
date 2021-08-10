use super::ObjectRef;
use bytes::{BufMut, BytesMut};
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
    ) -> Vec<(RcCornPtr<'a, D>, *mut u8)>;

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    /// TODO: for safety -- this should consume the received packet?
    fn inner_deserialize(&mut self, pkt: &ReceivedPkt<D>, relative_offset: usize);

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
    ) -> RcCornflake<'a, D> {
        let mut rc_cornflake = RcCornflake::new();
        // assert that the header buffer size is big enough
        assert!(header_buffer.len() >= self.dynamic_header_size());
        let header_ptr = header_buffer.as_mut_ptr();

        // get all the pointers, and fill in the header
        let mut rc_cornptrs = self.inner_serialize(header_ptr, copy_func, 0);

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

        rc_cornflake
    }

    /// Deserializes the packet into this object.
    /// Received packet could be a scatter-gather array.
    fn deserialize(&mut self, buf: ReceivedPkt<D>) {
        self.inner_deserialize(&buf, 0);
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
        offset: usize,
    ) -> Vec<(RcCornPtr<'a, D>, *mut u8)> {
        let mut obj_ref = ObjectRef(header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        vec![(RcCornPtr::RawRef(self.ptr), header_ptr)]
    }

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    fn inner_deserialize(&mut self, pkt: &ReceivedPkt<D>, relative_offset: usize) {
        let (header_seg, off_within_header_seg) = pkt.index_at_offset(relative_offset);
        let header_segment = pkt.index(header_seg);
        // TODO: have better error handling here -- error out if there is not if length in the
        // buffer
        assert!((header_segment.buf_size() - off_within_header_seg) >= (SIZE_FIELD + OFFSET_FIELD));
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
        assert!((payload_segment.buf_size() - off_within_payload_seg) >= payload_size);
        let payload_buf = unsafe {
            payload_segment
                .as_ref()
                .as_ptr()
                .offset(off_within_payload_seg as isize)
        };
        self.ptr = unsafe { slice::from_raw_parts(payload_buf, payload_size) };
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
        offset: usize,
    ) -> Vec<(RcCornPtr<'a, D>, *mut u8)> {
        let mut obj_ref = ObjectRef(header_ptr as _);
        obj_ref.write_size(self.ptr.len());
        vec![(RcCornPtr::RefCounted(self.ptr.clone()), header_ptr)]
    }

    /// Remember that the packet could be a scatter-gather array, so the offset into the buffer
    /// could show up in any of the entries of the scatter-gather array.
    fn inner_deserialize(&mut self, pkt: &ReceivedPkt<D>, relative_offset: usize) {
        /*let (header_seg, off_within_header_seg) = pkt.index_at_offset(relative_offset);
        let header_segment = pkt.index(header_seg);
        // TODO: have better error handling here -- error out if there is not if length in the
        // buffer
        assert!((header_segment.buf_size() - off_within_header_seg) >= (SIZE_FIELD + OFFSET_FIELD));
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
        assert!((payload_segment.buf_size() - off_within_payload_seg) >= payload_size);
        let payload_buf = unsafe {
            payload_segment
                .as_ref()
                .as_ptr()
                .offset(off_within_payload_seg as isize)
        };
        self.ptr = unsafe { slice::from_raw_parts(payload_buf, payload_size) };*/
    }
}
