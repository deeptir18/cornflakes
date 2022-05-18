use super::{align_up, ForwardPointer, MutForwardPointer};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    RcSga, RcSge,
};
use std::{
    convert::TryInto, default::Default, fmt::Debug, marker::PhantomData, mem::size_of, ops::Index,
    slice, str,
};

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;
/// u32 at beginning representing bitmap size in bytes
pub const BITMAP_LENGTH_FIELD: usize = 4;

pub trait RcSgaHeaderRepr<'a, D>
where
    D: Datapath,
{
    /// Maximum number of fields is max u32 * 8
    const NUMBER_OF_FIELDS: usize;

    /// Constant part of the header: containing constant sized fields and pointers to variable
    /// sized fields. Does not include the bitmap.
    const CONSTANT_HEADER_SIZE: usize;

    /// Number of bits required to represent the bitmap.
    /// Aligned to multiple of 4.
    fn bitmap_length() -> usize {
        align_up(Self::NUMBER_OF_FIELDS, 4)
    }

    fn get_bitmap_field(&self, field: usize) -> bool {
        self.get_bitmap()[field] == 1
    }

    fn set_bitmap_field(&mut self, field: usize) {
        self.get_mut_bitmap()[field] = 1;
    }

    fn clear_bitmap(&mut self) {
        for field in self
            .get_mut_bitmap()
            .iter_mut()
            .take(Self::NUMBER_OF_FIELDS)
        {
            *field = 0;
        }
    }

    fn get_bitmap(&self) -> &[u8];

    fn get_mut_bitmap(&mut self) -> &mut [u8];

    fn set_bitmap(&mut self, bitmap: &[u8]);

    fn serialize_bitmap(&self, header: &mut [u8], offset: usize) {
        LittleEndian::write_u32(
            &mut header[offset..(offset + BITMAP_LENGTH_FIELD)],
            Self::bitmap_length() as u32,
        );
        let slice = &mut header[(offset + BITMAP_LENGTH_FIELD)
            ..(offset + BITMAP_LENGTH_FIELD + Self::bitmap_length())];
        slice.copy_from_slice(&self.get_bitmap());
    }

    fn deserialize_bitmap(&mut self, header: &RcSge<'a, D>, offset: usize) {
        let bitmap_size =
            LittleEndian::read_u32(&header.as_ref()[offset..(offset + BITMAP_LENGTH_FIELD)]);
        self.set_bitmap(
            &header.as_ref()[(offset + BITMAP_LENGTH_FIELD)
                ..(offset + BITMAP_LENGTH_FIELD + (bitmap_size as usize))],
        );
    }

    /// Dynamic part of the header (actual bytes pointed to, lists, nested objects).
    fn dynamic_header_size(&self) -> usize;

    /// Total header size including the bitmap.
    fn total_header_size(&self, with_ref: bool) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + <Self as RcSgaHeaderRepr<'a, D>>::CONSTANT_HEADER_SIZE * with_ref as usize
            + self.dynamic_header_size()
    }

    /// Number of scatter-gather entries (pointers and nested pointers to variable bytes or string
    /// fields).
    fn num_scatter_gather_entries(&self) -> usize;

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize;

    /// Allocates context required for serialization: header vector and scatter-gather array with
    /// required capacity.
    fn alloc_context(&self) -> (Vec<u8>, RcSga<'a, D>) {
        (
            vec![0u8; self.total_header_size(false)],
            RcSga::allocate(self.num_scatter_gather_entries() + 1),
        )
    }

    fn inner_serialize_with_ref(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'a, D>],
        offsets: &mut [usize],
        with_ref: bool,
    ) -> Result<()> {
        if with_ref {
            // read forward pointer
            let slice = &mut header
                [constant_header_offset..(constant_header_offset + Self::CONSTANT_HEADER_SIZE)];
            let mut forward_pointer = MutForwardPointer(slice);
            forward_pointer.write_offset(dynamic_header_start as _);
            // TODO: write size?
            self.inner_serialize(
                header,
                dynamic_header_start,
                dynamic_header_start + self.dynamic_header_start(),
                scatter_gather_entries,
                offsets,
            )
        } else {
            self.inner_serialize(
                header,
                constant_header_offset,
                dynamic_header_start,
                scatter_gather_entries,
                offsets,
            )
        }
    }

    /// Nested serialization function.
    /// Params:
    /// @header - mutable header bytes to write header.
    /// @constant_header_offset - offset into array to start writing constant part of header.
    /// @dynamic_header_start - offset into array to start writing dynamic parts of header (list,
    /// nested object)
    /// @scatter_gather_entries - mutable slice of (Sge) entries. to write in scatter-gather
    /// entry data
    /// @offsets - corresponding mutable array of offsets where future pointer/size of this scatter-gather
    /// entry should be written.
    /// Assumes header has enough space for bytes and scatter_gather_entries has enough space for
    /// @offsets and @scatter_gather_entries should be the same size.
    /// nested entries.
    fn inner_serialize(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'a, D>],
        offsets: &mut [usize],
    ) -> Result<()>;

    fn inner_deserialize_with_ref(
        &mut self,
        buffer: &RcSge<'a, D>,
        header_offset: usize,
        with_ref: bool,
    ) -> Result<()> {
        if with_ref {
            let slice = &buffer.as_ref()
                [header_offset..(header_offset + Self::CONSTANT_HEADER_SIZE)]
                .try_into()?;
            let forward_pointer = ForwardPointer(slice);
            self.inner_deserialize(buffer, forward_pointer.get_offset() as usize)
        } else {
            self.inner_deserialize(buffer, header_offset)
        }
    }

    /// Nested deserialization function.
    /// Params:
    /// @buffer - Buffer to deserialize.
    /// @header_offset - Offset into constant part of header for nested deserialization.
    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, header_offset: usize) -> Result<()>;

    /// Serialize with context of existing ordered sga and existing header buffer.
    fn serialize_into_sga(
        &self,
        header_buffer: &'a mut [u8],
        sga: &'a mut RcSga<'a, D>,
        datapath: &D,
    ) -> Result<()>
    where
        D: Datapath,
    {
        let required_entries = self.num_scatter_gather_entries();
        let header_size = self.total_header_size(false);

        if sga.len() < (required_entries + 1) || header_buffer.len() < header_size {
            bail!("Cannot serialize into sga with num entries {} ({} required) and header buffer length {} ({} required", sga.len(), required_entries + 1, header_buffer.len(), header_size);
        }

        sga.set_length(required_entries);
        let mut offsets: Vec<usize> = vec![0; required_entries];

        // recursive serialize each item
        self.inner_serialize_with_ref(
            header_buffer,
            0,
            self.dynamic_header_start(),
            sga.mut_entries_slice(1, required_entries),
            offsets.as_mut_slice(),
            false,
        )?;

        // reorder entries according to size threshold and whether entries are registered.
        sga.reorder_by_size_and_registration(datapath, &mut offsets)?;
        // reorder entries if current (zero-copy segments + 1) exceeds max zero-copy segments
        sga.reorder_by_max_segs(datapath, &mut offsets)?;

        let mut cur_dynamic_offset = self.dynamic_header_size();

        // iterate over header, writing in forward pointers based on new ordering
        for (rcsge, offset) in sga
            .entries_slice(1, required_entries)
            .iter()
            .zip(offsets.into_iter())
        {
            let mut obj_ref = MutForwardPointer(&mut header_buffer[offset..(offset + 8)]);
            obj_ref.write_size(rcsge.len() as u32);
            obj_ref.write_offset(cur_dynamic_offset as u32);
            cur_dynamic_offset += rcsge.len();
        }

        // replace head entry with object header buffer
        sga.replace(1, RcSge::RawRef(&header_buffer[0..header_size]));
        Ok(())
    }

    /// Deserialize contiguous registered buffer into this object.
    fn deserialize_raw_buf(&mut self, packet: &'a [u8]) -> Result<()> {
        let rc_sge = RcSge::RawRef(packet);
        self.inner_deserialize(&rc_sge, 0)?;
        Ok(())
    }
    /// Deserialize contiguous buffer into this object.
    fn deserialize(&mut self, packet: &'a ReceivedPkt<D>) -> Result<()> {
        let rc_sge = RcSge::RefCounted(packet.seg(0).clone());
        self.inner_deserialize(&rc_sge, 0)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CFString<'a, D>
where
    D: Datapath,
{
    ptr: RcSge<'a, D>,
}

impl<'a, D> CFString<'a, D>
where
    D: Datapath,
{
    pub fn new(ptr: &'a str) -> Self {
        CFString {
            ptr: RcSge::RawRef(ptr.as_bytes()),
        }
    }

    pub fn new_from_bytes(ptr: &'a [u8]) -> Self {
        CFString {
            ptr: RcSge::RawRef(ptr),
        }
    }

    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    pub fn to_str(&self) -> Result<&str> {
        let slice = self.ptr.as_ref();
        let s = str::from_utf8(slice).wrap_err("Could not turn bytes into string")?;
        Ok(s)
    }

    /// Assumes that the string is utf8-encoded.
    pub fn to_string(&self) -> Result<String> {
        let s = self.to_str()?;
        Ok(s.to_string())
    }
}

impl<'a, D> RcSgaHeaderRepr<'a, D> for CFString<'a, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn num_scatter_gather_entries(&self) -> usize {
        1
    }

    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn get_bitmap(&self) -> &[u8] {
        &[]
    }

    fn get_mut_bitmap(&mut self) -> &mut [u8] {
        &mut []
    }

    fn set_bitmap(&mut self, _bitmap: &[u8]) {}

    fn inner_serialize(
        &self,
        _header: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'a, D>],
        offsets: &mut [usize],
    ) -> Result<()> {
        scatter_gather_entries[0] = self.ptr.clone();
        offsets[0] = constant_header_offset;
        Ok(())
    }

    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, header_offset: usize) -> Result<()> {
        let slice = &buffer.as_ref()[header_offset..(header_offset + Self::CONSTANT_HEADER_SIZE)]
            .try_into()?;
        let forward_pointer = ForwardPointer(slice);
        let off = forward_pointer.get_offset() as usize;
        let size = forward_pointer.get_size() as usize;
        self.ptr = buffer.clone_with_bounds(off, size)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CFBytes<'a, D>
where
    D: Datapath,
{
    ptr: RcSge<'a, D>,
}

impl<'a, D> CFBytes<'a, D>
where
    D: Datapath,
{
    pub fn new(ptr: &'a str) -> Self {
        CFBytes {
            ptr: RcSge::RawRef(ptr.as_bytes()),
        }
    }

    pub fn new_from_bytes(ptr: &'a [u8]) -> Self {
        CFBytes {
            ptr: RcSge::RawRef(ptr),
        }
    }

    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    pub fn get(&self) -> &[u8] {
        self.ptr.as_ref()
    }
}

impl<'a, D> RcSgaHeaderRepr<'a, D> for CFBytes<'a, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn num_scatter_gather_entries(&self) -> usize {
        1
    }

    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn get_bitmap(&self) -> &[u8] {
        &[]
    }

    fn get_mut_bitmap(&mut self) -> &mut [u8] {
        &mut []
    }

    fn set_bitmap(&mut self, _bitmap: &[u8]) {}

    fn inner_serialize(
        &self,
        _header: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'a, D>],
        offsets: &mut [usize],
    ) -> Result<()> {
        scatter_gather_entries[0] = self.ptr.clone();
        offsets[0] = constant_header_offset;
        Ok(())
    }

    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, header_offset: usize) -> Result<()> {
        let slice = &buffer.as_ref()[header_offset..(header_offset + Self::CONSTANT_HEADER_SIZE)]
            .try_into()?;
        let forward_pointer = ForwardPointer(slice);
        let off = forward_pointer.get_offset() as usize;
        let size = forward_pointer.get_size() as usize;
        self.ptr = buffer.clone_with_bounds(off, size)?;
        Ok(())
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    Owned(OwnedList<'a, T>),
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

impl<'a, T, D> RcSgaHeaderRepr<'a, D> for List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn num_scatter_gather_entries(&self) -> usize {
        0
    }

    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn get_bitmap(&self) -> &[u8] {
        &[]
    }

    fn get_mut_bitmap(&mut self) -> &mut [u8] {
        &mut []
    }

    fn set_bitmap(&mut self, _bitmap: &[u8]) {}

    fn inner_serialize(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'a, D>],
        offsets: &mut [usize],
    ) -> Result<()> {
        match self {
            List::Owned(l) => l.inner_serialize(
                header,
                constant_header_offset,
                dynamic_header_start,
                scatter_gather_entries,
                offsets,
            ),
            List::Ref(l) => l.inner_serialize(
                header,
                constant_header_offset,
                dynamic_header_start,
                scatter_gather_entries,
                offsets,
            ),
        }
    }

    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, header_offset: usize) -> Result<()> {
        match self {
            List::Owned(l) => l.inner_deserialize(buffer, header_offset),
            List::Ref(l) => l.inner_deserialize(buffer, header_offset),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OwnedList<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    num_space: usize,
    num_set: usize,
    list_ptr: BytesMut,
    _marker: PhantomData<&'a [T]>,
}

impl<'a, T> OwnedList<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    pub fn init(size: usize) -> OwnedList<'a, T> {
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

impl<'a, T> Index<usize> for OwnedList<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'a, T, D> RcSgaHeaderRepr<'a, D> for OwnedList<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.num_set * size_of::<T>()
    }

    fn num_scatter_gather_entries(&self) -> usize {
        0
    }

    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn get_bitmap(&self) -> &[u8] {
        &[]
    }

    fn get_mut_bitmap(&mut self) -> &mut [u8] {
        &mut []
    }

    fn set_bitmap(&mut self, _bitmap: &[u8]) {}

    fn inner_serialize(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        _scatter_gather_entries: &mut [RcSge<'a, D>],
        _offsets: &mut [usize],
    ) -> Result<()> {
        let header_slice = &mut header[constant_header_offset
            ..(constant_header_offset + <Self as RcSgaHeaderRepr<'a, D>>::CONSTANT_HEADER_SIZE)];
        let mut forward_pointer = MutForwardPointer(header_slice);
        forward_pointer.write_size(self.num_set as _);
        forward_pointer.write_offset(dynamic_header_start as _);
        let dest_slice = &mut header
            [dynamic_header_start..(dynamic_header_start + self.num_set * size_of::<T>())];
        dest_slice.copy_from_slice(&self.list_ptr);
        Ok(())
    }

    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, header_offset: usize) -> Result<()> {
        let header_slice = &buffer.as_ref()[header_offset
            ..(header_offset + <Self as RcSgaHeaderRepr<'a, D>>::CONSTANT_HEADER_SIZE)]
            .try_into()?;
        let forward_pointer = ForwardPointer(header_slice);
        let list_size = forward_pointer.get_size() as usize;
        let offset = forward_pointer.get_offset() as usize;
        self.num_set = list_size;
        self.num_space = list_size;
        self.list_ptr = BytesMut::with_capacity(list_size * size_of::<T>());
        self.list_ptr
            .chunk_mut()
            .copy_from_slice(&buffer.as_ref()[offset..(offset + list_size)]);
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
    list_ptr: RcSge<'a, D>,
    _marker: PhantomData<&'a [T]>,
}

impl<'a, T, D> Default for RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    fn default() -> Self {
        RefList {
            num_space: 0,
            list_ptr: RcSge::default(),
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

    // TODO: should this not be allowed? Would break ownership rules
    fn write_val(&mut self, val_idx: usize, val: T) {
        let offset =
            unsafe { (self.list_ptr.as_ref().as_ptr() as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    fn read_val(&self, val_idx: usize) -> &T {
        let offset =
            unsafe { (self.list_ptr.as_ref().as_ptr() as *const T).offset(val_idx as isize) };
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

impl<'a, T, D> RcSgaHeaderRepr<'a, D> for RefList<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    fn dynamic_header_size(&self) -> usize {
        self.num_space * size_of::<T>()
    }

    fn num_scatter_gather_entries(&self) -> usize {
        0
    }

    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn get_bitmap(&self) -> &[u8] {
        &[]
    }

    fn get_mut_bitmap(&mut self) -> &mut [u8] {
        &mut []
    }

    fn set_bitmap(&mut self, _bitmap: &[u8]) {}

    fn inner_serialize(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        _scatter_gather_entries: &mut [RcSge<'a, D>],
        _offsets: &mut [usize],
    ) -> Result<()> {
        let header_slice = &mut header[constant_header_offset
            ..(constant_header_offset + <Self as RcSgaHeaderRepr<'a, D>>::CONSTANT_HEADER_SIZE)];
        let mut forward_pointer = MutForwardPointer(header_slice);
        forward_pointer.write_size(self.num_space as _);
        forward_pointer.write_offset(dynamic_header_start as _);
        let list_slice = &mut header
            [dynamic_header_start..(dynamic_header_start + self.num_space * size_of::<T>())];
        list_slice.copy_from_slice(&self.list_ptr.as_ref()[0..self.num_space * size_of::<T>()]);
        Ok(())
    }

    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, header_offset: usize) -> Result<()> {
        let header_slice = &buffer.as_ref()[header_offset
            ..(header_offset + <Self as RcSgaHeaderRepr<'a, D>>::CONSTANT_HEADER_SIZE)]
            .try_into()?;
        let forward_pointer = ForwardPointer(header_slice);
        let list_size = forward_pointer.get_size() as usize;
        let offset = forward_pointer.get_offset() as usize;
        self.num_space = list_size;
        self.list_ptr = buffer.clone_with_bounds(offset, list_size * size_of::<T>())?;
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct VariableList<'a, T, D>
where
    T: RcSgaHeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    _marker: PhantomData<&'a [D]>,
}

impl<'a, T, D> VariableList<'a, T, D>
where
    T: RcSgaHeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
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
    T: RcSgaHeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        assert!(idx < self.num_space);
        &self.elts[idx]
    }
}

impl<'a, T, D> RcSgaHeaderRepr<'a, D> for VariableList<'a, T, D>
where
    T: RcSgaHeaderRepr<'a, D> + Debug + Default + PartialEq + Eq + Clone,
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUMBER_OF_FIELDS: usize = 1;

    fn dynamic_header_size(&self) -> usize {
        self.elts
            .iter()
            .map(|x| x.dynamic_header_size() + T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    fn dynamic_header_start(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|_x| T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    fn num_scatter_gather_entries(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|x| x.num_scatter_gather_entries())
            .sum()
    }

    fn get_bitmap(&self) -> &[u8] {
        &[]
    }

    fn get_mut_bitmap(&mut self) -> &mut [u8] {
        &mut []
    }

    fn set_bitmap(&mut self, _bitmap: &[u8]) {}

    fn inner_serialize(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'a, D>],
        offsets: &mut [usize],
    ) -> Result<()> {
        let header_slice = &mut header_buffer
            [constant_header_offset..(constant_header_offset) + Self::CONSTANT_HEADER_SIZE];
        let mut forward_pointer = MutForwardPointer(header_slice);
        forward_pointer.write_size(self.num_set as u32);
        forward_pointer.write_offset(dynamic_header_start as u32);

        let mut sge_idx = 0;
        let mut cur_dynamic_off = dynamic_header_start + self.dynamic_header_start();
        tracing::debug!(num_elts = self.elts.len(), "Info about list items");
        for (i, elt) in self.elts.iter().take(self.num_set).enumerate() {
            let required_sges = elt.num_scatter_gather_entries();
            elt.inner_serialize_with_ref(
                header_buffer,
                dynamic_header_start + T::CONSTANT_HEADER_SIZE * i,
                cur_dynamic_off,
                &mut scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                &mut offsets[sge_idx..(sge_idx + required_sges)],
                elt.dynamic_header_size() != 0,
            )?;
            sge_idx += required_sges;
            cur_dynamic_off += elt.dynamic_header_size();
        }
        Ok(())
    }

    // TODO: should offsets be written RELATIVE to the object that is being deserialized?
    fn inner_deserialize(&mut self, buffer: &RcSge<'a, D>, constant_offset: usize) -> Result<()> {
        let slice = &buffer.as_ref()
            [constant_offset..(constant_offset + Self::CONSTANT_HEADER_SIZE)]
            .try_into()?;
        let forward_pointer = ForwardPointer(slice);
        let size = forward_pointer.get_size() as usize;
        let dynamic_offset = forward_pointer.get_offset() as usize;

        self.num_set = size;
        if self.elts.len() < size {
            self.elts.resize(size, T::default());
        }

        for (i, elt) in self.elts.iter_mut().take(size).enumerate() {
            // for objects with no dynamic header size, no need to deserialize with ref
            elt.inner_deserialize_with_ref(
                buffer,
                dynamic_offset + i * T::CONSTANT_HEADER_SIZE,
                elt.dynamic_header_size() != 0,
            )?;
        }
        Ok(())
    }
}
