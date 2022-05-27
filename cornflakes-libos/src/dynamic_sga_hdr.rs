use super::{
    datapath::Datapath,
    {ArenaOrderedSga, OrderedSga, Sge},
};
use bitmaps::Bitmap;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use color_eyre::eyre::{bail, Result};
use std::{
    default::Default, fmt::Debug, marker::PhantomData, mem::size_of, ops::Index, slice,
    slice::Iter, str,
};

#[cfg(feature = "profiler")]
use perftools;

struct ForwardPointer<'a>(&'a [u8], usize);

impl<'a> ForwardPointer<'a> {
    #[inline]
    pub fn get_size(&self) -> u32 {
        LittleEndian::read_u32(&self.0[self.1..(self.1 + 34)])
    }

    #[inline]
    pub fn get_offset(&self) -> u32 {
        LittleEndian::read_u32(&self.0[(self.1 + 4)..(self.1 + 8)])
    }
}

struct MutForwardPointer<'a>(&'a mut [u8], usize);

impl<'a> MutForwardPointer<'a> {
    #[inline]
    pub fn write_size(&mut self, size: u32) {
        tracing::debug!("Writing size {} at {:?}", size, self.0.as_ptr());
        LittleEndian::write_u32(&mut self.0[self.1..(self.1 + 4)], size);
    }

    #[inline]
    pub fn write_offset(&mut self, off: u32) {
        LittleEndian::write_u32(&mut self.0[(self.1 + 4)..(self.1 + 8)], off);
    }
}
pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;
/// u32 at beginning representing bitmap size in bytes
pub const BITMAP_LENGTH_FIELD: usize = 4;

#[inline]
pub fn read_size_and_offset(offset: usize, buffer: &[u8]) -> Result<(usize, usize)> {
    let forward_pointer = ForwardPointer(buffer, offset);
    Ok((
        forward_pointer.get_size() as usize,
        forward_pointer.get_offset() as usize,
    ))
}

#[inline]
pub fn write_size_and_offset(write_offset: usize, size: usize, offset: usize, buffer: &mut [u8]) {
    let mut forward_pointer = MutForwardPointer(buffer, write_offset);
    tracing::debug!(write_offset, size, offset, "Writing in size and offset");
    forward_pointer.write_size(size as u32);
    forward_pointer.write_offset(offset as u32);
}

pub trait SgaHeaderRepr<'obj> {
    /// Maximum number of fields is max u32 * 8
    const NUMBER_OF_FIELDS: usize;

    /// Constant part of the header: containing constant sized fields and pointers to variable
    /// sized fields. Does not include the bitmap.
    const CONSTANT_HEADER_SIZE: usize;

    /// Number of bitmap entries needed to represent this type.
    const NUM_U32_BITMAPS: usize;

    fn get_bitmap_itermut(&mut self) -> std::slice::IterMut<Bitmap<32>> {
        [].iter_mut()
    }

    fn get_bitmap_iter(&self) -> std::slice::Iter<Bitmap<32>> {
        [].iter()
    }

    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unimplemented!();
    }

    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unimplemented!();
    }

    fn set_bitmap(&mut self, _bitmap: impl Iterator<Item = Bitmap<32>>) {}

    #[inline]
    fn bitmap_length() -> usize {
        Self::NUM_U32_BITMAPS * 4
    }

    #[inline]
    fn get_bitmap_field(&self, field: usize, bitmap_offset: usize) -> bool {
        self.get_bitmap_entry(bitmap_offset).get(field)
    }

    #[inline]
    fn set_bitmap_field(&mut self, field: usize, bitmap_offset: usize) {
        self.get_mut_bitmap_entry(bitmap_offset).set(field, true);
    }

    #[inline]
    fn clear_bitmap(&mut self) {
        for bitmap in self.get_bitmap_itermut() {
            *bitmap &= Bitmap::<32>::new();
        }
    }

    fn serialize_bitmap(&self, header: &mut [u8], offset: usize) {
        LittleEndian::write_u32(
            &mut header[offset..(offset + BITMAP_LENGTH_FIELD)],
            Self::NUM_U32_BITMAPS as u32,
        );

        for (i, bitmap) in self.get_bitmap_iter().enumerate() {
            let slice = &mut header[(offset + BITMAP_LENGTH_FIELD + i * 4)
                ..(offset + BITMAP_LENGTH_FIELD + (i + 1) * 4)];
            slice.copy_from_slice(bitmap.as_bytes());
        }
    }

    /// Copies bitmap into object's bitmap, returning the space from offset that the bitmap
    /// in the serialized header format takes.
    fn deserialize_bitmap(&mut self, header: &[u8], offset: usize) -> usize {
        let bitmap_size = LittleEndian::read_u32(&header[offset..(offset + BITMAP_LENGTH_FIELD)]);
        self.set_bitmap(
            (0..std::cmp::min(bitmap_size, Self::NUM_U32_BITMAPS as u32) as usize).map(|i| {
                let num = LittleEndian::read_u32(
                    &header[(offset + BITMAP_LENGTH_FIELD + i * 4)
                        ..(offset + BITMAP_LENGTH_FIELD + (i + 1) * 4)],
                );
                Bitmap::<32>::from_value(num)
            }),
        );
        bitmap_size as usize
    }

    fn check_deep_equality(&self, other: &Self) -> bool;

    /// Dynamic part of the header (actual bytes pointed to, lists, nested objects).
    fn dynamic_header_size(&self) -> usize;

    /// Total header size including the bitmap.
    #[inline]
    fn total_header_size(&self, with_ref: bool, _with_bitmap: bool) -> usize {
        //(BITMAP_LENGTH_FIELD + Self::bitmap_length()) * (with_bitmap as usize)
        <Self as SgaHeaderRepr>::CONSTANT_HEADER_SIZE * (with_ref as usize)
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
    #[inline]
    fn alloc_sga(&self) -> OrderedSga {
        OrderedSga::allocate(self.num_scatter_gather_entries())
    }

    #[inline]
    fn alloc_hdr(&self) -> Vec<u8> {
        vec![0u8; self.total_header_size(false, true)]
    }

    fn is_list(&self) -> bool {
        false
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
    fn inner_serialize<'sge>(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [Sge<'sge>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge;

    /// Nested deserialization function.
    /// Params:
    /// @buffer - Buffer to deserialize.
    /// @header_offset - Offset into constant part of header for nested deserialization.
    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], header_offset: usize) -> Result<()>
    where
        'buf: 'obj;

    #[inline]
    fn serialize_to_owned<D>(&self, datapath: &D) -> Result<Vec<u8>>
    where
        D: Datapath,
    {
        let mut sga = self.alloc_sga();
        self.serialize_into_sga(&mut sga, datapath)?;
        tracing::debug!(
            "Resulting header: {:?}, length: {}",
            sga.get_hdr(),
            sga.get_hdr().len()
        );
        Ok(sga.flatten())
    }

    #[inline]
    fn serialize_into_sga_with_hdr<'sge, D>(
        &self,
        header_buffer: &mut [u8],
        ordered_sga: &mut OrderedSga<'sge>,
        datapath: &D,
    ) -> Result<()>
    where
        D: Datapath,
        'obj: 'sge,
    {
        let required_entries = self.num_scatter_gather_entries();
        if ordered_sga.capacity() < required_entries {
            bail!(
                "Cannot serialize into sga with num entries {} ({} required)",
                ordered_sga.len(),
                required_entries,
            );
        }

        let header_size = self.total_header_size(false, true);
        if header_buffer.len() < header_size {
            bail!(
                "Cannot serialize into header with smaller than {} len, given: {:?}",
                header_size,
                header_buffer,
            );
        }

        ordered_sga.set_length(required_entries);
        let mut offsets: Vec<usize> = vec![0; required_entries];

        // recursive serialize each item
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("recursive serialization");
            self.inner_serialize(
                header_buffer,
                0,
                self.dynamic_header_start(),
                ordered_sga.mut_entries_slice(0, required_entries),
                offsets.as_mut_slice(),
            )?;
        }
        // reorder entries according to size threshold and whether entries are registered.
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("reorder sga");
            ordered_sga.reorder_by_size_and_registration(datapath, &mut offsets)?;
            // reorder entries if current (zero-copy segments + 1) exceeds max zero-copy segments
            ordered_sga.reorder_by_max_segs(datapath, &mut offsets)?;
        }
        let mut cur_dynamic_offset = self.total_header_size(false, false);

        tracing::debug!(starting_offsets = cur_dynamic_offset, "starting offsets");
        // iterate over header, writing in forward pointers based on new ordering
        tracing::debug!(header_addr =? header_buffer.as_ptr());
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("fill in sga offsets");
            for (sge, offset) in ordered_sga
                .entries_slice(0, required_entries)
                .iter()
                .zip(offsets.into_iter())
            {
                tracing::debug!(addr =? &header_buffer[offset..(offset + 8)].as_ptr(), "Addr without cast");
                let mut obj_ref = MutForwardPointer(header_buffer, offset);
                tracing::debug!(
                    "At offset {}, writing in size {} and offset {}",
                    offset,
                    sge.len(),
                    cur_dynamic_offset
                );
                obj_ref.write_size(sge.len() as u32);
                obj_ref.write_offset(cur_dynamic_offset as u32);

                cur_dynamic_offset += sge.len();
            }
        }
        Ok(())
    }

    #[inline]
    fn serialize_into_arena_sga_with_hdr<'sge, D>(
        &self,
        header_buffer: &mut [u8],
        ordered_sga: &mut ArenaOrderedSga<'sge>,
        datapath: &D,
    ) -> Result<()>
    where
        D: Datapath,
        'obj: 'sge,
    {
        let required_entries = self.num_scatter_gather_entries();
        if ordered_sga.capacity() < required_entries {
            bail!(
                "Cannot serialize into sga with num entries {} ({} required)",
                ordered_sga.len(),
                required_entries,
            );
        }

        let header_size = self.total_header_size(false, true);
        if header_buffer.len() < header_size {
            bail!(
                "Cannot serialize into header with smaller than {} len, given: {:?}",
                header_size,
                header_buffer,
            );
        }

        ordered_sga.set_length(required_entries);
        let mut offsets: Vec<usize> = vec![0; required_entries];

        // recursive serialize each item
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("recursive serialization");
            self.inner_serialize(
                header_buffer,
                0,
                self.dynamic_header_start(),
                ordered_sga.mut_entries_slice(0, required_entries),
                offsets.as_mut_slice(),
            )?;
        }
        // reorder entries according to size threshold and whether entries are registered.
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("reorder sga");
            ordered_sga.reorder_by_size_and_registration(datapath, &mut offsets)?;
            // reorder entries if current (zero-copy segments + 1) exceeds max zero-copy segments
            ordered_sga.reorder_by_max_segs(datapath, &mut offsets)?;
        }
        let mut cur_dynamic_offset = self.total_header_size(false, false);

        tracing::debug!(starting_offsets = cur_dynamic_offset, "starting offsets");
        // iterate over header, writing in forward pointers based on new ordering
        tracing::debug!(header_addr =? header_buffer.as_ptr());
        {
            #[cfg(feature = "profiler")]
            perftools::timer!("fill in sga offsets");
            for (sge, offset) in ordered_sga
                .entries_slice(0, required_entries)
                .iter()
                .zip(offsets.into_iter())
            {
                tracing::debug!(addr =? &header_buffer[offset..(offset + 8)].as_ptr(), "Addr without cast");
                let mut obj_ref = MutForwardPointer(header_buffer, offset);
                tracing::debug!(
                    "At offset {}, writing in size {} and offset {}",
                    offset,
                    sge.len(),
                    cur_dynamic_offset
                );
                obj_ref.write_size(sge.len() as u32);
                obj_ref.write_offset(cur_dynamic_offset as u32);

                cur_dynamic_offset += sge.len();
            }
        }
        Ok(())
    }

    #[inline]
    fn serialize_into_arena_sga<'sge, D>(
        &self,
        ordered_sga: &mut ArenaOrderedSga<'sge>,
        datapath: &D,
        arena: &'sge bumpalo::Bump,
    ) -> Result<()>
    where
        D: Datapath,
        'obj: 'sge,
    {
        let mut owned_hdr = {
            #[cfg(feature = "profiler")]
            perftools::timer!("alloc hdr");
            let size = self.total_header_size(false, true);
            bumpalo::collections::Vec::with_capacity_zeroed_in(size, arena)
        };
        tracing::debug!("Header size: {}", owned_hdr.len());
        let header_buffer = owned_hdr.as_mut_slice();
        self.serialize_into_arena_sga_with_hdr(header_buffer, ordered_sga, datapath)?;
        ordered_sga.set_hdr(owned_hdr);

        Ok(())
    }

    /// Serialize with context of existing ordered sga and existing header buffer.
    #[inline]
    fn serialize_into_sga<'sge, D>(
        &self,
        ordered_sga: &mut OrderedSga<'sge>,
        datapath: &D,
    ) -> Result<()>
    where
        D: Datapath,
        'obj: 'sge,
    {
        let mut owned_hdr = {
            #[cfg(feature = "profiler")]
            perftools::timer!("alloc hdr");
            self.alloc_hdr()
        };
        tracing::debug!("Header size: {}", owned_hdr.len());
        let header_buffer = owned_hdr.as_mut_slice();
        self.serialize_into_sga_with_hdr(header_buffer, ordered_sga, datapath)?;
        ordered_sga.set_hdr(owned_hdr);

        Ok(())
    }

    /// Deserialize contiguous buffer into this object.
    #[inline]
    fn deserialize<'buf>(&mut self, buffer: &'buf [u8]) -> Result<()>
    where
        'buf: 'obj,
    {
        tracing::debug!("buf addr: {:?}", buffer.as_ptr());
        self.inner_deserialize(buffer, 0)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CFString<'obj> {
    pub ptr: &'obj [u8],
}

impl<'obj> CFString<'obj> {
    #[inline]
    pub fn new(ptr: &'obj str) -> Self {
        CFString {
            ptr: ptr.as_bytes(),
        }
    }

    #[inline]
    pub fn new_from_bytes(ptr: &'obj [u8]) -> Self {
        CFString { ptr: ptr }
    }

    #[inline]
    pub fn bytes(&self) -> &'obj [u8] {
        self.ptr
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    /// Assumes that the string is utf8-encoded.
    #[inline]
    pub fn to_string(&self) -> String {
        str::from_utf8(self.ptr).unwrap().to_string()
    }
}

impl<'obj> Default for CFString<'obj> {
    #[inline]
    fn default() -> Self {
        CFString { ptr: &[] }
    }
}

impl<'obj> SgaHeaderRepr<'obj> for CFString<'obj> {
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        0
    }

    #[inline]
    fn num_scatter_gather_entries(&self) -> usize {
        1
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn check_deep_equality(&self, other: &CFString) -> bool {
        self.len() == other.len() && self.bytes().to_vec() == other.bytes().to_vec()
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        _header: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        scatter_gather_entries: &mut [Sge<'sge>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        scatter_gather_entries[0] = Sge::new(self.ptr);
        offsets[0] = constant_header_offset;
        Ok(())
    }

    #[inline]
    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], header_offset: usize) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(buffer, header_offset);
        let offset = forward_pointer.get_offset() as usize;
        let size = forward_pointer.get_size() as usize;
        tracing::debug!(offset = offset, size = size, "Deserializing into cf bytes");
        let ptr = &buffer[offset..(offset + size)];
        self.ptr = ptr;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct CFBytes<'obj> {
    ptr: &'obj [u8],
}

impl<'obj> CFBytes<'obj> {
    #[inline]
    pub fn new(ptr: &'obj [u8]) -> Self {
        CFBytes { ptr: ptr }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    #[inline]
    pub fn get_ptr(&self) -> &'obj [u8] {
        self.ptr
    }
}

impl<'obj> Default for CFBytes<'obj> {
    #[inline]
    fn default() -> Self {
        CFBytes {
            ptr: Default::default(),
        }
    }
}

impl<'obj> SgaHeaderRepr<'obj> for CFBytes<'obj> {
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        tracing::debug!("Dynamic hdr size for cf bytes: 0");
        0
    }

    #[inline]
    fn num_scatter_gather_entries(&self) -> usize {
        1
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn check_deep_equality(&self, other: &CFBytes) -> bool {
        if self.len() != other.len() {
            tracing::debug!(ours = self.len(), theirs = other.len(), "Lengths not equal");
        }

        if self.get_ptr().to_vec() != other.get_ptr().to_vec() {
            tracing::debug!("ours: {:?} {:?}", self.get_ptr().as_ptr(), self.get_ptr());
            tracing::debug!(
                "theirs: {:?} {:?}",
                other.get_ptr().as_ptr(),
                other.get_ptr()
            );
        }
        self.len() == other.len() && self.get_ptr().to_vec() == other.get_ptr().to_vec()
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        _header: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        scatter_gather_entries: &mut [Sge<'sge>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        scatter_gather_entries[0] = Sge::new(self.ptr);
        offsets[0] = constant_header_offset;
        Ok(())
    }

    #[inline]
    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], header_offset: usize) -> Result<()>
    where
        'buf: 'obj,
    {
        tracing::debug!("buffer addr: {:?}", buffer.as_ptr());
        let forward_pointer = ForwardPointer(buffer, header_offset);
        let offset = forward_pointer.get_offset() as usize;
        let size = forward_pointer.get_size() as usize;
        let ptr = &buffer[offset..(offset + size)];
        self.ptr = ptr;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum List<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    Owned(OwnedList<T>),
    Ref(RefList<'a, T>),
}

impl<'a, T> Default for List<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    #[inline]
    fn default() -> Self {
        List::Owned(OwnedList::default())
    }
}

impl<'a, T> Index<usize> for List<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    #[inline]
    fn index(&self, idx: usize) -> &T {
        match self {
            List::Owned(owned_list) => owned_list.index(idx),
            List::Ref(ref_list) => ref_list.index(idx),
        }
    }
}

impl<'obj, T> List<'obj, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    #[inline]
    pub fn init(size: usize) -> List<'obj, T> {
        List::Owned(OwnedList::init(size))
    }

    #[inline]
    pub fn init_ref() -> List<'obj, T> {
        List::Ref(RefList::default())
    }

    #[inline]
    pub fn append(&mut self, val: T) {
        match self {
            List::Owned(ref mut owned_list) => owned_list.append(val),
            List::Ref(ref mut _ref_list) => {
                panic!("Should not be calling append on a ref list.")
            }
        }
    }

    #[inline]
    pub fn replace(&mut self, idx: usize, val: T) {
        match self {
            List::Owned(ref mut owned_list) => owned_list.replace(idx, val),
            List::Ref(ref mut ref_list) => ref_list.replace(idx, val),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            List::Owned(owned_list) => owned_list.len(),
            List::Ref(ref_list) => ref_list.len(),
        }
    }
}

impl<'obj, T> SgaHeaderRepr<'obj> for List<'obj, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, offset: usize) -> &mut Bitmap<32> {
        match self {
            List::Owned(owned_list) => owned_list.get_mut_bitmap_entry(offset),
            List::Ref(ref_list) => ref_list.get_mut_bitmap_entry(offset),
        }
    }

    #[inline]
    fn get_bitmap_entry(&self, offset: usize) -> &Bitmap<32> {
        match self {
            List::Owned(owned_list) => owned_list.get_bitmap_entry(offset),
            List::Ref(ref_list) => ref_list.get_bitmap_entry(offset),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        match self {
            List::Owned(owned_list) => owned_list.dynamic_header_size(),
            List::Ref(ref_list) => ref_list.dynamic_header_size(),
        }
    }

    #[inline]
    fn num_scatter_gather_entries(&self) -> usize {
        0
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }

    #[inline]
    fn is_list(&self) -> bool {
        true
    }

    #[inline]
    fn check_deep_equality(&self, other: &List<T>) -> bool {
        if self.len() != other.len() {
            tracing::debug!(
                "List length not equal, ours: {}, theirs: {}",
                self.len(),
                other.len()
            );
            return false;
        }

        for i in 0..self.len() {
            if self[i] != other[i] {
                return false;
            }
        }
        true
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [Sge<'sge>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
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

    #[inline]
    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], header_offset: usize) -> Result<()>
    where
        'buf: 'obj,
    {
        match self {
            List::Owned(l) => l.inner_deserialize(buffer, header_offset),
            List::Ref(l) => l.inner_deserialize(buffer, header_offset),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OwnedList<T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    num_space: usize,
    num_set: usize,
    list_ptr: BytesMut,
    _marker: PhantomData<T>,
}

impl<T> OwnedList<T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    #[inline]
    pub fn init(size: usize) -> OwnedList<T> {
        let mut buf_mut = BytesMut::with_capacity(size * size_of::<T>());
        buf_mut.put(vec![0u8; size * size_of::<T>()].as_slice());
        OwnedList {
            num_space: size,
            num_set: 0,
            list_ptr: buf_mut,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn append(&mut self, val: T) {
        self.write_val(self.num_set, val);
        self.num_set += 1;
    }

    #[inline]
    pub fn replace(&mut self, idx: usize, val: T) {
        self.write_val(idx, val);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_set
    }

    #[inline]
    fn write_val(&mut self, val_idx: usize, val: T) {
        let offset = unsafe { (self.list_ptr.as_mut_ptr() as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    #[inline]
    fn read_val(&self, val_idx: usize) -> &T {
        let offset = unsafe { (self.list_ptr.as_ptr() as *const T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts(offset, 1) };
        &t_slice[0]
    }
}

impl<T> Index<usize> for OwnedList<T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    #[inline]
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'obj, T> SgaHeaderRepr<'obj> for OwnedList<T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        self.num_set * size_of::<T>()
    }

    #[inline]
    fn num_scatter_gather_entries(&self) -> usize {
        0
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }

    #[inline]
    fn is_list(&self) -> bool {
        true
    }

    #[inline]
    fn check_deep_equality(&self, other: &OwnedList<T>) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for i in 0..self.len() {
            if self[i] != other[i] {
                return false;
            }
        }
        true
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        _scatter_gather_entries: &mut [Sge<'sge>],
        _offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        let mut forward_pointer = MutForwardPointer(header, constant_header_offset);
        forward_pointer.write_size(self.num_set as _);
        forward_pointer.write_offset(dynamic_header_start as _);
        let dest_slice = &mut header
            [dynamic_header_start..(dynamic_header_start + self.num_set * size_of::<T>())];
        dest_slice.copy_from_slice(&self.list_ptr);
        Ok(())
    }

    #[inline]
    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], header_offset: usize) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(buffer, header_offset);
        let list_size = forward_pointer.get_size() as usize;
        let offset = forward_pointer.get_offset() as usize;
        self.num_set = list_size;
        self.num_space = list_size;
        self.list_ptr = BytesMut::with_capacity(list_size * size_of::<T>());
        self.list_ptr
            .chunk_mut()
            .copy_from_slice(&buffer[offset..(offset + list_size)]);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefList<'obj, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    num_space: usize,
    list_ptr: &'obj [u8],
    _marker: PhantomData<&'obj [T]>,
}

impl<'obj, T> Default for RefList<'obj, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    #[inline]
    fn default() -> Self {
        RefList {
            num_space: 0,
            list_ptr: &[],
            _marker: PhantomData,
        }
    }
}

impl<'obj, T> RefList<'obj, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    #[inline]
    pub fn replace(&mut self, idx: usize, val: T) {
        self.write_val(idx, val);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_space
    }

    // TODO: should this not be allowed? Would break ownership rules
    #[inline]
    fn write_val(&mut self, val_idx: usize, val: T) {
        let offset = unsafe { (self.list_ptr.as_ptr() as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    #[inline]
    fn read_val(&self, val_idx: usize) -> &T {
        let offset = unsafe { (self.list_ptr.as_ptr() as *const T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts(offset, 1) };
        &t_slice[0]
    }
}

impl<'a, T> Index<usize> for RefList<'a, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    type Output = T;
    #[inline]
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'obj, T> SgaHeaderRepr<'obj> for RefList<'obj, T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn is_list(&self) -> bool {
        true
    }

    #[inline]
    fn num_scatter_gather_entries(&self) -> usize {
        0
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        self.num_space * size_of::<T>()
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        Self::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn check_deep_equality(&self, other: &RefList<T>) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for i in 0..self.len() {
            if self[i] != other[i] {
                return false;
            }
        }
        true
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        _scatter_gather_entries: &mut [Sge<'sge>],
        _offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        let mut forward_pointer = MutForwardPointer(header, constant_header_offset);
        forward_pointer.write_size(self.num_space as _);
        forward_pointer.write_offset(dynamic_header_start as _);
        let list_slice = &mut header
            [dynamic_header_start..(dynamic_header_start + self.num_space * size_of::<T>())];
        list_slice.copy_from_slice(&self.list_ptr[0..self.num_space * size_of::<T>()]);
        Ok(())
    }

    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], header_offset: usize) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(&buffer, header_offset);
        let list_size = forward_pointer.get_size() as usize;
        let offset = forward_pointer.get_offset() as usize;
        self.num_space = list_size;
        self.list_ptr = &buffer[offset as usize..(list_size * size_of::<T>())];
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct VariableList<'obj, T>
where
    T: SgaHeaderRepr<'obj> + Debug + Default + PartialEq + Eq + Clone,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    _phantom_data: PhantomData<&'obj [u8]>,
}

impl<'obj, T> VariableList<'obj, T>
where
    T: SgaHeaderRepr<'obj> + Debug + Default + PartialEq + Eq + Clone,
{
    #[inline]
    pub fn init(num: usize) -> VariableList<'obj, T> {
        VariableList {
            num_space: num,
            num_set: 0,
            elts: Vec::with_capacity(num),
            _phantom_data: PhantomData,
        }
    }

    #[inline]
    pub fn iter(&self) -> std::iter::Take<Iter<T>> {
        self.elts.iter().take(self.num_set)
    }

    #[inline]
    pub fn append(&mut self, val: T) {
        tracing::debug!("Appending to the list");
        self.elts.push(val);
        self.num_set += 1;
    }

    #[inline]
    pub fn replace(&mut self, idx: usize, val: T) {
        self.elts[idx] = val;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_set
    }
}
impl<'obj, T> Index<usize> for VariableList<'obj, T>
where
    T: SgaHeaderRepr<'obj> + Debug + Default + PartialEq + Eq + Clone,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        tracing::debug!(idx = idx, self.num_space = self.num_space, "CALLING INDEX");
        &self.elts[idx]
    }
}

impl<'obj, T> SgaHeaderRepr<'obj> for VariableList<'obj, T>
where
    T: SgaHeaderRepr<'obj> + Debug + Default + PartialEq + Eq + Clone,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUMBER_OF_FIELDS: usize = 1;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        self.elts
            .iter()
            .map(|x| x.dynamic_header_size() + T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|_x| T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    #[inline]
    fn num_scatter_gather_entries(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|x| x.num_scatter_gather_entries())
            .sum()
    }

    #[inline]
    fn is_list(&self) -> bool {
        true
    }

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for i in 0..self.len() {
            tracing::debug!(i = i, "Checking equality for list elt");
            if !(self[i].check_deep_equality(&other[i])) {
                return false;
            }
        }

        true
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        scatter_gather_entries: &mut [Sge<'sge>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        #[cfg(feature = "profiler")]
        perftools::timer!("List inner serialize");

        {
            let mut forward_pointer = MutForwardPointer(header_buffer, constant_header_offset);
            forward_pointer.write_size(self.num_set as u32);
            forward_pointer.write_offset(dynamic_header_start as u32);
        }

        let mut sge_idx = 0;
        let mut cur_dynamic_off = dynamic_header_start + self.dynamic_header_start();
        tracing::debug!(num_elts = self.elts.len(), "Info about list items");
        for (i, elt) in self.elts.iter().take(self.num_set).enumerate() {
            let required_sges = elt.num_scatter_gather_entries();
            if elt.dynamic_header_size() != 0 {
                let mut forward_offset = MutForwardPointer(
                    header_buffer,
                    dynamic_header_start + T::CONSTANT_HEADER_SIZE * i,
                );
                // TODO: might be unnecessary
                forward_offset.write_size(elt.dynamic_header_size() as u32);
                forward_offset.write_offset(cur_dynamic_off as u32);
                elt.inner_serialize(
                    header_buffer,
                    cur_dynamic_off,
                    cur_dynamic_off + elt.dynamic_header_start(),
                    &mut scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                    &mut offsets[sge_idx..(sge_idx + required_sges)],
                )?;
            } else {
                elt.inner_serialize(
                    header_buffer,
                    dynamic_header_start + T::CONSTANT_HEADER_SIZE * i,
                    cur_dynamic_off,
                    &mut scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                    &mut offsets[sge_idx..(sge_idx + required_sges)],
                )?;
            }
            sge_idx += required_sges;
            cur_dynamic_off += elt.dynamic_header_size();
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize<'buf>(&mut self, buffer: &'buf [u8], constant_offset: usize) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(buffer, constant_offset);
        let size = forward_pointer.get_size() as usize;
        let dynamic_offset = forward_pointer.get_offset() as usize;

        self.num_set = size;
        if self.elts.len() < size {
            self.elts.resize(size, T::default());
        }
        self.num_space = size;
        tracing::debug!(
            size = size,
            offset = dynamic_offset,
            num_space = self.num_space,
            num_set = self.num_set,
            "Deserializing list at offset and size"
        );

        for (i, elt) in self.elts.iter_mut().take(size).enumerate() {
            if elt.dynamic_header_size() == 0 {
                elt.inner_deserialize(buffer, dynamic_offset + i * T::CONSTANT_HEADER_SIZE)?;
            } else {
                let (_size, dynamic_off) =
                    read_size_and_offset(dynamic_offset + i * T::CONSTANT_HEADER_SIZE, buffer)?;
                elt.inner_deserialize(buffer, dynamic_off)?;
            }
        }
        Ok(())
    }
}
