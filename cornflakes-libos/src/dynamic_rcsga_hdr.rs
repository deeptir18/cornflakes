use super::{
    datapath::{Datapath, ReceivedPkt},
    ArenaOrderedRcSga, OrderedRcSga, RcSge,
};
use bitmaps::Bitmap;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use color_eyre::eyre::{bail, Result, WrapErr};
use std::{
    default::Default, fmt::Debug, marker::PhantomData, mem::size_of, ops::Index, slice,
    slice::Iter, str,
};

#[inline]
pub fn write_size_and_offset(write_offset: usize, size: usize, offset: usize, buffer: &mut [u8]) {
    let mut forward_pointer = MutForwardPointer(buffer, write_offset);
    forward_pointer.write_size(size as u32);
    forward_pointer.write_offset(offset as u32);
}

#[inline]
pub fn read_size_and_offset<'buf, D>(
    offset: usize,
    buffer: &RcSge<'buf, D>,
) -> Result<(usize, usize)>
where
    D: Datapath,
{
    let forward_pointer = ForwardPointer(buffer.addr(), offset);
    Ok((
        forward_pointer.get_size() as usize,
        forward_pointer.get_offset() as usize,
    ))
}

struct ForwardPointer<'a>(&'a [u8], usize);

impl<'a> ForwardPointer<'a> {
    #[inline]
    pub fn get_size(&self) -> u32 {
        LittleEndian::read_u32(&self.0[self.1..(self.1 + 4)])
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

pub trait RcSgaHeaderRepr<'obj, D>
where
    D: Datapath,
{
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
    fn deserialize_bitmap<'buf>(&mut self, pkt: &RcSge<'buf, D>, offset: usize) -> usize
    where
        'buf: 'obj,
    {
        let header = pkt.as_ref();
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
        bitmap_size as usize * 4
    }

    fn check_deep_equality(&self, other: &Self) -> bool;

    /// Dynamic part of the header (actual bytes pointed to, lists, nested objects).
    fn dynamic_header_size(&self) -> usize;

    /// Total header size.
    fn total_header_size(&self, with_ref: bool, _with_bitmap: bool) -> usize {
        <Self as RcSgaHeaderRepr<D>>::CONSTANT_HEADER_SIZE * (with_ref as usize)
            + self.dynamic_header_size()
    }

    /// Number of scatter-gather entries (pointers and nested pointers to variable bytes or string
    /// fields).
    fn num_scatter_gather_entries(&self) -> usize;

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize;

    fn is_list(&self) -> bool {
        false
    }

    #[inline]
    fn alloc_sga(&self) -> OrderedRcSga<D> {
        OrderedRcSga::allocate(self.num_scatter_gather_entries())
    }

    #[inline]
    fn alloc_hdr(&self) -> Vec<u8> {
        vec![0u8; self.total_header_size(false, false)]
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
        scatter_gather_entries: &mut [RcSge<'sge, D>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge;

    /// Nested deserialization function.
    /// Params:
    /// @buffer - Buffer to deserialize.
    /// @header_offset - Offset into constant part of header for nested deserialization.
    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        header_offset: usize,
    ) -> Result<()>
    where
        'buf: 'obj;

    /// Serialize into buffer
    #[inline]
    fn serialize_into_buf(&self, datapath: &D, header_buffer: &mut [u8]) -> Result<usize> {
        let mut sga = self.alloc_sga();
        self.serialize_into_sga_with_hdr(header_buffer, &mut sga, datapath)?;
        // copy sga into header buffer
        let header_size = self.total_header_size(false, false);
        let size = sga.copy_into_buffer(&mut header_buffer[header_size..])?;
        Ok(size + header_size)
    }

    #[inline]
    fn serialize_to_owned(&self, datapath: &D) -> Result<Vec<u8>> {
        let mut sga = self.alloc_sga();
        self.serialize_into_sga(&mut sga, datapath)?;
        Ok(sga.flatten())
    }

    #[inline]
    fn serialize_into_sga_with_hdr<'sge>(
        &self,
        header_buffer: &mut [u8],
        ordered_sga: &mut OrderedRcSga<'sge, D>,
        datapath: &D,
    ) -> Result<()>
    where
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

        let header_size = self.total_header_size(false, false);
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
            demikernel::timer!("recursive serialization");
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
            demikernel::timer!("reorder sga");
            ordered_sga.reorder_by_size_and_registration(datapath, &mut offsets)?;
            // reorder entries if current (zero-copy segments + 1) exceeds max zero-copy segments
        }
        let mut cur_dynamic_offset = self.total_header_size(false, false);

        // iterate over header, writing in forward pointers based on new ordering
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("fill in sga offsets");
            for (sge, offset) in ordered_sga
                .entries_slice(0, required_entries)
                .iter()
                .zip(offsets.into_iter())
            {
                let mut obj_ref = MutForwardPointer(header_buffer, offset);
                obj_ref.write_size(sge.len() as u32);
                obj_ref.write_offset(cur_dynamic_offset as u32);

                cur_dynamic_offset += sge.len();
            }
        }
        Ok(())
    }

    /// Serialize with context of existing ordered sga and existing header buffer.
    #[inline]
    fn serialize_into_sga<'sge>(
        &self,
        ordered_sga: &mut OrderedRcSga<'sge, D>,
        datapath: &D,
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        let mut owned_hdr = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("alloc hdr");
            self.alloc_hdr()
        };
        let header_buffer = owned_hdr.as_mut_slice();
        self.serialize_into_sga_with_hdr(header_buffer, ordered_sga, datapath)?;
        ordered_sga.set_hdr(owned_hdr);

        Ok(())
    }

    #[inline]
    fn partially_serialize_into_arena_sga_with_hdr<'sge>(
        &self,
        header_buffer: &mut [u8],
        ordered_sga: &mut ArenaOrderedRcSga<'sge, D>,
    ) -> Result<()>
    where
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
        ordered_sga.set_length(required_entries);

        let header_size = self.total_header_size(false, false);
        if header_buffer.len() < header_size {
            bail!(
                "Cannot serialize into header with smaller than {} len, given: {:?}",
                header_size,
                header_buffer,
            );
        }

        // recursive serialize each item
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("recursive serialization");
            let entries = &mut ordered_sga.entries;
            let offsets = &mut ordered_sga.offsets;
            self.inner_serialize(
                header_buffer,
                0,
                self.dynamic_header_start(),
                &mut entries.as_mut_slice()[0..required_entries],
                &mut offsets.as_mut_slice()[0..required_entries],
            )?;
        }

        ordered_sga.set_header_offsets(self.total_header_size(false, false));
        Ok(())
    }

    #[inline]
    fn serialize_into_arena_sga_with_hdr<'sge>(
        &self,
        header_buffer: &mut [u8],
        ordered_sga: &mut ArenaOrderedRcSga<'sge, D>,
        datapath: &D,
        with_copy: bool,
    ) -> Result<()>
    where
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

        let header_size = self.total_header_size(false, false);
        if header_buffer.len() < header_size {
            bail!(
                "Cannot serialize into header with smaller than {} len, given: {:?}",
                header_size,
                header_buffer,
            );
        }

        ordered_sga.set_length(required_entries);

        // recursive serialize each item
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("recursive serialization");
            let entries = &mut ordered_sga.entries;
            let offsets = &mut ordered_sga.offsets;
            self.inner_serialize(
                header_buffer,
                0,
                self.dynamic_header_start(),
                &mut entries.as_mut_slice()[0..required_entries],
                &mut offsets.as_mut_slice()[0..required_entries],
            )?;
        }
        if !with_copy {
            // reorder entries according to size threshold and whether entries are registered.
            {
                #[cfg(feature = "profiler")]
                demikernel::timer!("reorder sga");
                ordered_sga.reorder_by_size_and_registration(datapath, with_copy)?;
            }
        } else {
            ordered_sga.set_num_copy_entries(required_entries);
        }
        let mut cur_dynamic_offset = self.total_header_size(false, false);

        // iterate over header, writing in forward pointers based on new ordering
        {
            #[cfg(feature = "profiler")]
            demikernel::timer!("fill in sga offsets");
            for (sge, offset) in ordered_sga
                .entries_slice(0, required_entries)
                .iter()
                .zip(ordered_sga.offsets_slice(0, required_entries))
            {
                let mut obj_ref = MutForwardPointer(header_buffer, *offset);
                obj_ref.write_size(sge.len() as u32);
                obj_ref.write_offset(cur_dynamic_offset as u32);

                cur_dynamic_offset += sge.len();
            }
        }
        Ok(())
    }

    #[inline]
    fn partially_serialize_into_arena_sga<'sge>(
        &self,
        ordered_sga: &mut ArenaOrderedRcSga<'sge, D>,
        arena: &'sge bumpalo::Bump,
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        let mut owned_hdr = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("alloc hdr");
            let size = self.total_header_size(false, false);
            bumpalo::collections::Vec::with_capacity_zeroed_in(size, arena)
        };
        let header_buffer = owned_hdr.as_mut_slice();
        self.partially_serialize_into_arena_sga_with_hdr(header_buffer, ordered_sga)?;
        ordered_sga.set_hdr(owned_hdr);

        Ok(())
    }
    #[inline]
    fn serialize_into_arena_sga<'sge>(
        &self,
        ordered_sga: &mut ArenaOrderedRcSga<'sge, D>,
        arena: &'sge bumpalo::Bump,
        datapath: &D,
        with_copy: bool,
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        let mut owned_hdr = {
            #[cfg(feature = "profiler")]
            demikernel::timer!("alloc hdr");
            let size = self.total_header_size(false, false);
            bumpalo::collections::Vec::with_capacity_zeroed_in(size, arena)
        };
        let header_buffer = owned_hdr.as_mut_slice();
        self.serialize_into_arena_sga_with_hdr(header_buffer, ordered_sga, datapath, with_copy)?;
        ordered_sga.set_hdr(owned_hdr);

        Ok(())
    }

    #[inline]
    fn deserialize_from_pkt(
        &mut self,
        packet: &ReceivedPkt<D>,
        framing_offset: usize,
    ) -> Result<()> {
        let packet_len = packet.data_len();
        let metadata = packet
            .contiguous_datapath_metadata(framing_offset, packet_len - framing_offset)?
            .unwrap();
        let rc_sge = RcSge::RefCounted(metadata);
        self.inner_deserialize(&rc_sge, 0)?;
        Ok(())
    }

    fn deserialize_from_buf<'buf>(&mut self, buf: &'buf [u8]) -> Result<()>
    where
        'buf: 'obj,
    {
        tracing::debug!("In deserialize from buf");
        let rc_sge = RcSge::RawRef(buf);
        self.inner_deserialize(&rc_sge, 0)?;
        Ok(())
    }
}

pub struct CFString<'obj, D>
where
    D: Datapath,
{
    ptr: RcSge<'obj, D>,
}

impl<'obj, D> Clone for CFString<'obj, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        CFString {
            ptr: self.ptr.clone(),
        }
    }
}

impl<'obj, D> Default for CFString<'obj, D>
where
    D: Datapath,
{
    fn default() -> Self {
        CFString {
            ptr: RcSge::default(),
        }
    }
}

impl<'obj, D> std::fmt::Debug for CFString<'obj, D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CFString").field("ptr", &self.ptr).finish()
    }
}

impl<'obj, D> CFString<'obj, D>
where
    D: Datapath,
{
    /// Constructs a new raw cf string from a string pointer.
    #[inline]
    pub fn new_from_str(ptr: &'obj str) -> Self {
        CFString {
            ptr: RcSge::RawRef(ptr.as_bytes()),
        }
    }

    /// Constructs a raw cf string from bytes.
    #[inline]
    pub fn new_from_bytes(ptr: &'obj [u8]) -> Self {
        CFString {
            ptr: RcSge::RawRef(ptr),
        }
    }

    /// Transparently constructs from bytes, recovering datapath metadata if possible.
    #[inline]
    pub fn new(buf: &'obj [u8], datapath: &D) -> Result<Self> {
        let rc_sge = match datapath.recover_metadata(buf)? {
            Some(m) => RcSge::RefCounted(m),
            None => RcSge::RawRef(buf),
        };
        Ok(CFString { ptr: rc_sge })
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

    pub fn as_bytes(&self) -> &[u8] {
        self.ptr.as_ref()
    }
}

impl<'obj, D> RcSgaHeaderRepr<'obj, D> for CFString<'obj, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

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

    fn check_deep_equality(&self, other: &CFString<D>) -> bool {
        self.len() == other.len() && self.as_bytes().to_vec() == other.as_bytes().to_vec()
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        _header: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'sge, D>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        scatter_gather_entries[0] = self.ptr.clone();
        offsets[0] = constant_header_offset;
        Ok(())
    }

    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        header_offset: usize,
    ) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(buffer.addr(), header_offset);
        let off = forward_pointer.get_offset() as usize;
        let size = forward_pointer.get_size() as usize;
        self.ptr = buffer.clone_with_bounds(off, size)?;
        Ok(())
    }
}

pub struct CFBytes<'obj, D>
where
    D: Datapath,
{
    ptr: RcSge<'obj, D>,
}

impl<'obj, D> Clone for CFBytes<'obj, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        CFBytes {
            ptr: self.ptr.clone(),
        }
    }
}

impl<'obj, D> Default for CFBytes<'obj, D>
where
    D: Datapath,
{
    fn default() -> Self {
        CFBytes {
            ptr: RcSge::default(),
        }
    }
}

impl<'obj, D> std::fmt::Debug for CFBytes<'obj, D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CFBytes").field("ptr", &self.ptr).finish()
    }
}

impl<'obj, D> CFBytes<'obj, D>
where
    D: Datapath,
{
    pub fn new_from_bytes(ptr: &'obj [u8]) -> Self {
        CFBytes {
            ptr: RcSge::RawRef(ptr),
        }
    }

    #[inline]
    pub fn new(ptr: &'obj [u8], datapath: &D) -> Result<Self> {
        let rc_sge = match datapath.recover_metadata(ptr)? {
            Some(m) => RcSge::RefCounted(m),
            None => RcSge::RawRef(ptr),
        };
        Ok(CFBytes { ptr: rc_sge })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.ptr.as_ref()
    }
}

impl<'obj, D> RcSgaHeaderRepr<'obj, D> for CFBytes<'obj, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

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

    fn check_deep_equality(&self, other: &CFBytes<D>) -> bool {
        self.len() == other.len() && self.as_bytes().to_vec() == other.as_bytes().to_vec()
    }

    #[inline]
    fn inner_serialize<'sge>(
        &self,
        _header: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        scatter_gather_entries: &mut [RcSge<'sge, D>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        scatter_gather_entries[0] = self.ptr.clone();
        offsets[0] = constant_header_offset;
        Ok(())
    }

    #[inline]
    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        header_offset: usize,
    ) -> Result<()>
    where
        'buf: 'obj,
    {
        tracing::debug!("Inner deserialize for cf bytes, off = {}", header_offset);
        let forward_pointer = ForwardPointer(buffer.addr(), header_offset);
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
    Owned(OwnedList<T>),
    Ref(RefList<'a, T, D>),
}

impl<'a, T, D> Default for List<'a, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    #[inline]
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
    #[inline]
    fn index(&self, idx: usize) -> &T {
        match self {
            List::Owned(owned_list) => owned_list.index(idx),
            List::Ref(ref_list) => ref_list.index(idx),
        }
    }
}

impl<'obj, T, D> List<'obj, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    #[inline]
    pub fn init(size: usize) -> Self {
        List::Owned(OwnedList::init(size))
    }

    #[inline]
    pub fn init_ref() -> Self {
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

impl<'obj, T, D> RcSgaHeaderRepr<'obj, D> for List<'obj, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn get_mut_bitmap_entry(&mut self, offset: usize) -> &mut Bitmap<32> {
        match self {
            List::Owned(owned_list) => {
                <OwnedList<T> as RcSgaHeaderRepr<'obj, D>>::get_mut_bitmap_entry(owned_list, offset)
            }
            List::Ref(ref_list) => ref_list.get_mut_bitmap_entry(offset),
        }
    }

    #[inline]
    fn get_bitmap_entry(&self, offset: usize) -> &Bitmap<32> {
        match self {
            List::Owned(owned_list) => {
                <OwnedList<T> as RcSgaHeaderRepr<'obj, D>>::get_bitmap_entry(owned_list, offset)
            }
            List::Ref(ref_list) => ref_list.get_bitmap_entry(offset),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        match self {
            List::Owned(owned_list) => {
                <OwnedList<T> as RcSgaHeaderRepr<'obj, D>>::dynamic_header_size(owned_list)
            }
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
    fn check_deep_equality(&self, other: &Self) -> bool {
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
        scatter_gather_entries: &mut [RcSge<'sge, D>],
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
    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        header_offset: usize,
    ) -> Result<()>
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

impl<'obj, D, T> RcSgaHeaderRepr<'obj, D> for OwnedList<T>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
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
        _scatter_gather_entries: &mut [RcSge<'sge, D>],
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
    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        header_offset: usize,
    ) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(buffer.addr(), header_offset);
        let list_size = forward_pointer.get_size() as usize;
        let offset = forward_pointer.get_offset() as usize;
        self.num_set = list_size;
        self.num_space = list_size;
        self.list_ptr = BytesMut::with_capacity(list_size * size_of::<T>());
        self.list_ptr
            .chunk_mut()
            .copy_from_slice(&buffer.addr()[offset..(offset + list_size)]);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefList<'obj, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    num_space: usize,
    list_ptr: RcSge<'obj, D>,
    _marker: PhantomData<&'obj [T]>,
}

impl<'obj, T, D> Default for RefList<'obj, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
{
    #[inline]
    fn default() -> Self {
        RefList {
            num_space: 0,
            list_ptr: RcSge::RawRef(&[]),
            _marker: PhantomData,
        }
    }
}

impl<'obj, T, D> RefList<'obj, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
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
        let offset =
            unsafe { (self.list_ptr.as_ref().as_ptr() as *mut T).offset(val_idx as isize) };
        let t_slice = unsafe { slice::from_raw_parts_mut(offset, 1) };
        t_slice[0] = val;
    }

    #[inline]
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
    #[inline]
    fn index(&self, idx: usize) -> &Self::Output {
        &self.read_val(idx)
    }
}

impl<'obj, D, T> RcSgaHeaderRepr<'obj, D> for RefList<'obj, T, D>
where
    T: Default + Debug + Clone + PartialEq + Eq,
    D: Datapath,
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
    fn check_deep_equality(&self, other: &Self) -> bool {
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
        _scatter_gather_entries: &mut [RcSge<'sge, D>],
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
        list_slice.copy_from_slice(&self.list_ptr.as_ref()[0..self.num_space * size_of::<T>()]);
        Ok(())
    }

    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        header_offset: usize,
    ) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(&buffer.addr(), header_offset);
        let list_size = forward_pointer.get_size() as usize;
        let offset = forward_pointer.get_offset() as usize;
        self.num_space = list_size;
        self.list_ptr = buffer.clone_with_bounds(offset as usize, list_size * size_of::<T>())?;
        Ok(())
    }
}

pub struct VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    _phantom_data: PhantomData<(&'obj [u8], D)>,
}

impl<'obj, T, D> Clone for VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    fn clone(&self) -> Self {
        VariableList {
            num_space: self.num_space,
            num_set: self.num_set,
            elts: self.elts.clone(),
            _phantom_data: PhantomData,
        }
    }
}

impl<'obj, T, D> std::fmt::Debug for VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VariableList<T>")
            .field("num_space", &self.num_space)
            .field("num_set", &self.num_set)
            .field("elts", &self.elts)
            .finish()
    }
}
impl<'obj, T, D> Default for VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    fn default() -> Self {
        VariableList {
            num_space: 0,
            num_set: 0,
            elts: vec![],
            _phantom_data: PhantomData,
        }
    }
}

impl<'obj, T, D> VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    #[inline]
    pub fn init(num: usize) -> VariableList<'obj, T, D> {
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
impl<'obj, T, D> Index<usize> for VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.elts[idx]
    }
}

impl<'obj, T, D> RcSgaHeaderRepr<'obj, D> for VariableList<'obj, T, D>
where
    T: RcSgaHeaderRepr<'obj, D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
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
        scatter_gather_entries: &mut [RcSge<'sge, D>],
        offsets: &mut [usize],
    ) -> Result<()>
    where
        'obj: 'sge,
    {
        #[cfg(feature = "profiler")]
        demikernel::timer!("List inner serialize");

        {
            let mut forward_pointer = MutForwardPointer(header_buffer, constant_header_offset);
            forward_pointer.write_size(self.num_set as u32);
            forward_pointer.write_offset(dynamic_header_start as u32);
        }

        let mut sge_idx = 0;
        let mut cur_dynamic_off = dynamic_header_start + self.dynamic_header_start();
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
    fn inner_deserialize<'buf>(
        &mut self,
        buffer: &RcSge<'buf, D>,
        constant_offset: usize,
    ) -> Result<()>
    where
        'buf: 'obj,
    {
        let forward_pointer = ForwardPointer(buffer.addr(), constant_offset);
        let size = forward_pointer.get_size() as usize;
        let dynamic_offset = forward_pointer.get_offset() as usize;

        self.num_set = size;
        if self.elts.len() < size {
            self.elts.resize(size, T::default());
        }
        self.num_space = size;

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
