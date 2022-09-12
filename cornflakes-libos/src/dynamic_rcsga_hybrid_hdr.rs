use super::{
    datapath::{Datapath, MetadataOps, ReceivedPkt},
    ArenaDatapathSga, CopyContext, CopyContextRef,
};
use bitmaps::Bitmap;
use byteorder::{ByteOrder, LittleEndian};
use color_eyre::eyre::{Result, WrapErr};
use std::{default::Default, marker::PhantomData, ops::Index, slice::Iter, str};

#[cfg(feature = "profiler")]
use perftools;

#[inline]
pub fn write_size_and_offset(write_offset: usize, size: usize, offset: usize, buffer: &mut [u8]) {
    let mut forward_pointer = MutForwardPointer(buffer, write_offset);
    forward_pointer.write_size(size as u32);
    forward_pointer.write_offset(offset as u32);
}

#[inline]
pub fn read_size_and_offset<D>(
    offset: usize,
    buffer: &D::DatapathMetadata,
) -> Result<(usize, usize)>
where
    D: Datapath,
{
    let forward_pointer = ForwardPointer(buffer.as_ref(), offset);
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

pub trait HybridArenaRcSgaHdr<D>
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
    fn deserialize_bitmap<'buf>(&mut self, pkt: &D::DatapathMetadata, offset: usize) -> usize {
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
        <Self as HybridArenaRcSgaHdr<D>>::CONSTANT_HEADER_SIZE * (with_ref as usize)
            + self.dynamic_header_size()
    }

    /// Number of zero-copy scatter-gather entries (pointers and nested pointers to variable bytes or string
    /// fields that have passed the zero-copy heuristic).
    fn num_zero_copy_scatter_gather_entries(&self) -> usize;

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize;

    #[inline]
    fn is_list(&self) -> bool {
        false
    }

    #[inline]
    fn alloc_hdr(&self) -> Vec<u8> {
        vec![0u8; self.total_header_size(false, false)]
    }

    fn inner_serialize<'a>(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_entries: &mut [D::DatapathMetadata],
        zero_copy_offsets: &mut [usize],
    ) -> Result<()>;

    #[inline]
    fn serialize_into_arena_datapath_sga<'a>(
        &self,
        mut copy_context: CopyContext<'a, D>,
        arena: &'a bumpalo::Bump,
    ) -> Result<ArenaDatapathSga<'a, D>> {
        let mut owned_hdr = {
            let size = self.total_header_size(false, true);
            bumpalo::collections::Vec::with_capacity_zeroed_in(size, arena)
        };
        let header_buffer = owned_hdr.as_mut_slice();
        let num_zero_copy_entries = self.num_zero_copy_scatter_gather_entries();
        let mut zero_copy_entries = bumpalo::collections::Vec::from_iter_in(
            std::iter::repeat(D::DatapathMetadata::default()).take(num_zero_copy_entries),
            arena,
        );
        let mut zero_copy_offsets =
            bumpalo::collections::Vec::with_capacity_zeroed_in(num_zero_copy_entries, arena);

        // inner serialize
        self.inner_serialize(
            header_buffer,
            0,
            self.dynamic_header_start(),
            &mut copy_context,
            zero_copy_entries.as_mut_slice(),
            zero_copy_offsets.as_mut_slice(),
        )?;

        Ok(ArenaDatapathSga::new(
            copy_context,
            zero_copy_entries,
            zero_copy_offsets,
            owned_hdr,
        ))
    }

    fn inner_deserialize(&mut self, buf: &D::DatapathMetadata, header_offset: usize) -> Result<()>;

    #[inline]
    fn deserialize(&mut self, pkt: &ReceivedPkt<D>, offset: usize) -> Result<()> {
        // Right now, for deserialize we assume one contiguous buffer
        let metadata = pkt.seg(0);
        self.inner_deserialize(metadata, offset)?;
        Ok(())
    }
}

pub enum CFBytes<D>
where
    D: Datapath,
{
    /// Either directly references a segment for zero-copy
    RefCounted(D::DatapathMetadata),
    /// Or references the user provided copy context
    Copied(CopyContextRef<D>),
}

impl<D> Clone for CFBytes<D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            CFBytes::RefCounted(metadata) => CFBytes::RefCounted(metadata.clone()),
            CFBytes::Copied(copy_context_ref) => CFBytes::Copied(copy_context_ref.clone()),
        }
    }
}

impl<D> AsRef<[u8]> for CFBytes<D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            CFBytes::RefCounted(m) => m.as_ref(),
            CFBytes::Copied(copy_context_ref) => copy_context_ref.as_ref(),
        }
    }
}
impl<D> Default for CFBytes<D>
where
    D: Datapath,
{
    fn default() -> Self {
        CFBytes::RefCounted(D::DatapathMetadata::default())
    }
}

impl<D> std::fmt::Debug for CFBytes<D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CFBytes::RefCounted(metadata) => f
                .debug_struct("CFBytes zero-copy")
                .field("metadata", metadata)
                .finish(),
            CFBytes::Copied(copy_context_ref) => f
                .debug_struct("CFBytes copied")
                .field("metadata addr", &copy_context_ref.as_ref().as_ptr())
                .field("start", &copy_context_ref.offset())
                .field("len", &copy_context_ref.len())
                .finish(),
        }
    }
}

impl<D> CFBytes<D>
where
    D: Datapath,
{
    pub fn new<'a>(
        ptr: &[u8],
        datapath: &mut D,
        copy_context: &mut CopyContext<'a, D>,
    ) -> Result<Self> {
        if copy_context.should_copy(ptr) {
            let copy_context_ref = copy_context.copy(ptr, datapath)?;
            return Ok(CFBytes::Copied(copy_context_ref));
        }

        match datapath.recover_metadata(ptr)? {
            Some(m) => Ok(CFBytes::RefCounted(m)),
            None => Ok(CFBytes::Copied(copy_context.copy(ptr, datapath)?)),
        }
    }

    pub fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl<D> HybridArenaRcSgaHdr<D> for CFBytes<D>
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

    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        0
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        match self {
            CFBytes::RefCounted(_) => 1,
            CFBytes::Copied(_) => 0,
        }
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn check_deep_equality(&self, other: &CFBytes<D>) -> bool {
        self.len() == other.len() && self.as_ref().to_vec() == other.as_ref().to_vec()
    }

    #[inline]
    fn inner_serialize<'a>(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        zero_copy_offsets: &mut [usize],
    ) -> Result<()> {
        match self {
            CFBytes::RefCounted(metadata) => {
                zero_copy_scatter_gather_entries[0] = metadata.clone();
                zero_copy_offsets[0] = constant_header_offset;
            }
            CFBytes::Copied(copy_context_ref) => {
                // check the copy_context against the copy context ref
                copy_context.check(&copy_context_ref)?;
                // write in the offset and length into the correct location in the header buffer
                let offset_to_write = copy_context_ref.total_offset() + header_buffer.len();
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(copy_context_ref.len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
            }
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize(&mut self, buf: &D::DatapathMetadata, header_offset: usize) -> Result<()> {
        let mut new_metadata = buf.clone();
        let forward_pointer = ForwardPointer(buf.as_ref(), header_offset);
        new_metadata.set_data_len_and_offset(
            forward_pointer.get_size() as usize,
            forward_pointer.get_offset() as usize,
        )?;
        *self = CFBytes::RefCounted(new_metadata);
        Ok(())
    }
}

pub enum CFString<D>
where
    D: Datapath,
{
    /// Either directly references a segment for zero-copy
    RefCounted(D::DatapathMetadata),
    /// Or references the user provided copy context
    Copied(CopyContextRef<D>),
}

impl<D> Clone for CFString<D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            CFString::RefCounted(metadata) => CFString::RefCounted(metadata.clone()),
            CFString::Copied(copy_context_ref) => CFString::Copied(copy_context_ref.clone()),
        }
    }
}

impl<D> AsRef<[u8]> for CFString<D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            CFString::RefCounted(m) => m.as_ref(),
            CFString::Copied(copy_context_ref) => copy_context_ref.as_ref(),
        }
    }
}
impl<D> Default for CFString<D>
where
    D: Datapath,
{
    fn default() -> Self {
        CFString::RefCounted(D::DatapathMetadata::default())
    }
}

impl<D> std::fmt::Debug for CFString<D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CFString::RefCounted(metadata) => f
                .debug_struct("CFString zero-copy")
                .field("metadata", metadata)
                .finish(),
            CFString::Copied(copy_context_ref) => f
                .debug_struct("CFString copied")
                .field("metadata addr", &copy_context_ref.as_ref().as_ptr())
                .field("start", &copy_context_ref.offset())
                .field("len", &copy_context_ref.len())
                .finish(),
        }
    }
}

impl<D> CFString<D>
where
    D: Datapath,
{
    pub fn new<'a>(
        ptr: &[u8],
        datapath: &mut D,
        copy_context: &mut CopyContext<'a, D>,
    ) -> Result<Self> {
        if copy_context.should_copy(ptr) {
            let copy_context_ref = copy_context.copy(ptr, datapath)?;
            return Ok(CFString::Copied(copy_context_ref));
        }

        match datapath.recover_metadata(ptr)? {
            Some(m) => Ok(CFString::RefCounted(m)),
            None => Ok(CFString::Copied(copy_context.copy(ptr, datapath)?)),
        }
    }

    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    pub fn to_str(&self) -> Result<&str> {
        let slice = self.as_ref();
        let s = str::from_utf8(slice).wrap_err("Could not turn bytes into string")?;
        Ok(s)
    }

    pub fn to_string(&self) -> Result<String> {
        let s = self.to_str()?;
        Ok(s.to_string())
    }
}

impl<D> HybridArenaRcSgaHdr<D> for CFString<D>
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

    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        0
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        match self {
            CFString::RefCounted(_) => 1,
            CFString::Copied(_) => 0,
        }
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }

    fn check_deep_equality(&self, other: &CFString<D>) -> bool {
        self.len() == other.len() && self.as_ref().to_vec() == other.as_ref().to_vec()
    }

    #[inline]
    fn inner_serialize<'a>(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        zero_copy_offsets: &mut [usize],
    ) -> Result<()> {
        match self {
            CFString::RefCounted(metadata) => {
                zero_copy_scatter_gather_entries[0] = metadata.clone();
                zero_copy_offsets[0] = constant_header_offset;
            }
            CFString::Copied(copy_context_ref) => {
                // check the copy_context against the copy context ref
                copy_context.check(&copy_context_ref)?;
                // write in the offset and length into the correct location in the header buffer
                let offset_to_write = copy_context_ref.total_offset() + header_buffer.len();
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(copy_context_ref.len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
            }
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize(&mut self, buf: &D::DatapathMetadata, header_offset: usize) -> Result<()> {
        let mut new_metadata = buf.clone();
        let forward_pointer = ForwardPointer(buf.as_ref(), header_offset);
        new_metadata.set_data_len_and_offset(
            forward_pointer.get_size() as usize,
            forward_pointer.get_offset() as usize,
        )?;
        *self = CFString::RefCounted(new_metadata);
        Ok(())
    }
}

pub struct VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>, // TODO: maybe this should be a arena vec
    _phantom_data: PhantomData<D>,
}

impl<T, D> Clone for VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
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

impl<T, D> std::fmt::Debug for VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
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
impl<T, D> Default for VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
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

impl<T, D> VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    #[inline]
    pub fn init(num: usize) -> VariableList<T, D> {
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
impl<T, D> Index<usize> for VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.elts[idx]
    }
}

impl<T, D> HybridArenaRcSgaHdr<D> for VariableList<T, D>
where
    T: HybridArenaRcSgaHdr<D> + Clone + Default + std::fmt::Debug,
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
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|x| x.num_zero_copy_scatter_gather_entries())
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
    fn inner_serialize<'a>(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        zero_copy_offsets: &mut [usize],
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        perftools::timer!("List inner serialize");

        {
            let mut forward_pointer = MutForwardPointer(header_buffer, constant_header_offset);
            forward_pointer.write_size(self.num_set as u32);
            forward_pointer.write_offset(dynamic_header_start as u32);
        }

        let mut sge_idx = 0;
        let mut cur_dynamic_off = dynamic_header_start + self.dynamic_header_start();
        for (i, elt) in self.elts.iter().take(self.num_set).enumerate() {
            let required_sges = elt.num_zero_copy_scatter_gather_entries();
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
                    copy_context,
                    &mut zero_copy_scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                    &mut zero_copy_offsets[sge_idx..(sge_idx + required_sges)],
                )?;
            } else {
                elt.inner_serialize(
                    header_buffer,
                    dynamic_header_start + T::CONSTANT_HEADER_SIZE * i,
                    cur_dynamic_off,
                    copy_context,
                    &mut zero_copy_scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                    &mut zero_copy_offsets[sge_idx..(sge_idx + required_sges)],
                )?;
            }
            sge_idx += required_sges;
            cur_dynamic_off += elt.dynamic_header_size();
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        constant_offset: usize,
    ) -> Result<()> {
        let forward_pointer = ForwardPointer(buffer.as_ref(), constant_offset);
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
                let (_size, dynamic_off) = read_size_and_offset::<D>(
                    dynamic_offset + i * T::CONSTANT_HEADER_SIZE,
                    buffer,
                )?;
                elt.inner_deserialize(buffer, dynamic_off)?;
            }
        }
        Ok(())
    }
}
