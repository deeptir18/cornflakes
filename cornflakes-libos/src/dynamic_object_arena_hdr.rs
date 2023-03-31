use super::{
    datapath::{Datapath, MetadataOps, ReceivedPkt},
    SerializationInfo,
};
use bitmaps::Bitmap;
use byteorder::{ByteOrder, LittleEndian};
use color_eyre::eyre::{Result, WrapErr};
use std::{default::Default, marker::PhantomData, ops::Index, slice::Iter};

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
pub trait CornflakesArenaObject<'arena, D>
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

    /// New 'default'
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized;

    fn get_bitmap_itermut(&mut self) -> std::slice::IterMut<Bitmap<32>> {
        [].iter_mut()
    }

    fn get_bitmap_iter(&self) -> std::slice::Iter<Bitmap<32>> {
        [].iter()
    }

    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unimplemented!();
    }

    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
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
    fn deserialize_bitmap(
        &mut self,
        pkt: &D::DatapathMetadata,
        offset: usize,
        buffer_offset: usize,
    ) -> usize {
        tracing::debug!(offset, buffer_offset, "In deserialize bitmap");
        let header = pkt.as_ref();
        let bitmap_size = LittleEndian::read_u32(
            &header[(buffer_offset + offset)..(buffer_offset + offset + BITMAP_LENGTH_FIELD)],
        );
        self.set_bitmap(
            (0..std::cmp::min(bitmap_size, Self::NUM_U32_BITMAPS as u32) as usize).map(|i| {
                let num = LittleEndian::read_u32(
                    &header[(buffer_offset + offset + BITMAP_LENGTH_FIELD + i * 4)
                        ..(buffer_offset + offset + BITMAP_LENGTH_FIELD + (i + 1) * 4)],
                );
                Bitmap::<32>::from_value(num)
            }),
        );
        bitmap_size as usize * 4
    }

    /// Checks serialized format represents bytes with the same fields.
    fn check_deep_equality(&self, other: &Self) -> bool;

    /// Dynamic part of header (for lists, nested objects).
    fn dynamic_header_size(&self) -> usize;

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize;

    /// Get header size and number of zero copy entries, and  copy length
    fn get_serialization_info(&self) -> SerializationInfo {
        let mut info = SerializationInfo::default();
        // TODO: can we recursively calculate the header size at the same time?
        info.header_size = self.total_header_size(false);
        self.modify_serialization_info_inner(&mut info);
        info
    }

    fn modify_serialization_info_inner(&self, info: &mut SerializationInfo);

    /// Total header size: constant (if referenced) + dynamic
    fn total_header_size(&self, with_ref: bool) -> usize {
        <Self as CornflakesArenaObject<'arena, D>>::CONSTANT_HEADER_SIZE * (with_ref as usize)
            + self.dynamic_header_size()
    }
    #[inline]
    fn is_list(&self) -> bool {
        false
    }

    fn iterate_over_entries<F>(
        &self,
        serialization_info: &SerializationInfo,
        header_buffer: &mut [u8],
        copy_data_buffer: &mut Option<&mut [u8]>,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_copy_offset: &mut usize,
        cur_zero_copy_offset: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<()>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>;

    fn inner_deserialize(
        &mut self,
        buf: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()>;

    #[inline]
    fn deserialize(
        &mut self,
        pkt: &ReceivedPkt<D>,
        offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        // Right now, for deserialize we assume one contiguous buffer
        let metadata = pkt.seg(0);
        self.inner_deserialize(metadata, 0, offset, arena)?;
        Ok(())
    }
}

pub enum CFBytes<'arena, D>
where
    D: Datapath,
{
    RefCounted(D::DatapathMetadata),
    Copied(bumpalo::collections::Vec<'arena, u8>),
}

impl<'arena, D> Clone for CFBytes<'arena, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            CFBytes::RefCounted(m) => CFBytes::RefCounted(m.clone()),
            CFBytes::Copied(vec) => CFBytes::Copied(vec.clone()),
        }
    }
}
impl<'arena, D> AsRef<[u8]> for CFBytes<'arena, D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            CFBytes::RefCounted(m) => m.as_ref(),
            CFBytes::Copied(vec) => vec.as_ref(),
        }
    }
}

impl<'arena, D> std::fmt::Debug for CFBytes<'arena, D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CFBytes::RefCounted(metadata) => f
                .debug_struct("CFBytes zero-copy")
                .field("metadata", metadata)
                .finish(),
            CFBytes::Copied(vec) => f
                .debug_struct("CFBytes copied")
                .field("copied bytes", &vec.as_slice())
                .field("len", &vec.len())
                .finish(),
        }
    }
}

impl<'arena, D> CFBytes<'arena, D>
where
    D: Datapath,
{
    pub fn new(ptr: &[u8], datapath: &mut D, arena: &'arena bumpalo::Bump) -> Result<Self> {
        if ptr.len() < datapath.get_copying_threshold() {
            let mut arr = bumpalo::collections::Vec::with_capacity_zeroed_in(ptr.len(), arena);
            arr.copy_from_slice(ptr);
            return Ok(CFBytes::Copied(arr));
        } else {
            match datapath.recover_metadata(ptr)? {
                Some(m) => Ok(CFBytes::RefCounted(m)),
                None => {
                    let mut arr =
                        bumpalo::collections::Vec::with_capacity_zeroed_in(ptr.len(), arena);
                    arr.copy_from_slice(ptr);
                    Ok(CFBytes::Copied(arr))
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CFBytes::Copied(vec) => vec.len(),
            CFBytes::RefCounted(m) => m.as_ref().len(),
        }
    }
}

impl<'arena, D> CornflakesArenaObject<'arena, D> for CFBytes<'arena, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    /// New 'default'
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        CFBytes::Copied(bumpalo::collections::Vec::new_in(arena))
    }

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unimplemented!();
    }
    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unimplemented!();
    }

    /// Checks serialized format represents bytes with the same fields.
    /// Should not be called on the critical path.
    fn check_deep_equality(&self, other: &Self) -> bool {
        self.len() == other.len() && self.as_ref().to_vec() == other.as_ref().to_vec()
    }

    /// Dynamic part of header (for lists, nested objects).
    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn modify_serialization_info_inner(&self, info: &mut SerializationInfo) {
        match self {
            CFBytes::RefCounted(m) => {
                info.num_zero_copy_entries += 1;
                info.zero_copy_length += m.as_ref().len();
            }
            CFBytes::Copied(vec) => {
                info.copy_length += vec.len();
            }
        }
    }

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize {
        0
    }

    #[inline]
    fn is_list(&self) -> bool {
        false
    }

    fn iterate_over_entries<F>(
        &self,
        serialization_info: &SerializationInfo,
        header_buffer: &mut [u8],
        copy_data_buffer: &mut Option<&mut [u8]>,
        constant_header_offset: usize,
        _dynamic_header_offset: usize,
        cur_copy_offset: &mut usize,
        cur_zero_copy_offset: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<()>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        match self {
            CFBytes::RefCounted(metadata) => {
                let object_len = metadata.as_ref().len();
                if object_len > 0 {
                    datapath_callback(&metadata, callback_state)?;
                }
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(object_len as u32);
                obj_ref.write_offset(
                    (*cur_zero_copy_offset
                        + serialization_info.header_size
                        + serialization_info.copy_length) as u32,
                );
                *cur_zero_copy_offset += object_len;
                tracing::debug!(len = object_len, ptr =? metadata.as_ref().as_ptr(), "Reached iterate over entries for cf bytes.");
            }
            CFBytes::Copied(vec) => {
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(vec.len() as u32);
                obj_ref.write_offset((*cur_copy_offset + serialization_info.header_size) as u32);
                tracing::debug!(
                    offset_to_write = *cur_copy_offset + serialization_info.header_size,
                    size = vec.len(),
                    "Reached inner serialize for cf bytes"
                );
                // copy actual bytes into copy offset .. (copy offset + data length)
                match copy_data_buffer {
                    Some(buf) => buf[*cur_copy_offset..(*cur_copy_offset + vec.len())]
                        .copy_from_slice(&vec.as_slice()),
                    None => header_buffer[(*cur_copy_offset + serialization_info.header_size)
                        ..(*cur_copy_offset + serialization_info.header_size + vec.len())]
                        .copy_from_slice(&vec.as_slice()),
                }
                *cur_copy_offset += vec.len();
            }
        }
        Ok(())
    }

    fn inner_deserialize(
        &mut self,
        buf: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        _arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        tracing::debug!(
            header_offset = header_offset,
            buffer_offset = buffer_offset,
            "In deserialize cf bytes"
        );
        let mut new_metadata = buf.clone();
        let forward_pointer = ForwardPointer(buf.as_ref(), header_offset + buffer_offset);
        let original_offset = buf.offset();
        new_metadata.set_data_len_and_offset(
            forward_pointer.get_size() as usize,
            forward_pointer.get_offset() as usize + original_offset + buffer_offset,
        )?;
        tracing::debug!("Deserialized cf bytes: {:?}", new_metadata);
        *self = CFBytes::RefCounted(new_metadata);
        Ok(())
    }
}

pub enum CFString<'arena, D>
where
    D: Datapath,
{
    RefCounted(D::DatapathMetadata),
    Copied(bumpalo::collections::Vec<'arena, u8>),
}

impl<'arena, D> Clone for CFString<'arena, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            CFString::RefCounted(m) => CFString::RefCounted(m.clone()),
            CFString::Copied(vec) => CFString::Copied(vec.clone()),
        }
    }
}

impl<'arena, D> AsRef<[u8]> for CFString<'arena, D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            CFString::RefCounted(m) => m.as_ref(),
            CFString::Copied(vec) => vec.as_ref(),
        }
    }
}

impl<'arena, D> std::fmt::Debug for CFString<'arena, D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CFString::RefCounted(metadata) => f
                .debug_struct("CFString zero-copy")
                .field("metadata", metadata)
                .finish(),
            CFString::Copied(vec) => f
                .debug_struct("CFString copied")
                .field("copied bytes", &vec.as_slice())
                .field("len", &vec.len())
                .finish(),
        }
    }
}

impl<'arena, D> CFString<'arena, D>
where
    D: Datapath,
{
    pub fn new(ptr: &[u8], datapath: &mut D, arena: &'arena bumpalo::Bump) -> Result<Self> {
        if ptr.len() < datapath.get_copying_threshold() {
            let mut arr = bumpalo::collections::Vec::with_capacity_zeroed_in(ptr.len(), arena);
            arr.copy_from_slice(ptr);
            return Ok(CFString::Copied(arr));
        } else {
            match datapath.recover_metadata(ptr)? {
                Some(m) => Ok(CFString::RefCounted(m)),
                None => {
                    let mut arr =
                        bumpalo::collections::Vec::with_capacity_zeroed_in(ptr.len(), arena);
                    arr.copy_from_slice(ptr);
                    Ok(CFString::Copied(arr))
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CFString::Copied(vec) => vec.len(),
            CFString::RefCounted(m) => m.as_ref().len(),
        }
    }

    pub fn to_str(&self) -> Result<&str> {
        let slice = self.as_ref();
        tracing::debug!("Slice: {:?}", slice);
        let s = std::str::from_utf8(slice)
            .wrap_err("Could not turn bytes into string")?
            .trim_end();
        Ok(s)
    }

    pub fn to_string(&self) -> Result<String> {
        let s = self.to_str()?;
        Ok(s.to_string())
    }
}

impl<'arena, D> CornflakesArenaObject<'arena, D> for CFString<'arena, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    /// New 'default'
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        CFString::Copied(bumpalo::collections::Vec::new_in(arena))
    }

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unimplemented!();
    }
    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unimplemented!();
    }

    /// Checks serialized format represents bytes with the same fields.
    /// Should not be called on the critical path.
    fn check_deep_equality(&self, other: &Self) -> bool {
        self.len() == other.len() && self.as_ref().to_vec() == other.as_ref().to_vec()
    }

    /// Dynamic part of header (for lists, nested objects).
    fn dynamic_header_size(&self) -> usize {
        0
    }

    fn modify_serialization_info_inner(&self, info: &mut SerializationInfo) {
        match self {
            CFString::RefCounted(m) => {
                info.num_zero_copy_entries += 1;
                info.zero_copy_length += m.as_ref().len();
            }
            CFString::Copied(vec) => {
                info.copy_length += vec.len();
            }
        }
    }

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize {
        0
    }

    #[inline]
    fn is_list(&self) -> bool {
        false
    }

    fn iterate_over_entries<F>(
        &self,
        serialization_info: &SerializationInfo,
        header_buffer: &mut [u8],
        copy_data_buffer: &mut Option<&mut [u8]>,
        constant_header_offset: usize,
        _dynamic_header_offset: usize,
        cur_copy_offset: &mut usize,
        cur_zero_copy_offset: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<()>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        match self {
            CFString::RefCounted(metadata) => {
                let object_len = metadata.as_ref().len();
                if object_len > 0 {
                    datapath_callback(&metadata, callback_state)?;
                }
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(object_len as u32);
                obj_ref.write_offset(
                    (*cur_zero_copy_offset
                        + serialization_info.header_size
                        + serialization_info.copy_length) as u32,
                );
                *cur_zero_copy_offset += object_len;
                tracing::debug!(len = object_len, ptr =? metadata.as_ref().as_ptr(), "Reached iterate over entries for cf bytes.");
            }
            CFString::Copied(vec) => {
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(vec.len() as u32);
                obj_ref.write_offset((*cur_copy_offset + serialization_info.header_size) as u32);
                tracing::debug!(
                    offset_to_write = *cur_copy_offset + serialization_info.header_size,
                    size = vec.len(),
                    "Reached inner serialize for cf bytes"
                );
                match copy_data_buffer {
                    Some(buf) => buf[*cur_copy_offset..(*cur_copy_offset + vec.len())]
                        .copy_from_slice(&vec.as_slice()),
                    None => header_buffer[(*cur_copy_offset + serialization_info.header_size)
                        ..(*cur_copy_offset + serialization_info.header_size + vec.len())]
                        .copy_from_slice(&vec.as_slice()),
                }
                *cur_copy_offset += vec.len();
            }
        }
        Ok(())
    }

    fn inner_deserialize(
        &mut self,
        buf: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        _arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        tracing::debug!(
            header_offset = header_offset,
            buffer_offset = buffer_offset,
            "In deserialize cf bytes"
        );
        let mut new_metadata = buf.clone();
        let forward_pointer = ForwardPointer(buf.as_ref(), header_offset + buffer_offset);
        let original_offset = buf.offset();
        new_metadata.set_data_len_and_offset(
            forward_pointer.get_size() as usize,
            forward_pointer.get_offset() as usize + original_offset + buffer_offset,
        )?;
        tracing::debug!("Deserialized cf bytes: {:?}", new_metadata);
        *self = CFString::RefCounted(new_metadata);
        Ok(())
    }
}

pub struct VariableList<'arena, T, D>
where
    T: CornflakesArenaObject<'arena, D> + Clone + std::fmt::Debug,
    D: Datapath,
{
    num_space: usize,
    num_set: usize,
    elts: bumpalo::collections::Vec<'arena, T>,
    _phantom_data: PhantomData<D>,
}

impl<'arena, T, D> Clone for VariableList<'arena, T, D>
where
    T: CornflakesArenaObject<'arena, D> + Clone + std::fmt::Debug,
    D: Datapath,
{
    fn clone(&self) -> Self {
        VariableList {
            num_space: self.num_space,
            num_set: self.num_set,
            elts: self.elts.clone(),
            _phantom_data: PhantomData::default(),
        }
    }
}

impl<'arena, T, D> std::fmt::Debug for VariableList<'arena, T, D>
where
    T: CornflakesArenaObject<'arena, D> + Clone + std::fmt::Debug,
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VariableList")
            .field("num_space", &self.num_space)
            .field("num_set", &self.num_set)
            .field("elts", &self.elts)
            .finish()
    }
}

impl<'arena, T, D> VariableList<'arena, T, D>
where
    T: CornflakesArenaObject<'arena, D> + Clone + std::fmt::Debug,
    D: Datapath,
{
    pub fn init(num: usize, arena: &'arena bumpalo::Bump) -> VariableList<'arena, T, D> {
        let entries = bumpalo::collections::Vec::from_iter_in(
            std::iter::repeat(T::new_in(arena)).take(num),
            arena,
        );

        VariableList {
            num_space: num,
            num_set: 0,
            elts: entries,
            _phantom_data: PhantomData,
        }
    }

    #[inline]
    pub fn iter(&self) -> std::iter::Take<Iter<T>> {
        self.elts.iter().take(self.num_set)
    }

    #[inline]
    pub fn append(&mut self, val: T) {
        if self.elts.len() == self.num_set {
            self.elts.push(val);
        } else {
            self.elts[self.num_set] = val;
        }
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

impl<'arena, T, D> Index<usize> for VariableList<'arena, T, D>
where
    T: CornflakesArenaObject<'arena, D> + Clone + std::fmt::Debug,
    D: Datapath,
{
    type Output = T;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.elts[idx]
    }
}

impl<'arena, T, D> CornflakesArenaObject<'arena, D> for VariableList<'arena, T, D>
where
    T: CornflakesArenaObject<'arena, D> + Clone + std::fmt::Debug,
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = OFFSET_FIELD + SIZE_FIELD;

    const NUMBER_OF_FIELDS: usize = 1;

    const NUM_U32_BITMAPS: usize = 0;

    /// New 'default'
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        VariableList {
            num_space: 0,
            num_set: 0,
            elts: bumpalo::collections::Vec::new_in(arena),
            _phantom_data: PhantomData,
        }
    }

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unimplemented!();
    }
    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unimplemented!();
    }

    /// Checks serialized format represents bytes with the same fields.
    /// Should not be called on the critical path.
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

    /// Dynamic part of header (for lists, nested objects).
    fn dynamic_header_size(&self) -> usize {
        self.elts
            .iter()
            .map(|x| x.dynamic_header_size() + T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    fn modify_serialization_info_inner(&self, info: &mut SerializationInfo) {
        for i in 0..self.len() {
            self[i].modify_serialization_info_inner(info);
        }
    }

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_start(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|_x| T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    #[inline]
    fn is_list(&self) -> bool {
        true
    }

    fn iterate_over_entries<F>(
        &self,
        serialization_info: &SerializationInfo,
        header_buffer: &mut [u8],
        copy_data_buffer: &mut Option<&mut [u8]>,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_copy_offset: &mut usize,
        cur_zero_copy_offset: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<()>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        {
            let mut forward_pointer = MutForwardPointer(header_buffer, constant_header_offset);
            forward_pointer.write_size(self.num_set as u32);
            forward_pointer.write_offset(dynamic_header_offset as u32);
        }

        tracing::debug!(
            num_set = self.num_set,
            dynamic_offset = dynamic_header_offset,
            num_set = self.num_set,
            "Writing in forward pointer at position {}",
            constant_header_offset
        );

        let mut cur_dynamic_off = dynamic_header_offset + self.dynamic_header_start();
        for (i, elt) in self.elts.iter().take(self.num_set).enumerate() {
            if elt.dynamic_header_size() != 0 {
                let mut forward_offset = MutForwardPointer(
                    header_buffer,
                    dynamic_header_offset + T::CONSTANT_HEADER_SIZE * i,
                );
                forward_offset.write_size(elt.dynamic_header_size() as u32);
                forward_offset.write_offset(cur_dynamic_off as u32);
                elt.iterate_over_entries(
                    &serialization_info,
                    header_buffer,
                    copy_data_buffer,
                    cur_dynamic_off,
                    cur_dynamic_off + elt.dynamic_header_start(),
                    cur_copy_offset,
                    cur_zero_copy_offset,
                    datapath_callback,
                    callback_state,
                )?;
            } else {
                tracing::debug!(
                    constant = dynamic_header_offset + T::CONSTANT_HEADER_SIZE * i,
                    "Calling inner serialize recursively in list inner serialize"
                );
                elt.iterate_over_entries(
                    &serialization_info,
                    header_buffer,
                    copy_data_buffer,
                    dynamic_header_offset + T::CONSTANT_HEADER_SIZE * i,
                    cur_dynamic_off,
                    cur_copy_offset,
                    cur_zero_copy_offset,
                    datapath_callback,
                    callback_state,
                )?;
            }
            cur_dynamic_off += elt.dynamic_header_size();
        }

        Ok(())
    }

    fn inner_deserialize(
        &mut self,
        buf: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let forward_pointer = ForwardPointer(buf.as_ref(), header_offset + buffer_offset);
        let size = forward_pointer.get_size() as usize;
        let dynamic_offset = forward_pointer.get_offset() as usize;

        self.num_set = size;
        if self.elts.len() < size {
            self.elts.resize(size, T::new_in(arena));
        }
        self.num_space = size;

        for (i, elt) in self.elts.iter_mut().take(size).enumerate() {
            if elt.dynamic_header_size() == 0 {
                elt.inner_deserialize(
                    buf,
                    dynamic_offset + i * T::CONSTANT_HEADER_SIZE,
                    buffer_offset,
                    arena,
                )?;
            } else {
                let (_size, dynamic_off) =
                    read_size_and_offset::<D>(dynamic_offset + i * T::CONSTANT_HEADER_SIZE, buf)?;
                elt.inner_deserialize(buf, dynamic_off, buffer_offset, arena)?;
            }
        }
        Ok(())
    }
}
