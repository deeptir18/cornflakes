use super::{ForwardPointer, MutForwardPointer};
use bytes::{BufMut, BytesMut};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt},
    {OrderedSga, Sga, Sge},
};
use std::{
    convert::TryInto, default::Default, fmt::Debug, marker::PhantomData, mem::size_of, ops::Index,
    ptr, slice, str,
};

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;
/// u32 at beginning representing bitmap size in bytes
pub const BITMAP_LENGTH_FIELD: usize = 4;

pub trait HeaderRepr<'a> {
    /// Maximum number of fields is max u32 * 8
    const NUMBER_OF_FIELDS: usize;

    /// Constant part of the header: containing constant sized fields and pointers to variable
    /// sized fields. Does not include the bitmap.
    const CONSTANT_HEADER_SIZE: usize;

    /// Number of bytes reprenting the bitmap (should be multiple of 4)
    const BITMAP_LENGTH: u32;

    /// Dynamic part of the header (actual bytes pointed to, lists, nested objects).
    fn dynamic_header_size(&self) -> usize;

    /// Total header size including the bitmap.
    fn total_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + <Self as HeaderRepr<'a>>::BITMAP_LENGTH as usize
            + self.dynamic_header_size()
            + <Self as HeaderRepr<'a>>::CONSTANT_HEADER_SIZE
    }

    /// Number of scatter-gather entries (pointers and nested pointers to variable bytes or string
    /// fields).
    fn num_scatter_gather_entries(&self) -> usize;

    /// Offset to start writing dynamic parts of the header (e.g., variable sized list and nested
    /// object header data).
    fn dynamic_header_offset(&self) -> usize;

    /// Allocates context required for serialization: header vector and scatter-gather array with
    /// required capacity.
    fn alloc_context(&self) -> (Vec<u8>, OrderedSga<'a>) {
        (
            vec![0u8; self.total_header_size()],
            OrderedSga::allocate(self.num_scatter_gather_entries() + 1),
        )
    }

    /// Nested serialization function.
    /// Params:
    /// @header - mutable header bytes to write header.
    /// @offset - offset into array to start writing this part of header.
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
        offset: usize,
        scatter_gather_entries: &mut [Sge<'a>],
        offsets: &mut [usize],
    );

    /// Nested deserialization function.
    /// Params:
    /// @buffer - Buffer to deserialize. If this is a nested data structure,
    /// buffer is already at that offset and length.
    fn inner_deserialize(&mut self, buffer: &[u8]);

    /// Serialize with context of existing ordered sga and existing header buffer.
    fn serialize_into_sga<D>(
        &self,
        header_buffer: &'a mut [u8],
        ordered_sga: &'a mut OrderedSga<'a>,
        datapath: &D,
    ) -> Result<()>
    where
        D: Datapath,
    {
        let required_entries = self.num_scatter_gather_entries();
        let header_size = self.total_header_size();

        if ordered_sga.len() < (required_entries + 1) || header_buffer.len() < header_size {
            bail!("Cannot serialize into sga with num entries {} ({} required) and header buffer length {} ({} required", ordered_sga.len(), required_entries + 1, header_buffer.len(), header_size);
        }

        ordered_sga.set_length(required_entries);
        let mut offsets: Vec<usize> = vec![0; required_entries];

        // recursive serialize each item
        self.inner_serialize(
            header_buffer,
            0,
            ordered_sga.mut_entries_slice(1, required_entries),
            offsets.as_mut_slice(),
        );

        // reorder entries according to size threshold and whether entries are registered.
        ordered_sga.reorder_by_size_and_registration(datapath, &mut offsets);
        // reorder entries if current (zero-copy segments + 1) exceeds max zero-copy segments
        ordered_sga.reorder_by_max_segs(datapath.get_max_segments(), &mut offsets);

        let mut cur_dynamic_offset = self.dynamic_header_size();

        // iterate over header, writing in forward pointers based on new ordering
        for (sge, offset) in ordered_sga
            .entries_slice(1, required_entries)
            .iter()
            .zip(offsets.into_iter())
        {
            let slice = &mut header_buffer[offset..(offset + 8)].try_into()?;
            let mut obj_ref = MutForwardPointer(slice);
            obj_ref.write_size(sge.len() as u32);
            obj_ref.write_offset(cur_dynamic_offset as u32);
            cur_dynamic_offset += sge.len();
        }

        // replace head entry with object header buffer
        ordered_sga.replace(1, Sge::new(&header_buffer[0..header_size]));
        Ok(())
    }

    /// Deserialize contiguous buffer into this object.
    fn deserialize(&mut self, buffer: &[u8]) -> Result<()> {
        self.inner_deserialize(buffer);
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

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct CFBytes<'a> {
    pub ptr: &'a [u8],
}
