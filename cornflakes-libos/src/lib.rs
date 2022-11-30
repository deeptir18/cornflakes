//! Welcome to cornflakes!
//!
//! This crate, cornflakes-libos, implements the networking layer for cornflakes.
//! This includes:
//!  1. An interface for datapaths to implement.
//!  2. DPDK bindings, which are used to implement the DPDK datapath.
//!  3. A DPDK based datapath.
pub mod allocator;
pub mod datapath;
pub mod dynamic_rcsga_hdr;
pub mod dynamic_rcsga_hybrid_hdr;
pub mod dynamic_sga_hdr;
pub mod loadgen;
pub mod mem;
pub mod state_machine;
pub mod timing;
pub mod utils;

use byteorder::{ByteOrder, LittleEndian};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::AppMode;
use datapath::MetadataOps;
#[cfg(feature = "profiler")]
use demikernel::perftools;
use loadgen::request_schedule::{PacketSchedule, SpinTimer};
use mem::MmapMetadata;
use std::{
    io::Write,
    net::Ipv4Addr,
    ops::{Fn, FnMut},
    slice::{Iter, IterMut},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use timing::HistogramWrapper;
use utils::AddressInfo;
#[cfg(feature = "profiler")]
const PROFILER_DEPTH: usize = 10;

pub static mut USING_REF_COUNTING: bool = true;

pub type MsgID = u32;
pub type ConnID = usize;

pub static MAX_SCATTER_GATHER_ENTRIES: usize = 32;

pub fn turn_off_ref_counting() {
    unsafe {
        USING_REF_COUNTING = false;
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

/// Trait defining functionality any ``scatter-gather'' types should have,
/// that datapaths can access or modify about these types,
/// to be able to be able to send scattered memory packets, or receive datapath packets.
/// For transmitting packets, most systems can just use the Cornflake struct, which implements this
/// trait.
pub trait ScatterGather {
    /// Pointer type, to refer to scattered memory segments
    type Ptr: AsRef<[u8]> + PtrAttributes;

    /// A collection type over the pointer type.
    type Collection: IntoIterator<Item = Self::Ptr>;

    /// Returns ID field for this packet.
    /// Should be unique per connection.
    fn get_id(&self) -> MsgID;

    /// Sets the id field for this packet.
    fn set_id(&mut self, id: MsgID);

    /// Returns total number of memory regions represented by this packet.
    fn num_segments(&self) -> usize;

    /// Returns total number of borrowed memory regions represented by this packet.
    fn num_borrowed_segments(&self) -> usize;

    /// Amount of data in total represented by this packet.
    fn data_len(&self) -> usize;

    /// Returns an iterator over the scattered memory regions this packet represents.
    fn collection(&self) -> Self::Collection;

    /// Returns item at index
    fn index(&self, idx: usize) -> &Self::Ptr;

    /// Applies the provided closure on all of the pointer types.
    fn iter_apply(&self, consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()>;

    /// Returns a buffer where all the scatter-gather entries are copied into a contiguous array.
    fn contiguous_repr(&self) -> Vec<u8>;

    /// Replaces an index with another element.
    fn replace(&mut self, idx: usize, entry: Self::Ptr);
}

/// Whether an underlying buffer is borrowed or
/// actually owned (most likely on the heap).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CornType {
    Registered,
    Normal,
}

/// Must be implemented by any Ptr type referred to in the ScatterGather trait.
pub trait PtrAttributes {
    fn buf_type(&self) -> CornType;
    fn buf_size(&self) -> usize;
}

/// Represents either a borrowed piece of memory.
/// Or an owned value.
/// TODO: having this be an enum might double storage necessary for IOvecs
#[derive(Clone, PartialEq, Eq, Copy)]
pub enum CornPtr<'registered, 'normal> {
    /// Reference to some other memory (used for zero-copy send).
    Registered(&'registered [u8]),
    /// "Normal" Reference to un-registered memory.
    Normal(&'normal [u8]),
}

impl<'registered, 'normal> Default for CornPtr<'registered, 'normal> {
    fn default() -> Self {
        CornPtr::Normal(&[])
    }
}

impl<'registered, 'normal> AsRef<[u8]> for CornPtr<'registered, 'normal> {
    fn as_ref(&self) -> &[u8] {
        match self {
            CornPtr::Registered(buf) => buf,
            CornPtr::Normal(buf) => buf,
        }
    }
}

impl<'registered, 'normal> PtrAttributes for CornPtr<'registered, 'normal> {
    fn buf_type(&self) -> CornType {
        match self {
            CornPtr::Registered(_) => CornType::Registered,
            CornPtr::Normal(_) => CornType::Normal,
        }
    }

    fn buf_size(&self) -> usize {
        match self {
            CornPtr::Registered(buf) => buf.len(),
            CornPtr::Normal(buf) => buf.len(),
        }
    }
}

/// A Cornflake represents a general-purpose scatter-gather array.
/// Datapaths must be able to send and receive cornflakes.
/// TODO: might not be necessary to separately keep track of lengths.
#[derive(Clone, Eq, PartialEq)]
pub struct Cornflake<'registered, 'normal> {
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<CornPtr<'registered, 'normal>>,
    /// Num borrowed segments
    num_borrowed: usize,
    /// Data size
    data_size: usize,
    /// Number of total filled entries
    num_filled: usize,
}

impl<'registered, 'normal> Default for Cornflake<'registered, 'normal> {
    fn default() -> Self {
        Cornflake {
            id: 0,
            entries: Vec::new(),
            num_borrowed: 0,
            data_size: 0,
            num_filled: 0,
        }
    }
}

impl<'registered, 'normal> ScatterGather for Cornflake<'registered, 'normal> {
    /// Pointer type is reference to CornPtr.
    type Ptr = CornPtr<'registered, 'normal>;
    /// Can return an iterator over CornPtr references.
    type Collection = Vec<CornPtr<'registered, 'normal>>;

    /// Returns the id of this cornflake.
    fn get_id(&self) -> MsgID {
        self.id
    }

    /// Sets the id.
    fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    /// Returns number of entries in this cornflake.
    fn num_segments(&self) -> usize {
        self.num_filled
    }

    /// Returns the number of borrowed memory regions in this cornflake.
    fn num_borrowed_segments(&self) -> usize {
        self.num_borrowed
    }

    /// Amount of data represented by this scatter-gather array.
    fn data_len(&self) -> usize {
        self.data_size
    }

    /// Exposes an iterator over the entries in the scatter-gather array.
    fn collection(&self) -> Self::Collection {
        let mut vec = Vec::new();
        for i in 0..self.num_filled {
            vec.push(self.entries[i].clone());
        }
        vec
    }

    /// Returns CornPtr at Index
    fn index(&self, idx: usize) -> &Self::Ptr {
        &self.entries[idx]
    }

    /// Apply an iterator to entries of the scatter-gather array, without consuming the
    /// scatter-gather array.
    fn iter_apply(&self, mut consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()> {
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            consume_element(entry).wrap_err(format!(
                "Unable to run function on pointer {} in cornflake",
                i
            ))?;
        }
        Ok(())
    }

    fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            ret.extend_from_slice(entry.as_ref());
        }
        ret
    }

    fn replace(&mut self, index: usize, entry: CornPtr<'registered, 'normal>) {
        assert!(index <= self.num_filled);
        self.entries[index] = entry;
    }
}

impl<'registered, 'normal> Cornflake<'registered, 'normal> {
    /// Returns a cornflake with this many entries.
    pub fn with_capacity(capacity: usize) -> Cornflake<'registered, 'normal> {
        Cornflake {
            id: 0,
            entries: vec![CornPtr::default(); capacity],
            num_borrowed: 0,
            data_size: 0,
            num_filled: 0,
        }
    }

    /// Adds a scatter-gather entry to this cornflake.
    /// Passes ownership of the CornPtr.
    /// Arguments:
    /// * ptr - CornPtr<'a> representing owned or borrowed memory.
    /// * length - usize representing length of memory region.
    pub fn add_entry(&mut self, ptr: CornPtr<'registered, 'normal>) {
        self.data_size += ptr.buf_size();
        if ptr.buf_type() == CornType::Registered {
            self.num_borrowed += 1;
        }
        if self.num_filled == self.entries.len() {
            self.entries.push(ptr);
        } else {
            self.entries[self.num_filled] = ptr;
        }
        self.num_filled += 1;
    }

    pub fn iter(&self) -> Iter<CornPtr<'registered, 'normal>> {
        self.entries.iter()
    }

    pub fn get(&self, idx: usize) -> Result<&CornPtr<'registered, 'normal>> {
        ensure!(
            idx >= self.entries.len(),
            "Trying to get {} idx from CornPtr, but it only has length {}.",
            idx,
            self.entries.len()
        );
        Ok(&self.entries[idx])
    }
}

/// cbindgen:field-names=[addr, len]
#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(C)]
pub struct CSge(pub *mut ::std::os::raw::c_void, pub usize);

impl CSge {
    pub fn to_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.0 as _, self.1) }
    }
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct Sge<'a> {
    addr: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct Sga<'a> {
    /// entries underlying the scatter-gather array.
    entries: Vec<Sge<'a>>,
    /// Actual length. Underlying sg array may have more capacity than current length.
    length: usize,
}

impl<'a> Sge<'a> {
    pub fn new(addr: &'a [u8]) -> Self {
        Sge { addr: addr }
    }

    #[inline]
    pub fn addr(&self) -> &'a [u8] {
        self.addr
    }

    pub fn len(&self) -> usize {
        self.addr.len()
    }
}

impl<'a> Sga<'a> {
    pub fn with_capacity(size: usize) -> Self {
        Sga {
            entries: Vec::with_capacity(size),
            length: 0,
        }
    }

    pub fn allocate(size: usize) -> Self {
        let entries = vec![Sge::default(); size];
        Sga {
            entries: entries,
            length: 0,
        }
    }

    pub fn from_static_array(
        &mut self,
        addrs: &[*mut ::std::os::raw::c_void; 32],
        sizes: &[usize; 32],
        num_entries: usize,
    ) {
        for (i, entry) in self.entries.iter_mut().take(num_entries).enumerate() {
            let slice = unsafe { std::slice::from_raw_parts(addrs[i] as *mut u8, sizes[i]) };
            let sge = Sge::new(slice);
            *entry = sge;
        }
        self.length = num_entries;
    }

    pub fn with_entries(entries: Vec<Sge<'a>>) -> Self {
        Sga {
            length: entries.len(),
            entries: entries,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    pub fn flatten(&self) -> Vec<u8> {
        self.entries
            .iter()
            .take(self.length)
            .map(|bytes| bytes.addr().to_vec())
            .flatten()
            .collect()
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        self.entries.iter().take(self.length).map(|e| e.len()).sum()
    }

    pub fn set_length(&mut self, length: usize) {
        self.length = length;
    }

    #[inline]
    pub fn get(&self, idx: usize) -> &Sge<'a> {
        &self.entries[idx]
    }

    #[inline]
    pub fn iter(&self) -> std::iter::Take<Iter<Sge<'a>>> {
        self.entries.iter().take(self.length)
    }

    pub fn entries_slice(&self, start: usize, length: usize) -> &[Sge<'a>] {
        &self.entries.as_slice()[start..(start + length)]
    }

    pub fn mut_entries_slice(&mut self, start: usize, length: usize) -> &mut [Sge<'a>] {
        &mut self.entries.as_mut_slice()[start..(start + length)]
    }

    pub fn swap(&mut self, a: usize, b: usize) {
        self.entries.swap(a, b);
    }

    #[inline]
    pub fn add_entry(&mut self, entry: Sge<'a>) {
        if self.length < self.entries.len() {
            self.replace(self.length, entry);
            self.length += 1;
        } else {
            self.entries.push(entry);
            self.length += 1;
        }
    }

    pub fn replace(&mut self, idx: usize, entry: Sge<'a>) {
        self.entries[idx] = entry;
    }

    pub fn clear(&mut self) {
        for entry in self.entries.iter_mut().take(self.length) {
            *entry = Sge::new(&[]);
        }
        self.length = 0;
    }

    pub fn capacity(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct OrderedSga<'a> {
    /// Underlying scatter-gather array, where entries have been ordered according to heuristics or will be ordered.
    sga: Sga<'a>,
    /// Number of sg entries that will be sent as "copy".
    num_copy_entries: usize,
    /// Bytes: meant for header
    hdr: Vec<u8>,
}

impl<'a> OrderedSga<'a> {
    pub fn new(sga: Sga<'a>, num_copy_entries: usize) -> OrderedSga<'a> {
        OrderedSga {
            sga: sga,
            num_copy_entries: num_copy_entries,
            hdr: Vec::default(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.sga.capacity()
    }

    pub fn clear(&mut self) {
        self.sga.clear();
        self.num_copy_entries = 0;
    }

    pub fn flatten(&self) -> Vec<u8> {
        let mut buf = self.hdr.clone();
        buf.append(&mut self.sga.flatten());
        buf
    }

    pub fn allocate(size: usize) -> OrderedSga<'a> {
        OrderedSga {
            sga: Sga::allocate(size),
            num_copy_entries: 0,
            hdr: Vec::default(),
        }
    }

    pub fn set_hdr(&mut self, vec: Vec<u8>) {
        self.hdr = vec;
    }

    pub fn get_hdr(&self) -> &[u8] {
        self.hdr.as_slice()
    }

    pub fn set_num_copy_entries(&mut self, num: usize) {
        self.num_copy_entries = num;
    }

    pub fn num_copy_entries(&self) -> usize {
        self.num_copy_entries
    }

    pub fn num_zero_copy_entries(&self) -> usize {
        self.sga.len() - self.num_copy_entries
    }

    pub fn sga(&self) -> &Sga {
        &self.sga
    }

    pub fn add_entry(&mut self, entry: Sge<'a>) {
        self.sga.add_entry(entry);
    }

    pub fn entries_slice(&self, start: usize, length: usize) -> &[Sge<'a>] {
        self.sga.entries_slice(start, length)
    }

    pub fn copy_into_buffer(&self, buf: &mut [u8]) -> Result<usize> {
        let mut off = 0;
        for entry in self.sga.entries_slice(0, self.sga.len()).iter() {
            tracing::debug!("Copying entry into [{}, {}]", off, off + entry.len());
            let buf_to_copy = &mut buf[off..(off + entry.len())];
            buf_to_copy.copy_from_slice(&entry.addr());
            off += entry.len();
        }
        Ok(off)
    }

    pub fn mut_entries_slice(&mut self, start: usize, length: usize) -> &mut [Sge<'a>] {
        self.sga.mut_entries_slice(start, length)
    }

    pub fn set_length(&mut self, length: usize) {
        self.sga.set_length(length);
    }

    pub fn replace(&mut self, idx: usize, entry: Sge<'a>) {
        self.sga.replace(idx, entry);
    }

    pub fn len(&self) -> usize {
        self.sga.len()
    }

    pub fn copy_length(&self) -> usize {
        let size: usize = self
            .sga
            .iter()
            .take(self.num_copy_entries)
            .map(|seg| seg.len())
            .sum();
        size + self.hdr.len()
    }

    #[inline]
    fn is_zero_copy_seg<D>(&self, i: usize, d: &D) -> bool
    where
        D: datapath::Datapath,
    {
        let seg = self.sga.get(i).addr();
        seg.len() >= d.get_copying_threshold() && d.is_registered(seg)
    }

    /// Reorders the scatter-gather array according to size heuristics, from the first entry
    /// forward,
    /// setting the number of copy entries to be the number of entries found under the
    /// threshold or not registered.
    /// Parameters:
    /// @datapath: Datapath handle providing function to check current size threshold, and whether
    /// buffers are registered.
    /// @offsets: Corresponding offsets vector (where indices correspond to current entries in the
    /// scatter-gather list). When indices are swapped in sga, corresponding indices must be
    /// swapped in offsets (assumes self.length == offsets.len() + 1)
    #[inline]
    pub fn reorder_by_size_and_registration<D>(
        &mut self,
        datapath: &D,
        offsets: &mut Vec<usize>,
    ) -> Result<()>
    where
        D: datapath::Datapath,
    {
        if self.sga().len() != offsets.len() {
            bail!("passed in offsets array should be one less than sga length");
        }
        // reorder scatter-gather entries

        if self.sga.len() == 1 {
            if !self.is_zero_copy_seg(0, datapath) {
                self.num_copy_entries = 1;
            }
            tracing::debug!("Setting num copy entries as {}/1", self.num_copy_entries);
            return Ok(());
        }

        let mut forward_index = 0;
        let mut end_index = self.sga.len() - 1;
        let mut switch_forward = self.is_zero_copy_seg(forward_index, datapath);
        let mut switch_backward = !self.is_zero_copy_seg(end_index, datapath);
        let mut num_copy_segs = 0; // copy object header segment
        let mut ct = 0;

        // everytime forward index is advanced, we record a copy segment
        while !(forward_index >= end_index) {
            tracing::debug!(
                forward_index,
                end_index,
                switch_forward,
                switch_backward,
                "Looping over reordering segments"
            );
            ct += 1;
            match (switch_forward, switch_backward) {
                (true, true) => {
                    num_copy_segs += 1;
                    self.sga.swap(forward_index, end_index);
                    offsets.swap(forward_index, end_index);
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
                (true, false) => {
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
                (false, true) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                }
                (false, false) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
            }
        }
        self.num_copy_entries = num_copy_segs;
        tracing::debug!(
            num_copy_segs = self.num_copy_entries,
            loop_ct = ct,
            "DOne with reordering"
        );
        Ok(())
    }

    /// Reorder by max segments.
    /// Max segments includes the 1 segment for all entries that will be copied together
    pub fn reorder_by_max_segs<D>(&mut self, datapath: &D, offsets: &mut Vec<usize>) -> Result<()>
    where
        D: datapath::Datapath,
    {
        if self.sga().len() != offsets.len() {
            bail!("passed in offsets array should be one less than sga length");
        }
        let current_zero_copy_segs = self.num_zero_copy_entries();
        let zero_copy_limit = datapath.get_max_segments() - 1;

        // already under maximum segments
        if current_zero_copy_segs <= zero_copy_limit {
            return Ok(());
        }

        let sga_len = self.sga.len();
        let segs_to_order = self
            .sga
            .mut_entries_slice(self.num_copy_entries, current_zero_copy_segs);

        let extra_to_copy = current_zero_copy_segs - zero_copy_limit;

        if extra_to_copy < zero_copy_limit {
            // move smallest entries to the front
            for i in self.num_copy_entries..(self.num_copy_entries + extra_to_copy) {
                let mut min_index = i;
                let mut min_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[i + 1..].iter().enumerate() {
                    if entry.len() < min_length {
                        min_length = entry.len();
                        min_index = entry_idx;
                    }
                }

                if min_index != i {
                    segs_to_order.swap(i, min_index);
                    offsets.swap(i, min_index);
                }
            }
        } else {
            // move max (zero_copy limit) segs to back
            for i in ((sga_len - zero_copy_limit)..sga_len).rev() {
                let mut max_index = i;
                let mut max_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[(sga_len - zero_copy_limit)..(i - 1)]
                    .iter()
                    .rev()
                    .enumerate()
                {
                    if entry.len() > max_length {
                        max_length = entry.len();
                        max_index = entry_idx;
                    }
                }

                if max_index != i {
                    segs_to_order.swap(i, max_index);
                    offsets.swap(i, max_index);
                }
            }
        }

        self.num_copy_entries += extra_to_copy;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct OrderedRcSga<'a, D>
where
    D: datapath::Datapath,
{
    /// Underlying scatter-gather array, where entries have been ordered according to heuristics or will be ordered.
    sga: RcSga<'a, D>,
    /// Number of sg entries that will be sent as "copy".
    num_copy_entries: usize,
    /// Bytes: meant for header
    hdr: Vec<u8>,
}

impl<'a, D> OrderedRcSga<'a, D>
where
    D: datapath::Datapath,
{
    pub fn new(sga: Sga<'a>, num_copy_entries: usize) -> OrderedSga<'a> {
        OrderedSga {
            sga: sga,
            num_copy_entries: num_copy_entries,
            hdr: Vec::default(),
        }
    }

    pub fn data_len(&self) -> usize {
        self.sga.data_len() + self.hdr.len()
    }

    pub fn iter(&self) -> Iter<RcSge<'a, D>> {
        self.sga.iter()
    }

    pub fn capacity(&self) -> usize {
        self.sga.capacity()
    }

    pub fn clear(&mut self) {
        self.sga.clear();
        self.num_copy_entries = 0;
    }

    pub fn flatten(&self) -> Vec<u8> {
        let mut buf = self.hdr.clone();
        buf.append(&mut self.sga.flatten());
        buf
    }

    pub fn allocate(size: usize) -> OrderedRcSga<'a, D> {
        OrderedRcSga {
            sga: RcSga::allocate(size),
            num_copy_entries: 0,
            hdr: Vec::default(),
        }
    }

    pub fn set_hdr(&mut self, vec: Vec<u8>) {
        self.hdr = vec;
    }

    pub fn get_hdr(&self) -> &[u8] {
        self.hdr.as_slice()
    }

    pub fn set_num_copy_entries(&mut self, num: usize) {
        self.num_copy_entries = num;
    }

    pub fn num_copy_entries(&self) -> usize {
        self.num_copy_entries
    }

    pub fn num_zero_copy_entries(&self) -> usize {
        self.sga.len() - self.num_copy_entries
    }

    pub fn sga(&self) -> &RcSga<D> {
        &self.sga
    }

    pub fn add_entry(&mut self, entry: RcSge<'a, D>) {
        self.sga.add_entry(entry);
    }

    pub fn entries_slice(&self, start: usize, length: usize) -> &[RcSge<'a, D>] {
        self.sga.entries_slice(start, length)
    }

    pub fn copy_into_buffer(&self, buf: &mut [u8]) -> Result<usize> {
        let mut off = 0;
        for entry in self.sga.entries_slice(0, self.sga.len()).iter() {
            tracing::debug!("Copying entry into [{}, {}]", off, off + entry.len());
            let buf_to_copy = &mut buf[off..(off + entry.len())];
            buf_to_copy.copy_from_slice(&entry.addr());
            off += entry.len();
        }
        Ok(off)
    }

    pub fn mut_entries_slice(&mut self, start: usize, length: usize) -> &mut [RcSge<'a, D>] {
        self.sga.mut_entries_slice(start, length)
    }

    pub fn set_length(&mut self, length: usize) {
        self.sga.set_length(length);
    }

    pub fn replace(&mut self, idx: usize, entry: RcSge<'a, D>) {
        self.sga.replace(idx, entry);
    }

    pub fn len(&self) -> usize {
        self.sga.len()
    }

    pub fn copy_length(&self) -> usize {
        let size: usize = self
            .sga
            .iter()
            .take(self.num_copy_entries)
            .map(|seg| seg.len())
            .sum();
        size + self.hdr.len()
    }

    #[inline]
    fn is_zero_copy_seg(&self, i: usize, datapath: &D) -> bool {
        let seg = self.sga.get(i);
        seg.len() >= datapath.get_copying_threshold() && seg.is_ref_counted()
    }

    /// Reorders the scatter-gather array according to size heuristics, from the first entry
    /// forward,
    /// setting the number of copy entries to be the number of entries found under the
    /// threshold or not registered.
    /// Parameters:
    /// @datapath: Datapath handle providing function to check current size threshold, and whether
    /// buffers are registered.
    /// @offsets: Corresponding offsets vector (where indices correspond to current entries in the
    /// scatter-gather list). When indices are swapped in sga, corresponding indices must be
    /// swapped in offsets (assumes self.length == offsets.len() + 1)
    #[inline]
    pub fn reorder_by_size_and_registration(
        &mut self,
        datapath: &D,
        offsets: &mut Vec<usize>,
    ) -> Result<()> {
        if self.sga().len() != offsets.len() {
            bail!("passed in offsets array should be one less than sga length");
        }
        // reorder scatter-gather entries

        if self.sga.len() == 1 {
            if !self.is_zero_copy_seg(0, datapath) {
                self.num_copy_entries = 1;
            }
            tracing::debug!("Setting num copy entries as {}/1", self.num_copy_entries);
            return Ok(());
        }

        let mut forward_index = 0;
        let mut end_index = self.sga.len() - 1;
        let mut switch_forward = self.is_zero_copy_seg(forward_index, datapath);
        let mut switch_backward = !self.is_zero_copy_seg(end_index, datapath);
        let mut num_copy_segs = 0; // copy object header segment
        let mut ct = 0;

        // everytime forward index is advanced, we record a copy segment
        while !(forward_index >= end_index) {
            tracing::debug!(
                forward_index,
                end_index,
                switch_forward,
                switch_backward,
                "Looping over reordering segments"
            );
            ct += 1;
            match (switch_forward, switch_backward) {
                (true, true) => {
                    num_copy_segs += 1;
                    self.sga.swap(forward_index, end_index);
                    offsets.swap(forward_index, end_index);
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
                (true, false) => {
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
                (false, true) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                }
                (false, false) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
            }
        }
        self.num_copy_entries = num_copy_segs;
        tracing::debug!(
            num_copy_segs = self.num_copy_entries,
            loop_ct = ct,
            "DOne with reordering"
        );
        Ok(())
    }

    /// Reorder by max segments.
    /// Max segments includes the 1 segment for all entries that will be copied together
    pub fn reorder_by_max_segs(&mut self, datapath: &D, offsets: &mut Vec<usize>) -> Result<()> {
        if self.sga().len() != offsets.len() {
            bail!("passed in offsets array should be one less than sga length");
        }
        let current_zero_copy_segs = self.num_zero_copy_entries();
        let zero_copy_limit = datapath.get_max_segments() - 1;

        // already under maximum segments
        if current_zero_copy_segs <= zero_copy_limit {
            return Ok(());
        }

        let sga_len = self.sga.len();
        let segs_to_order = self
            .sga
            .mut_entries_slice(self.num_copy_entries, current_zero_copy_segs);

        let extra_to_copy = current_zero_copy_segs - zero_copy_limit;

        if extra_to_copy < zero_copy_limit {
            // move smallest entries to the front
            for i in self.num_copy_entries..(self.num_copy_entries + extra_to_copy) {
                let mut min_index = i;
                let mut min_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[i + 1..].iter().enumerate() {
                    if entry.len() < min_length {
                        min_length = entry.len();
                        min_index = entry_idx;
                    }
                }

                if min_index != i {
                    segs_to_order.swap(i, min_index);
                    offsets.swap(i, min_index);
                }
            }
        } else {
            // move max (zero_copy limit) segs to back
            for i in ((sga_len - zero_copy_limit)..sga_len).rev() {
                let mut max_index = i;
                let mut max_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[(sga_len - zero_copy_limit)..(i - 1)]
                    .iter()
                    .rev()
                    .enumerate()
                {
                    if entry.len() > max_length {
                        max_length = entry.len();
                        max_index = entry_idx;
                    }
                }

                if max_index != i {
                    segs_to_order.swap(i, max_index);
                    offsets.swap(i, max_index);
                }
            }
        }

        self.num_copy_entries += extra_to_copy;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ArenaOrderedSga<'a> {
    pub entries: bumpalo::collections::Vec<'a, Sge<'a>>,
    pub offsets: bumpalo::collections::Vec<'a, usize>,
    pub header_offsets: usize,
    length: usize,
    num_copy_entries: usize,
    hdr: bumpalo::collections::Vec<'a, u8>,
}

impl<'a> ArenaOrderedSga<'a> {
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub fn arena_size(batch_size: usize, max_packet_size: usize, max_entries: usize) -> usize {
        batch_size
            * 10
            * (max_packet_size * std::mem::size_of::<u8>()
                + std::mem::size_of::<usize>() * 2
                + max_entries * std::mem::size_of::<Sge>()
                + std::mem::size_of::<usize>() * 2
                + max_entries * std::mem::size_of::<usize>()
                + std::mem::size_of::<usize>() * 2)
    }

    pub fn clear(&mut self) {
        self.length = 0;
        self.num_copy_entries = 0;
    }

    pub fn flatten(&self) -> Vec<u8> {
        let mut buf = self.hdr.as_slice().to_vec();
        let mut others: Vec<u8> = self
            .entries
            .iter()
            .take(self.length)
            .map(|bytes| bytes.addr().to_vec())
            .flatten()
            .collect();
        buf.append(&mut others);
        buf
    }

    pub fn allocate(num_entries: usize, arena: &'a bumpalo::Bump) -> ArenaOrderedSga<'a> {
        ArenaOrderedSga {
            entries: bumpalo::collections::Vec::from_iter_in(
                std::iter::repeat(Sge::default()).take(num_entries),
                arena,
            ),
            offsets: bumpalo::collections::Vec::with_capacity_zeroed_in(num_entries, arena),
            header_offsets: 0,
            length: 0,
            num_copy_entries: 0,
            hdr: bumpalo::collections::Vec::new_in(&arena),
        }
    }

    pub fn set_header_offsets(&mut self, num: usize) {
        self.header_offsets = num;
    }

    pub fn finish_offsets(&mut self) {
        #[cfg(feature = "profiler")]
        demikernel::timer!("fill in sga offsets");
        let mut cur_dynamic_offset = self.header_offsets;

        for (sge, offset) in self
            .entries
            .iter()
            .take(self.length)
            .zip(self.offsets.iter().take(self.length))
        {
            let header_buffer = &mut self.hdr.as_mut_slice();
            tracing::debug!(addr =? &header_buffer[*offset..(*offset + 8)].as_ptr(), "Addr without cast");
            let mut obj_ref = MutForwardPointer(header_buffer, *offset);
            tracing::debug!(
                "At offset {}, writing in size {} and offset {}",
                *offset,
                sge.len(),
                cur_dynamic_offset
            );
            obj_ref.write_size(sge.len() as u32);
            obj_ref.write_offset(cur_dynamic_offset as u32);

            cur_dynamic_offset += sge.len();
        }
    }

    pub fn offsets_slice(&self, start: usize, end: usize) -> &[usize] {
        &self.offsets.as_slice()[start..end]
    }

    pub fn mut_offsets_slice(&mut self, start: usize, end: usize) -> &mut [usize] {
        &mut self.offsets.as_mut_slice()[start..end]
    }

    pub fn data_len(&self) -> usize {
        self.hdr.len()
            + self
                .entries
                .iter()
                .take(self.length)
                .map(|seg| seg.len())
                .sum::<usize>()
    }

    pub fn iter(&self) -> Iter<Sge<'a>> {
        self.entries.iter()
    }

    pub fn set_hdr(&mut self, vec: bumpalo::collections::Vec<'a, u8>) {
        self.hdr = vec;
    }

    pub fn get_hdr(&self) -> &[u8] {
        self.hdr.as_slice()
    }

    pub fn set_num_copy_entries(&mut self, num: usize) {
        self.num_copy_entries = num;
    }

    pub fn num_copy_entries(&self) -> usize {
        self.num_copy_entries
    }

    pub fn num_zero_copy_entries(&self) -> usize {
        self.length - self.num_copy_entries
    }

    pub fn add_entry(&mut self, entry: Sge<'a>) {
        if self.length < self.capacity() {
            self.entries[self.length] = entry;
        } else {
            self.entries.push(entry);
        }
        self.length += 1;
    }

    pub fn entries_slice(&self, start: usize, length: usize) -> &[Sge<'a>] {
        &self.entries.as_slice()[start..(start + length)]
    }

    pub fn mut_entries_slice(&mut self, start: usize, length: usize) -> &mut [Sge<'a>] {
        &mut self.entries.as_mut_slice()[start..(start + length)]
    }

    pub fn set_length(&mut self, length: usize) {
        self.length = length;
    }

    pub fn replace(&mut self, idx: usize, entry: Sge<'a>) {
        self.entries[idx] = entry;
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn copy_length(&self) -> usize {
        let size: usize = self
            .entries
            .iter()
            .take(self.num_copy_entries)
            .map(|seg| seg.len())
            .sum();
        size + self.hdr.len()
    }

    #[inline]
    fn is_zero_copy_seg<D>(&self, i: usize, d: &D) -> bool
    where
        D: datapath::Datapath,
    {
        let seg = self.entries[i].addr();
        //d.is_registered(seg) && seg.len() >= d.get_copying_threshold()
        //seg.len() >= d.get_copying_threshold() && d.is_registered(seg)
        seg.len() >= d.get_copying_threshold() && d.is_registered(seg)
    }

    /// Reorders scatter-gather array AND fills in offsets in header buffer.
    pub fn reorder_by_size_and_registration_and_finish_serialization<M, D>(
        &mut self,
        allocator: &allocator::MemoryPoolAllocator<M>,
        copying_threshold: usize,
        buffers: &mut [Option<D::DatapathMetadata>],
        with_copy: bool,
    ) -> Result<()>
    where
        M: allocator::DatapathMemoryPool<DatapathImpl = D> + std::fmt::Debug + Eq + PartialEq,
        D: datapath::Datapath,
    {
        if with_copy {
            self.num_copy_entries = self.length;
            self.finish_offsets();
            return Ok(());
        }

        // iterate over all segments, and recover buffers and increment ref count if need be
        for (seg, buf) in self
            .entries
            .iter()
            .take(self.length)
            .zip(buffers.iter_mut())
        {
            if seg.addr().len() < copying_threshold {
                *buf = None;
            } else {
                *buf = allocator.recover_buffer(seg.addr())?;
            }
        }

        if self.length == 1 {
            if let Some(_x) = &buffers[0] {
                self.num_copy_entries = 0;
            } else {
                self.num_copy_entries = 1;
            }
            self.finish_offsets();
            return Ok(());
        }

        let mut forward_index = 0;
        let mut end_index = self.length - 1;
        let mut switch_forward = buffers[forward_index] != None;
        let mut switch_backward = buffers[end_index] == None;
        let mut num_copy_segs = 0; // copy object header segment
        let mut ct: usize = 0;

        // everytime forward index is advanced, we record a copy segment
        while !(forward_index >= end_index) {
            tracing::debug!(
                forward_index,
                end_index,
                switch_forward,
                switch_backward,
                "Looping over reordering segments"
            );
            ct += 1;
            match (switch_forward, switch_backward) {
                (true, true) => {
                    num_copy_segs += 1;
                    self.entries.swap(forward_index, end_index);
                    self.offsets.swap(forward_index, end_index);
                    buffers.swap(forward_index, end_index);
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = buffers[forward_index] != None;
                    switch_backward = buffers[end_index] == None;
                }
                (true, false) => {
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_backward = buffers[end_index] == None;
                }
                (false, true) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    if forward_index >= end_index {
                        num_copy_segs += 1;
                        break;
                    }
                    switch_forward = buffers[forward_index] != None;
                }
                (false, false) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = buffers[forward_index] != None;
                    switch_backward = buffers[end_index] == None;
                }
            }
        }
        self.num_copy_entries = num_copy_segs;
        self.finish_offsets();
        tracing::debug!(
            num_copy_segs = self.num_copy_entries,
            loop_ct = ct,
            "DONE WITH REORDERING"
        );
        Ok(())
    }

    /// Reorders the scatter-gather array according to size heuristics, from the first entry
    /// forward,
    /// setting the number of copy entries to be the number of entries found under the
    /// threshold or not registered.
    /// Parameters:
    /// @datapath: Datapath handle providing function to check current size threshold, and whether
    /// buffers are registered.
    /// @offsets: Corresponding offsets vector (where indices correspond to current entries in the
    /// scatter-gather list). When indices are swapped in sga, corresponding indices must be
    /// swapped in offsets (assumes self.length == offsets.len() + 1)
    pub fn reorder_by_size_and_registration<D>(
        &mut self,
        datapath: &D,
        with_copy: bool,
    ) -> Result<()>
    where
        D: datapath::Datapath,
    {
        if with_copy {
            return Ok(());
        }
        if self.length == 1 {
            // check if segment is zero-copy or not
            if !self.is_zero_copy_seg(0, datapath) {
                self.num_copy_entries = 1;
            }
            tracing::debug!("Setting num copy entries as {}/1", self.num_copy_entries);
            return Ok(());
        }

        let mut forward_index = 0;
        let mut end_index = self.length - 1;
        let mut switch_forward = self.is_zero_copy_seg(forward_index, datapath);
        let mut switch_backward = !self.is_zero_copy_seg(end_index, datapath);
        let mut num_copy_segs = 0; // copy object header segment
        let mut ct: usize = 0;

        // everytime forward index is advanced, we record a copy segment
        while !(forward_index >= end_index) {
            tracing::debug!(
                forward_index,
                end_index,
                switch_forward,
                switch_backward,
                "Looping over reordering segments"
            );
            ct += 1;
            match (switch_forward, switch_backward) {
                (true, true) => {
                    num_copy_segs += 1;
                    self.entries.swap(forward_index, end_index);
                    self.offsets.swap(forward_index, end_index);
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
                (true, false) => {
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
                (false, true) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    if forward_index >= end_index {
                        num_copy_segs += 1;
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                }
                (false, false) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = !self.is_zero_copy_seg(end_index, datapath);
                }
            }
        }
        self.num_copy_entries = num_copy_segs;
        tracing::debug!(
            num_copy_segs = self.num_copy_entries,
            loop_ct = ct,
            "DONE WITH REORDERING"
        );
        Ok(())
    }

    /// Reorder by max segments.
    /// Max segments includes the 1 segment for all entries that will be copied together
    pub fn reorder_by_max_segs<D>(&mut self, datapath: &D) -> Result<()>
    where
        D: datapath::Datapath,
    {
        let current_zero_copy_segs = self.num_zero_copy_entries();
        let zero_copy_limit = datapath.get_max_segments() - 1;
        let num_copy_entries = self.num_copy_entries;

        // already under maximum segments
        if current_zero_copy_segs <= zero_copy_limit {
            return Ok(());
        }

        let sga_len = self.length;
        let segs_to_order = &mut self.entries[self.num_copy_entries..current_zero_copy_segs];
        let offsets = &mut self.offsets[self.num_copy_entries..current_zero_copy_segs];

        let extra_to_copy = current_zero_copy_segs - zero_copy_limit;

        if extra_to_copy < zero_copy_limit {
            // move smallest entries to the front
            for i in num_copy_entries..(num_copy_entries + extra_to_copy) {
                let mut min_index = i;
                let mut min_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[i + 1..].iter().enumerate() {
                    if entry.len() < min_length {
                        min_length = entry.len();
                        min_index = entry_idx;
                    }
                }

                if min_index != i {
                    segs_to_order.swap(i, min_index);
                    offsets.swap(i, min_index);
                }
            }
        } else {
            // move max (zero_copy limit) segs to back
            for i in ((sga_len - zero_copy_limit)..sga_len).rev() {
                let mut max_index = i;
                let mut max_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[(sga_len - zero_copy_limit)..(i - 1)]
                    .iter()
                    .rev()
                    .enumerate()
                {
                    if entry.len() > max_length {
                        max_length = entry.len();
                        max_index = entry_idx;
                    }
                }

                if max_index != i {
                    segs_to_order.swap(i, max_index);
                    offsets.swap(i, max_index);
                }
            }
        }

        self.num_copy_entries += extra_to_copy;
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub enum RcSge<'a, D>
where
    D: datapath::Datapath,
{
    RawRef(&'a [u8]),
    RefCounted(D::DatapathMetadata),
}

impl<'a, D> Default for RcSge<'a, D>
where
    D: datapath::Datapath,
{
    fn default() -> RcSge<'a, D> {
        RcSge::RawRef(&[])
    }
}

impl<'a, D> std::fmt::Debug for RcSge<'a, D>
where
    D: datapath::Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RcSge::RawRef(ptr) => {
                write!(f, "RcSge::RawRef: {:?}", ptr.as_ptr())
            }
            RcSge::RefCounted(ptr) => {
                write!(f, "RcSge::RefCounted: {:?}", ptr)
            }
        }
    }
}

impl<'a, D> Clone for RcSge<'a, D>
where
    D: datapath::Datapath,
{
    fn clone(&self) -> RcSge<'a, D> {
        match self {
            RcSge::RawRef(buf) => RcSge::RawRef(buf),
            RcSge::RefCounted(metadata) => RcSge::RefCounted(metadata.clone()),
        }
    }
}

impl<'a, D> RcSge<'a, D>
where
    D: datapath::Datapath,
{
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            RcSge::RawRef(buf) => buf.len(),
            RcSge::RefCounted(metadata) => metadata.data_len(),
        }
    }

    #[inline]
    pub fn is_ref_counted(&self) -> bool {
        match self {
            RcSge::RawRef(_) => false,
            RcSge::RefCounted(_) => true,
        }
    }

    #[inline]
    pub fn addr(&self) -> &[u8] {
        match self {
            RcSge::RawRef(buf) => buf,
            RcSge::RefCounted(metadata) => metadata.as_ref(),
        }
    }

    #[inline]
    pub fn inner_datapath_pkt(&self) -> Option<&D::DatapathMetadata> {
        match self {
            RcSge::RawRef(_) => None,
            RcSge::RefCounted(m) => Some(m),
        }
    }

    #[inline]
    pub fn inner_datapath_pkt_mut(&mut self) -> Option<&mut D::DatapathMetadata> {
        match self {
            RcSge::RawRef(_) => None,
            RcSge::RefCounted(ref mut m) => Some(m),
        }
    }

    #[inline]
    pub fn clone_with_bounds(&self, off: usize, size: usize) -> Result<RcSge<'a, D>> {
        match self {
            RcSge::RawRef(buf) => Ok(RcSge::RawRef(&buf[off..(off + size)])),
            RcSge::RefCounted(metadata) => {
                let mut new_metadata = metadata.clone();
                new_metadata.set_data_len_and_offset(size, metadata.offset() + off)?;
                Ok(RcSge::RefCounted(new_metadata))
            }
        }
    }
}

impl<'a, D> AsRef<[u8]> for RcSge<'a, D>
where
    D: datapath::Datapath,
{
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match self {
            RcSge::RawRef(buf) => buf,
            RcSge::RefCounted(metadata) => metadata.as_ref(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct RcSga<'a, D>
where
    D: datapath::Datapath,
{
    entries: Vec<RcSge<'a, D>>,
    length: usize,
}

impl<'a, D> RcSga<'a, D>
where
    D: datapath::Datapath,
{
    pub fn new() -> Self {
        RcSga {
            entries: Vec::default(),
            length: 0,
        }
    }
    pub fn with_capacity(num_entries: usize) -> Self {
        RcSga {
            entries: Vec::with_capacity(num_entries),
            length: 0,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn swap(&mut self, a: usize, b: usize) {
        self.entries.swap(a, b);
    }

    pub fn clear(&mut self) {
        self.length = 0;
    }

    pub fn allocate(num_entries: usize) -> Self {
        let mut sga = RcSga::with_capacity(num_entries);
        for _i in 0..num_entries {
            sga.add_entry(RcSge::default());
        }
        sga.set_length(0);
        sga
    }

    pub fn with_entries(entries: Vec<RcSge<'a, D>>) -> Self {
        RcSga {
            length: entries.len(),
            entries: entries,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn set_length(&mut self, len: usize) {
        self.length = len;
    }

    pub fn data_len(&self) -> usize {
        self.entries.iter().take(self.length).map(|e| e.len()).sum()
    }

    pub fn add_entry(&mut self, entry: RcSge<'a, D>) {
        if self.length < self.entries.len() {
            self.entries[self.length] = entry;
            self.length += 1;
        } else {
            self.entries.push(entry);
            self.length += 1;
        }
    }

    pub fn replace(&mut self, idx: usize, entry: RcSge<'a, D>) {
        self.entries[idx] = entry;
    }

    pub fn get(&self, idx: usize) -> &RcSge<'a, D> {
        &self.entries[idx]
    }

    pub fn flatten(&self) -> Vec<u8> {
        self.entries
            .iter()
            .take(self.length)
            .map(|seg| seg.as_ref().to_vec())
            .flatten()
            .collect()
    }

    pub fn get_mut(&mut self, idx: usize) -> &mut RcSge<'a, D> {
        &mut self.entries[idx]
    }

    pub fn iter(&self) -> Iter<RcSge<'a, D>> {
        self.entries.iter()
    }

    pub fn mut_entries_slice(&mut self, start: usize, length: usize) -> &mut [RcSge<'a, D>] {
        &mut self.entries.as_mut_slice()[start..(start + length)]
    }

    pub fn entries_slice(&self, start: usize, length: usize) -> &[RcSge<'a, D>] {
        &self.entries.as_slice()[start..(start + length)]
    }

    pub fn is_zero_copy_seg(&self, i: usize, d: &D) -> bool {
        match &self.entries[i] {
            RcSge::RawRef(_buf) => false,
            RcSge::RefCounted(metadata) => metadata.data_len() >= d.get_copying_threshold(),
        }
    }

    /// Reorders from first entry forward, by size and registration
    pub fn reorder_by_size_and_registration(
        &mut self,
        datapath: &D,
        offsets: &mut Vec<usize>,
    ) -> Result<()> {
        if self.len() != offsets.len() + 1 {
            bail!("passed in offsets array should be one less than sga length");
        }

        let mut forward_index = 1;
        let mut end_index = self.len() - 1;
        let mut switch_forward = !self.is_zero_copy_seg(forward_index, datapath);
        let mut switch_backward = self.is_zero_copy_seg(end_index, datapath);

        // everytime forward index is advanced, we record a copy segment
        while !(forward_index >= end_index) {
            match (switch_forward, switch_backward) {
                (true, true) => {
                    self.entries.swap(forward_index, end_index);
                    offsets.swap(forward_index, end_index);
                    forward_index += 1;
                    end_index -= 1;
                    switch_forward = !self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = self.is_zero_copy_seg(end_index, datapath);
                }
                (true, false) => {
                    end_index -= 1;
                    switch_backward = self.is_zero_copy_seg(end_index, datapath);
                }
                (false, true) => {
                    forward_index += 1;
                    switch_forward = !self.is_zero_copy_seg(forward_index, datapath);
                }
                (false, false) => {
                    forward_index += 1;
                    end_index -= 1;
                    switch_forward = !self.is_zero_copy_seg(forward_index, datapath);
                    switch_backward = self.is_zero_copy_seg(end_index, datapath);
                }
            }
        }
        Ok(())
    }

    fn cur_zero_copy_segs(&self, datapath: &D) -> usize {
        let threshold = datapath.get_copying_threshold();
        self.entries
            .iter()
            .map(|seg| match seg {
                RcSge::RawRef(_) => 0,
                RcSge::RefCounted(m) => (m.data_len() >= threshold) as usize,
            })
            .sum()
    }

    fn first_zero_copy_seg(&self, datapath: &D) -> usize {
        let threshold = datapath.get_copying_threshold();
        self.entries
            .iter()
            .take_while(|seg| match seg {
                RcSge::RawRef(_) => true,
                RcSge::RefCounted(m) => m.data_len() < threshold,
            })
            .count()
    }

    pub fn reorder_by_max_segs(&mut self, datapath: &D, offsets: &mut Vec<usize>) -> Result<()> {
        if self.len() != offsets.len() + 1 {
            bail!("passed in offsets array should be one less than sga length");
        }

        let current_zero_copy_segs = self.cur_zero_copy_segs(datapath);
        let zero_copy_limit = datapath.get_max_segments() - 1;

        // already under maximum segments
        if current_zero_copy_segs <= zero_copy_limit {
            return Ok(());
        }

        let sga_len = self.len();
        let num_copy_entries = self.first_zero_copy_seg(datapath);
        let segs_to_order = self.mut_entries_slice(num_copy_entries, current_zero_copy_segs);

        let extra_to_copy = current_zero_copy_segs - zero_copy_limit;

        if extra_to_copy < zero_copy_limit {
            // move smallest entries to the front
            for i in num_copy_entries..(num_copy_entries + extra_to_copy) {
                let mut min_index = i;
                let mut min_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[i + 1..].iter().enumerate() {
                    if entry.len() < min_length {
                        min_length = entry.len();
                        min_index = entry_idx;
                    }
                }

                if min_index != i {
                    segs_to_order.swap(i, min_index);
                    offsets.swap(i, min_index);
                }
            }
        } else {
            // move max (zero_copy limit) segs to back
            for i in ((sga_len - zero_copy_limit)..sga_len).rev() {
                let mut max_index = i;
                let mut max_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[(sga_len - zero_copy_limit)..(i - 1)]
                    .iter()
                    .rev()
                    .enumerate()
                {
                    if entry.len() > max_length {
                        max_length = entry.len();
                        max_index = entry_idx;
                    }
                }

                if max_index != i {
                    segs_to_order.swap(i, max_index);
                    offsets.swap(i, max_index);
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyContextRef<D>
where
    D: datapath::Datapath,
{
    datapath_buffer: D::DatapathBuffer,
    index: usize,
    start: usize,
    len: usize,
    total_offset: usize,
}

impl<D> CopyContextRef<D>
where
    D: datapath::Datapath,
{
    pub fn new(
        datapath_buffer: D::DatapathBuffer,
        index: usize,
        start: usize,
        len: usize,
        total_offset: usize,
    ) -> Self {
        CopyContextRef {
            datapath_buffer: datapath_buffer,
            index: index,
            start: start,
            len: len,
            total_offset: total_offset,
        }
    }
}

impl<D> CopyContextRef<D>
where
    D: datapath::Datapath,
{
    #[inline]
    fn datapath_buffer(&self) -> &D::DatapathBuffer {
        &self.datapath_buffer
    }
    #[inline]
    fn index(&self) -> usize {
        self.index
    }
    #[inline]
    fn offset(&self) -> usize {
        self.start
    }
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn total_offset(&self) -> usize {
        self.total_offset
    }
}

impl<D> AsRef<[u8]> for CopyContextRef<D>
where
    D: datapath::Datapath,
{
    fn as_ref(&self) -> &[u8] {
        &self.datapath_buffer.as_ref()[self.start..(self.start + self.len)]
    }
}

impl<D> Clone for CopyContextRef<D>
where
    D: datapath::Datapath,
{
    fn clone(&self) -> Self {
        CopyContextRef {
            datapath_buffer: self.datapath_buffer.clone(),
            index: self.index,
            start: self.start,
            len: self.len,
            total_offset: self.total_offset,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SerializationCopyBuf<D>
where
    D: datapath::Datapath,
{
    // underlying datapath buffer
    buf: D::DatapathBuffer,
    // how much is allocated
    total_len: usize,
}

impl<D> SerializationCopyBuf<D>
where
    D: datapath::Datapath,
{
    pub fn get_buffer(&self) -> D::DatapathBuffer {
        self.buf.clone()
    }
    pub fn new(datapath: &mut D) -> Result<Self> {
        let (buf_option, max_len) = datapath.allocate_tx_buffer()?;
        match buf_option {
            Some(buf) => {
                tracing::debug!(
                    "Allocated new serialization copy buf, current length is {}",
                    buf.as_ref().len()
                );
                Ok(SerializationCopyBuf {
                    buf: buf,
                    total_len: max_len,
                })
            }
            None => {
                bail!("Could not allocate tx buffer for serialization copying.");
            }
        }
    }

    #[inline]
    pub fn contains(&self, copy_context_ref: &CopyContextRef<D>) -> Result<()> {
        let buffer = self.buf.as_ref();
        match buffer.as_ptr() == copy_context_ref.datapath_buffer().as_ref().as_ptr()
            && buffer.len() > copy_context_ref.offset()
            && buffer.len() >= (copy_context_ref.offset() + copy_context_ref.len())
        {
            true => Ok(()),
            false => {
                bail!("Copy context ref not contained inside copy buffer");
            }
        }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.total_len - self.len()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.as_ref().len()
    }

    #[inline]
    pub fn copy_context_ref(
        &self,
        index: usize,
        start: usize,
        len: usize,
        total_offset: usize,
    ) -> CopyContextRef<D> {
        tracing::debug!(
            index = index,
            start = start,
            len = len,
            total_offset = total_offset,
            "Copy context ref being made"
        );
        CopyContextRef::new(self.buf.clone(), index, start, len, total_offset)
    }
}

impl<D> std::io::Write for SerializationCopyBuf<D>
where
    D: datapath::Datapath,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyContext<'a, D>
where
    D: datapath::Datapath,
{
    pub copy_buffers: Vec<SerializationCopyBuf<D>>,
    threshold: usize,
    current_length: usize,
    remaining: usize,
    phantom: std::marker::PhantomData<&'a D>
}

impl<'a, D> CopyContext<'a, D>
where
    D: datapath::Datapath,
{
    #[inline]
    pub fn data_len(&self) -> usize {
        self.copy_buffers.iter().map(|buf| buf.len()).sum::<usize>()
    }
    #[inline]
    pub fn num_segs(&self) -> usize {
        self.copy_buffers.len()
    }

    #[inline]
    pub fn copy_buffers_slice(&self) -> &[SerializationCopyBuf<D>] {
        &self.copy_buffers.as_slice()
    }

    #[inline]
    pub fn new(arena: &'a bumpalo::Bump, datapath: &mut D) -> Result<Self> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Allocate new copy context");
        Ok(CopyContext {
            copy_buffers: Vec::new(),
            threshold: datapath.get_copying_threshold(),
            current_length: 0,
            remaining: 0,
            phantom: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn reset(&mut self) {
        self.copy_buffers.clear();
        self.current_length = 0;
        self.remaining = 0;
    }

    #[inline]
    pub fn check(&self, copy_context_ref: &CopyContextRef<D>) -> Result<()> {
        ensure!(
            copy_context_ref.index() < self.copy_buffers.len(),
            "Copy context ref index too large"
        );
        let serialization_copy_buf = &self.copy_buffers[copy_context_ref.index()];
        serialization_copy_buf.contains(copy_context_ref)
    }

    #[inline]
    pub fn should_copy(&self, ptr: &[u8]) -> bool {
        ptr.len() < self.threshold
    }

    #[inline]
    pub fn push(&mut self, datapath: &mut D) -> Result<()> {
        let buf = SerializationCopyBuf::new(datapath)?;
        self.remaining = buf.remaining();
        self.copy_buffers.push(buf);
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.copy_buffers.len()
    }

    /// Copies data into copy context.
    /// Returns (start, end) range of copy context that buffer was copied into.
    #[inline]
    pub fn copy(&mut self, buf: &[u8], datapath: &mut D) -> Result<CopyContextRef<D>> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Copy in copy context");

        let current_length = self.current_length;
        // TODO: doesn't work if buffer is > than an MTU
        if self.remaining < buf.len() {
            self.push(datapath)?;
        }
        let copy_buffers_len = self.copy_buffers.len();
        let last_buf = &mut self.copy_buffers[copy_buffers_len - 1];
        let current_offset = last_buf.len();
        let written = last_buf.write(buf)?;
        if written != buf.len() {
            bail!(
                "Failed to write entire buf len into copy buffer, only wrote: {:?}",
                written
            );
        }
        self.current_length += written;
        self.remaining -= written;
        return Ok(last_buf.copy_context_ref(
            copy_buffers_len - 1,
            current_offset,
            written,
            current_length,
        ));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ArenaDatapathSga<'a, D>
where
    D: datapath::Datapath,
{
    // buffers user has copied into
    copy_context: CopyContext<'a, D>,
    // zero copy entries
    zero_copy_entries: bumpalo::collections::Vec<'a, D::DatapathMetadata>,
    // actual hdr
    header: bumpalo::collections::Vec<'a, u8>,
}

impl<'a, D> ArenaDatapathSga<'a, D>
where
    D: datapath::Datapath,
{
    pub fn new(
        copy_context: CopyContext<'a, D>,
        zero_copy_entries: bumpalo::collections::Vec<'a, D::DatapathMetadata>,
        header: bumpalo::collections::Vec<'a, u8>,
    ) -> Self {
        ArenaDatapathSga {
            copy_context: copy_context,
            zero_copy_entries: zero_copy_entries,
            header: header,
        }
    }

    #[inline]
    pub fn zero_copy_entries_mut_slice(&mut self) -> &mut [D::DatapathMetadata] {
        self.zero_copy_entries.as_mut_slice()
    }

    #[inline]
    pub fn copy_context(&self) -> &CopyContext<'a, D> {
        &self.copy_context
    }

    pub fn copy_len(&self) -> usize {
        self.copy_context.data_len()
    }

    /// Get mutable reference to header
    pub fn get_header(&self) -> &[u8] {
        self.header.as_slice()
    }

    /// Data length of entire SGA
    pub fn data_len(&self) -> usize {
        self.copy_context.data_len()
            + self
                .zero_copy_entries
                .iter()
                .map(|seg| seg.as_ref().len())
                .sum::<usize>()
            + self.header.len()
    }

    pub fn num_zero_copy_entries(&self) -> usize {
        self.zero_copy_entries.len()
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ArenaOrderedRcSga<'a, D>
where
    D: datapath::Datapath,
{
    pub entries: bumpalo::collections::Vec<'a, RcSge<'a, D>>,
    pub offsets: bumpalo::collections::Vec<'a, usize>,
    pub header_offsets: usize,
    length: usize,
    num_copy_entries: usize,
    hdr: bumpalo::collections::Vec<'a, u8>,
}

impl<'a, D> ArenaOrderedRcSga<'a, D>
where
    D: datapath::Datapath,
{
    #[inline]
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn arena_size(batch_size: usize, max_packet_size: usize, max_entries: usize) -> usize {
        batch_size
            * 10
            * (max_packet_size * std::mem::size_of::<u8>()
                + std::mem::size_of::<usize>() * 2
                + max_entries * std::mem::size_of::<RcSge<D>>()
                + std::mem::size_of::<usize>() * 2
                + max_entries * std::mem::size_of::<usize>()
                + std::mem::size_of::<usize>() * 2)
    }

    #[inline]
    pub fn clear(&mut self) {
        self.length = 0;
        self.num_copy_entries = 0;
    }

    pub fn flatten(&self) -> Vec<u8> {
        let mut buf = self.hdr.as_slice().to_vec();
        let mut others: Vec<u8> = self
            .entries
            .iter()
            .take(self.length)
            .map(|bytes| bytes.addr().to_vec())
            .flatten()
            .collect();
        buf.append(&mut others);
        buf
    }

    #[inline]
    pub fn allocate(num_entries: usize, arena: &'a bumpalo::Bump) -> ArenaOrderedRcSga<'a, D> {
        ArenaOrderedRcSga {
            entries: bumpalo::collections::Vec::from_iter_in(
                std::iter::repeat(RcSge::default()).take(num_entries),
                arena,
            ),
            offsets: bumpalo::collections::Vec::with_capacity_zeroed_in(num_entries, arena),
            header_offsets: 0,
            length: 0,
            num_copy_entries: 0,
            hdr: bumpalo::collections::Vec::new_in(&arena),
        }
    }

    #[inline]
    pub fn set_header_offsets(&mut self, num: usize) {
        self.header_offsets = num;
    }

    #[inline]
    pub fn finish_offsets(&mut self) {
        #[cfg(feature = "profiler")]
        demikernel::timer!("fill in sga offsets");
        let mut cur_dynamic_offset = self.header_offsets;

        for (rc_sge, offset) in self
            .entries
            .iter()
            .take(self.length)
            .zip(self.offsets.iter().take(self.length))
        {
            let header_buffer = &mut self.hdr.as_mut_slice();
            tracing::debug!(addr =? &header_buffer[*offset..(*offset + 8)].as_ptr(), "Addr without cast");
            let mut obj_ref = MutForwardPointer(header_buffer, *offset);
            tracing::debug!(
                "At offset {}, writing in size {} and offset {}",
                *offset,
                rc_sge.len(),
                cur_dynamic_offset
            );
            obj_ref.write_size(rc_sge.len() as u32);
            obj_ref.write_offset(cur_dynamic_offset as u32);

            cur_dynamic_offset += rc_sge.len();
        }
    }

    #[inline]
    pub fn offsets_slice(&self, start: usize, end: usize) -> &[usize] {
        &self.offsets.as_slice()[start..end]
    }

    #[inline]
    pub fn mut_offsets_slice(&mut self, start: usize, end: usize) -> &mut [usize] {
        &mut self.offsets.as_mut_slice()[start..end]
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        self.hdr.len()
            + self
                .entries
                .iter()
                .take(self.length)
                .map(|seg| seg.len())
                .sum::<usize>()
    }

    #[inline]
    pub fn iter(&self) -> Iter<RcSge<'a, D>> {
        self.entries.iter()
    }

    #[inline]
    pub fn set_hdr(&mut self, vec: bumpalo::collections::Vec<'a, u8>) {
        self.hdr = vec;
    }

    #[inline]
    pub fn get_hdr(&self) -> &[u8] {
        self.hdr.as_slice()
    }

    #[inline]
    pub fn set_num_copy_entries(&mut self, num: usize) {
        self.num_copy_entries = num;
    }

    #[inline]
    pub fn num_copy_entries(&self) -> usize {
        self.num_copy_entries
    }

    #[inline]
    pub fn num_zero_copy_entries(&self) -> usize {
        self.length - self.num_copy_entries
    }

    #[inline]
    pub fn add_entry(&mut self, entry: RcSge<'a, D>) {
        if self.length < self.capacity() {
            self.entries[self.length] = entry;
        } else {
            self.entries.push(entry);
        }
        self.length += 1;
    }

    #[inline]
    pub fn entries_slice(&self, start: usize, length: usize) -> &[RcSge<'a, D>] {
        &self.entries.as_slice()[start..(start + length)]
    }

    #[inline]
    pub fn mut_entries_slice(&mut self, start: usize, length: usize) -> &mut [RcSge<'a, D>] {
        &mut self.entries.as_mut_slice()[start..(start + length)]
    }

    #[inline]
    pub fn set_length(&mut self, length: usize) {
        self.length = length;
    }

    #[inline]
    pub fn replace(&mut self, idx: usize, entry: RcSge<'a, D>) {
        self.entries[idx] = entry;
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn copy_length(&self) -> usize {
        let size: usize = self
            .entries
            .iter()
            .take(self.num_copy_entries)
            .map(|seg| seg.len())
            .sum();
        size + self.hdr.len()
    }

    #[inline]
    fn is_zero_copy_seg(&self, seg_idx: usize, copying_threshold: usize) -> bool {
        let seg = &self.entries[seg_idx];
        seg.len() >= copying_threshold && seg.is_ref_counted()
    }

    /// Reorders the scatter-gather array according to size heuristics, from the first entry
    /// forward,
    /// setting the number of copy entries to be the number of entries found under the
    /// threshold or not registered.
    /// Parameters:
    /// @datapath: Datapath handle providing function to check current size threshold, and whether
    /// buffers are registered.
    /// @offsets: Corresponding offsets vector (where indices correspond to current entries in the
    /// scatter-gather list). When indices are swapped in sga, corresponding indices must be
    /// swapped in offsets (assumes self.length == offsets.len() + 1)
    #[inline]
    pub fn reorder_by_size_and_registration(
        &mut self,
        datapath: &D,
        with_copy: bool,
    ) -> Result<()> {
        if with_copy {
            self.num_copy_entries = self.length;
            return Ok(());
        }
        let copying_threshold = datapath.get_copying_threshold();
        if self.length == 1 {
            // check if segment is zero-copy or not
            if !self.is_zero_copy_seg(0, copying_threshold) {
                self.num_copy_entries = 1;
            }
            tracing::debug!("Setting num copy entries as {}/1", self.num_copy_entries);
            return Ok(());
        }
        let mut forward_index = 0;
        let mut end_index = self.length - 1;
        let mut switch_forward = self.is_zero_copy_seg(forward_index, copying_threshold);
        let mut switch_backward = !self.is_zero_copy_seg(end_index, copying_threshold);
        let mut num_copy_segs = 0; // copy object header segment
        let mut ct: usize = 0;

        // everytime forward index is advanced, we record a copy segment
        while !(forward_index >= end_index) {
            tracing::debug!(
                forward_index,
                end_index,
                switch_forward,
                switch_backward,
                "Looping over reordering segments"
            );
            ct += 1;
            match (switch_forward, switch_backward) {
                (true, true) => {
                    num_copy_segs += 1;
                    self.entries.swap(forward_index, end_index);
                    self.offsets.swap(forward_index, end_index);
                    tracing::debug!(
                        "Swapping {} and {}; length 1: {}, length 2: {}",
                        forward_index,
                        end_index,
                        self.entries[forward_index].len(),
                        self.entries[end_index].len()
                    );
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, copying_threshold);
                    switch_backward = !self.is_zero_copy_seg(end_index, copying_threshold);
                }
                (true, false) => {
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_backward = !self.is_zero_copy_seg(end_index, copying_threshold);
                }
                (false, true) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    if forward_index >= end_index {
                        num_copy_segs += 1;
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, copying_threshold);
                }
                (false, false) => {
                    num_copy_segs += 1;
                    forward_index += 1;
                    end_index -= 1;
                    if forward_index >= end_index {
                        break;
                    }
                    switch_forward = self.is_zero_copy_seg(forward_index, copying_threshold);
                    switch_backward = !self.is_zero_copy_seg(end_index, copying_threshold);
                }
            }
        }

        // induce some extra swapping
        /*forward_index = 0;
        end_index = self.length - 1;
        while (forward_index < end_index) {
            self.entries.swap(forward_index, end_index);
            self.offsets.swap(forward_index, end_index);
            forward_index += 1;
            end_index -= 1;
        }*/
        self.num_copy_entries = num_copy_segs;
        tracing::debug!(
            num_copy_segs = self.num_copy_entries,
            loop_ct = ct,
            "DONE WITH REORDERING"
        );
        Ok(())
    }

    /// Reorder by max segments.
    /// Max segments includes the 1 segment for all entries that will be copied together
    pub fn reorder_by_max_segs(&mut self, datapath: &D) -> Result<()> {
        let current_zero_copy_segs = self.num_zero_copy_entries();
        let zero_copy_limit = datapath.get_max_segments() - 1;
        let num_copy_entries = self.num_copy_entries;

        // already under maximum segments
        if current_zero_copy_segs <= zero_copy_limit {
            return Ok(());
        }

        let sga_len = self.length;
        let segs_to_order = &mut self.entries[self.num_copy_entries..current_zero_copy_segs];
        let offsets = &mut self.offsets[self.num_copy_entries..current_zero_copy_segs];

        let extra_to_copy = current_zero_copy_segs - zero_copy_limit;

        if extra_to_copy < zero_copy_limit {
            // move smallest entries to the front
            for i in num_copy_entries..(num_copy_entries + extra_to_copy) {
                let mut min_index = i;
                let mut min_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[i + 1..].iter().enumerate() {
                    if entry.len() < min_length {
                        min_length = entry.len();
                        min_index = entry_idx;
                    }
                }

                if min_index != i {
                    segs_to_order.swap(i, min_index);
                    offsets.swap(i, min_index);
                }
            }
        } else {
            // move max (zero_copy limit) segs to back
            for i in ((sga_len - zero_copy_limit)..sga_len).rev() {
                let mut max_index = i;
                let mut max_length = segs_to_order[i].len();

                for (entry_idx, entry) in segs_to_order[(sga_len - zero_copy_limit)..(i - 1)]
                    .iter()
                    .rev()
                    .enumerate()
                {
                    if entry.len() > max_length {
                        max_length = entry.len();
                        max_index = entry_idx;
                    }
                }

                if max_index != i {
                    segs_to_order.swap(i, max_index);
                    offsets.swap(i, max_index);
                }
            }
        }

        self.num_copy_entries += extra_to_copy;
        Ok(())
    }
}

// how do we make it such that the programmer DOESN'T need to know what is registered and what is
#[derive(Debug, PartialEq, Eq)]
pub enum RcCornPtr<'a, D>
where
    D: Datapath,
{
    RawRef(&'a [u8]),     // used for object header potentially
    RefCounted(CfBuf<D>), // used for any datapath value
}

impl<'a, D> Clone for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn clone(&self) -> Self {
        match self {
            RcCornPtr::RawRef(buf) => RcCornPtr::RawRef(buf),
            RcCornPtr::RefCounted(buf) => RcCornPtr::RefCounted(buf.clone()),
        }
    }
}

impl<'a, D> Default for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        RcCornPtr::RawRef(&[])
    }
}

impl<'a, D> AsRef<[u8]> for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            RcCornPtr::RawRef(buf) => buf,
            RcCornPtr::RefCounted(buf) => buf.as_ref(),
        }
    }
}

impl<'a, D> PtrAttributes for RcCornPtr<'a, D>
where
    D: Datapath,
{
    fn buf_type(&self) -> CornType {
        match self {
            RcCornPtr::RefCounted(_) => CornType::Registered,
            RcCornPtr::RawRef(_) => CornType::Normal,
        }
    }

    fn buf_size(&self) -> usize {
        match self {
            RcCornPtr::RefCounted(buf) => buf.len(),
            RcCornPtr::RawRef(buf) => buf.len(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RcCornflake<'a, D>
where
    D: Datapath,
{
    /// Id for message. If None, datapath doesn't need to keep track of per-packet timeouts.
    id: MsgID,
    /// Pointers to scattered memory segments.
    entries: Vec<RcCornPtr<'a, D>>,
    /// Data size represented by this cornflake (in ttoal)
    data_size: usize,
    /// Number that refer to ref-counted regions.
    num_refcnted: usize,
    /// Number of total filled entries
    num_filled: usize,
}

impl<'a, D> Default for RcCornflake<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        RcCornflake {
            id: MsgID::default(),
            entries: Vec::default(),
            data_size: 0,
            num_refcnted: 0,
            num_filled: 0,
        }
    }
}

impl<'a, D> RcCornflake<'a, D>
where
    D: Datapath,
{
    pub fn new() -> Self {
        RcCornflake {
            id: MsgID::default(),
            entries: Vec::default(),
            data_size: 0,
            num_refcnted: 0,
            num_filled: 0,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        RcCornflake {
            id: MsgID::default(),
            entries: vec![RcCornPtr::RawRef(&[]); cap],
            data_size: 0,
            num_refcnted: 0,
            num_filled: 0,
        }
    }

    pub fn add_entry(&mut self, entry: RcCornPtr<'a, D>) {
        self.data_size += entry.buf_size();
        if entry.buf_type() == CornType::Registered {
            self.num_refcnted += 1;
        }
        if self.num_filled == self.entries.len() {
            self.entries.push(entry);
        } else {
            self.entries[self.num_filled] = entry;
        }
        self.num_filled += 1;
    }
}

impl<'a, D> ScatterGather for RcCornflake<'a, D>
where
    D: Datapath,
{
    /// Pointer type is reference to CornPtr.
    type Ptr = RcCornPtr<'a, D>;
    /// Can return an iterator over CornPtr references.
    type Collection = Vec<RcCornPtr<'a, D>>;

    /// Returns the id of this cornflake.
    fn get_id(&self) -> MsgID {
        self.id
    }

    /// Sets the id.
    fn set_id(&mut self, id: MsgID) {
        self.id = id;
    }

    /// Returns number of entries in this cornflake.
    fn num_segments(&self) -> usize {
        self.num_filled
    }

    /// Returns the number of borrowed memory regions in this cornflake.
    fn num_borrowed_segments(&self) -> usize {
        self.num_refcnted
    }

    /// Amount of data represented by this scatter-gather array.
    fn data_len(&self) -> usize {
        self.data_size
    }

    /// Exposes an iterator over the entries in the scatter-gather array.
    fn collection(&self) -> Self::Collection {
        let mut vec = Vec::new();
        for i in 0..self.num_filled {
            vec.push(self.entries[i].clone());
        }
        vec
    }

    /// Returns CornPtr at Index
    fn index(&self, idx: usize) -> &Self::Ptr {
        &self.entries[idx]
    }

    /// Apply an iterator to entries of the scatter-gather array, without consuming the
    /// scatter-gather array.
    fn iter_apply(&self, mut consume_element: impl FnMut(&Self::Ptr) -> Result<()>) -> Result<()> {
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            consume_element(entry).wrap_err(format!(
                "Unable to run function on pointer {} in cornflake",
                i
            ))?;
        }
        Ok(())
    }

    fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        for i in 0..self.num_filled {
            let entry = &self.entries[i];
            ret.extend_from_slice(entry.as_ref());
        }
        ret
    }

    fn replace(&mut self, index: usize, entry: RcCornPtr<'a, D>) {
        assert!(index <= self.num_filled);
        self.entries[index] = entry;
    }
}

pub struct CfBuf<D>
where
    D: Datapath,
{
    buf: D::DatapathPkt,
    offset: usize,
    len: usize,
}

impl<D> PartialEq for CfBuf<D>
where
    D: Datapath,
{
    fn eq(&self, other: &CfBuf<D>) -> bool {
        self.buf == other.get_buf()
            && self.offset == other.get_offset()
            && self.len == other.get_len()
    }
}
impl<D> Eq for CfBuf<D> where D: Datapath {}

impl<D> CfBuf<D>
where
    D: Datapath,
{
    pub fn free_inner(&mut self) {
        self.buf.free_inner();
    }
    pub fn new(buf: D::DatapathPkt) -> Self {
        CfBuf {
            len: buf.buf_size(),
            buf: buf,
            offset: 0,
        }
    }

    #[inline]
    pub fn get_inner(&self) -> &D::DatapathPkt {
        &self.buf
    }

    #[inline]
    pub fn get_len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn new_with_bounds(buf: &D::DatapathPkt, offset: usize, len: usize) -> Self {
        // increment the ref count of the datapath packet
        let owned_buf = buf.clone();
        CfBuf {
            len: len,
            offset: offset,
            buf: owned_buf,
        }
    }

    pub fn clone_with_bounds(&self, offset: usize, len: usize) -> Result<CfBuf<D>> {
        ensure!(
            offset < self.offset,
            "Offset arg: {} cannot be less than current offset: {}",
            offset,
            self.offset
        );
        ensure!(
            len > (self.len - (self.offset - offset)),
            "Invalid length: {}; length must be <= {}",
            len,
            (self.len - (self.offset - offset))
        );
        let mut buf = self.clone();
        buf.set_len(len);
        buf.set_offset(offset);
        Ok(buf)
    }

    pub fn get_buf(&self) -> D::DatapathPkt {
        self.buf.clone()
    }

    /// Assumes len is valid
    pub fn set_len(&mut self, len: usize) {
        self.len = len;
    }

    /// Assumes offset is valid
    pub fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    pub fn allocate(conn: &mut D, size: usize, alignment: usize) -> Result<Self> {
        let buf = conn.allocate(size, alignment)?;
        Ok(CfBuf {
            len: buf.buf_size(),
            buf: buf,
            offset: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<D> Default for CfBuf<D>
where
    D: Datapath,
{
    fn default() -> Self {
        CfBuf {
            buf: D::DatapathPkt::default(),
            offset: 0,
            len: 0,
        }
    }
}

impl<D> Clone for CfBuf<D>
where
    D: Datapath,
{
    fn clone(&self) -> CfBuf<D> {
        //tracing::debug!("Cloning cfbuf with datapath pkt: {:?}", self.buf);
        CfBuf {
            buf: self.buf.clone(),
            offset: self.offset,
            len: self.len,
        }
    }
}

impl<D> Write for CfBuf<D>
where
    D: Datapath,
{
    // TODO: should this just overwrite the entire buffer?
    // Should it keep an index of what has been written of sorts?
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.as_mut().write(&buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<D> AsRef<[u8]> for CfBuf<D>
where
    D: Datapath,
{
    fn as_ref(&self) -> &[u8] {
        &self.buf.as_ref()[self.offset..(self.offset + self.len)]
    }
}

impl<D> AsMut<[u8]> for CfBuf<D>
where
    D: Datapath,
{
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buf.as_mut()[self.offset..(self.offset + self.len)]
    }
}

impl<D> std::fmt::Debug for CfBuf<D>
where
    D: Datapath,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "pkt: {:?}, off: {}, len: {}",
            self.buf, self.offset, self.len
        )
    }
}

/// Some datapaths have their own ref counting schema,
/// So this trait exposes ref counting over those schemes.
pub trait RefCnt {
    // drops inner variable (for debugging)
    fn free_inner(&mut self);

    // TODO: change implementation to get rid of CfBuf object completely
    fn inner_offset(&self) -> usize;

    fn count_rc(&self) -> usize;

    fn change_rc(&mut self, amt: isize);

    fn increase_rc(&mut self, amt: usize) {
        self.change_rc(amt as isize);
    }

    fn decrease_rc(&mut self, amt: usize) {
        self.change_rc((amt as isize) * -1);
    }

    fn increment_rc(&mut self) {
        self.change_rc(1);
    }

    fn decrement_rc(&mut self) {
        self.change_rc(-1);
    }
}

pub struct ReceivedPkt<D>
where
    D: Datapath,
{
    pkts: Vec<D::DatapathPkt>,
    id: MsgID,
    addr: AddressInfo,
}

impl<D> ReceivedPkt<D>
where
    D: Datapath,
{
    pub fn new(pkts: Vec<D::DatapathPkt>, id: MsgID, addr: AddressInfo) -> Self {
        ReceivedPkt {
            pkts: pkts,
            id: id,
            addr: addr,
        }
    }

    pub fn free_inner(&mut self) {
        for pkt in self.pkts.iter_mut() {
            pkt.free_inner();
        }
    }

    pub fn get_id(&self) -> MsgID {
        self.id
    }

    pub fn get_addr(&self) -> AddressInfo {
        self.addr
    }

    pub fn index(&self, idx: usize) -> &D::DatapathPkt {
        &self.pkts[idx]
    }

    pub fn iter(&self) -> Iter<D::DatapathPkt> {
        self.pkts.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<D::DatapathPkt> {
        self.pkts.iter_mut()
    }

    pub fn num_segments(&self) -> usize {
        self.pkts.len()
    }

    pub fn data_len(&self) -> usize {
        let mut ret: usize = 0;
        for pkt in self.pkts.iter() {
            ret += pkt.as_ref().len();
        }
        ret
    }

    pub fn contiguous_repr(&self) -> Vec<u8> {
        let mut ret: Vec<u8> = Vec::new();
        for pkt in self.pkts.iter() {
            ret.append(&mut pkt.as_ref().to_vec());
        }
        ret
    }

    /// Returns the index and offset within index a certain index (into the entire array) appears
    pub fn index_at_offset(&self, offset: usize) -> (usize, usize) {
        let mut cur_idx_size = 0;
        for idx in 0..self.num_segments() {
            if (cur_idx_size + self.index(idx).buf_size()) > offset {
                return (idx, (offset - cur_idx_size));
            }
            cur_idx_size += self.index(idx).buf_size();
        }
        // TODO: possibly use results for better error handling
        tracing::error!(
            data_len = self.data_len(),
            offset = offset,
            "Passed in offset larger than data_len"
        );
        unreachable!();
    }

    /// Returns a contiguous slice in the packet.
    /// Returns an error if the given offset and length cannot create a contiguous slice (e.g.,
    /// would span two segments).
    pub fn contiguous_slice(&self, offset: usize, len: usize) -> Result<&[u8]> {
        let (seg, seg_off) = self.index_at_offset(offset);
        tracing::debug!(
            off = offset,
            len = len,
            seg = seg,
            seg_off = seg_off,
            buf_len = self.index(seg).buf_size(),
            "Params"
        );
        ensure!(
            (self.index(seg).buf_size() - seg_off) >= len,
            "Given params cannot create a contiguous slice, would span two boundaries."
        );
        Ok(&(self.index(seg).as_ref()[seg_off..(seg_off + len)]))
    }
}

impl<D> IntoIterator for ReceivedPkt<D>
where
    D: Datapath,
{
    type Item = D::DatapathPkt;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.pkts.into_iter()
    }
}

/// Set of functions each datapath must implement.
/// Datapaths must be able to send a receive packets,
/// as well as optionally keep track of per-packet timeouts.
pub trait Datapath {
    // Performance debugging of why ECHO is SOOOO much better:
    //  - It doesn't need to allocate any outgoing buffer (just flips buffer on incoming packet)
    //  - Experiment:
    //      - Echo of incoming packet with flip of header vs. allocating + copying (1 copy manual)
    //      - This is a separate issue from just writing a new datapath
    // We want to add:
    // ``serialize_and_send``: takes an object that implements some serialization trait (?)
    // serializes it into a datapath packet (whatever that looks like) and send it
    // We can try two modes for the applications:
    //      - inlining header into the PCIe request (not possible with some datapaths)
    //      - use 2 mbufs for header and non-header when using zero-copy (or 1 mbuf with data
    //      copied in)
    //  Is there some overhead from parsing addr into struct and responding?
    //          - Can we pass the incoming packet in as "reference" for the address buffer?
    //          - This probably isn't adding that much overhead?
    //  Do we need to ALLOCATE any outgoing buffer -- for the inlining case -- we don't need to
    //  allocate any buffer at all and can directly write in the header into the
    //  What does the object need to implement?
    //      - Either: processes zero-copy because the enum expicitly says if it's zero-copy or not
    //      - Or: just has a raw address -- and the datapath figures out how to increment the
    //      reference count once?
    //          - For "set_val" do we need the length of the field?
    //          - For "get_val" --> should the reference count of the object increase? YES: this is
    //          "correctness" -- so perhaps the fact that the metadata is just laid out separately
    //          / contiguously from the data is helpful
    //          - So the API NEEDS to change in one of two ways:
    //              - set_field() either needs to explicitly pass in the field and the handle to
    //              the datapath object where things will be sent -- so the metadata can be queried
    //              - OR set_field() explicitly passes in either a reference or a pointer to
    //              PinnedMetadata
    //          - But why is serialize and send optimal?
    //              - The datapath can make all sorts of optimizations:
    //                  - Directly write in the object header either into an mbuf or into the pci
    //                  request if inlined (don't need to separate allocate a vec and write into
    //                  the vec, which is copied into the mbuf
    //  So the object needs to implement:
    // pub fn serialize_and_send(&mut self, object: impl some trait, addr: outgoing address to
    // send)
    /// Base datapath buffer type.
    type DatapathPkt: AsRef<[u8]>
        + AsMut<[u8]>
        + RefCnt
        + PartialEq
        + Eq
        + Clone
        + std::fmt::Debug
        + Default
        + PtrAttributes;

    type RxPacketAllocator: Send + Clone;

    /// Global initialization necessary before any threads are spawned.
    /// Args:
    /// @config_file: Configuration information.
    /// @num_cores: Number of cores being used.
    fn global_init(
        config_file: &str,
        num_cores: usize,
        app_mode: AppMode,
        remote_ip: Option<Ipv4Addr>,
    ) -> Result<(u16, Vec<(Self::RxPacketAllocator, utils::AddressInfo)>)>;

    fn per_thread_init(
        physical_port: u16,
        config_file: &str,
        app_mode: AppMode,
        use_scatter_gather: bool,
        queue_id: usize,
        packet_allocator: Self::RxPacketAllocator,
        addr_info: utils::AddressInfo,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Send a single buffer to the specified address.
    fn push_buf(&mut self, buf: (MsgID, &[u8]), addr: utils::AddressInfo) -> Result<()>;

    /// Echo back a received packet.
    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized;

    fn push_rc_sgas(&mut self, sga: &Vec<(RcCornflake<Self>, utils::AddressInfo)>) -> Result<()>
    where
        Self: Sized;

    /// Send a scatter-gather array to the specified address.
    fn push_sgas(&mut self, sga: &Vec<(impl ScatterGather, utils::AddressInfo)>) -> Result<()>;

    /// Receive the next packet (from any) underlying `connection`, if any.
    /// Application is responsible for freeing any memory referred to by Cornflake.
    /// None response means no packet received.
    fn pop(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized;

    /// Check if any outstanding packets have timed-out.
    fn timed_out(&self, time_out: Duration) -> Result<Vec<MsgID>>;

    /// Some datapaths use specific timing functions.
    /// This provides an interface to access time.
    fn current_cycles(&self) -> u64;

    /// Number of cycles per second.
    fn timer_hz(&self) -> u64;

    /// Max scatter-gather entries.
    fn max_scatter_entries(&self) -> usize;

    /// Max packet len.
    fn max_packet_len(&self) -> usize;

    /// For debugging purposes, get timers to print at end of execution.
    fn get_timers(&self) -> Vec<Arc<Mutex<HistogramWrapper>>>;

    /// Get destination address information from Ipv4 Address
    fn get_outgoing_addr_from_ip(
        &self,
        dst_addr: &Ipv4Addr,
        udp_port: u16,
    ) -> Result<utils::AddressInfo>;

    /// What is the transport header size for this datapath
    fn get_header_size(&self) -> usize;

    fn allocate_tx(&self, len: usize) -> Result<Self::DatapathPkt>;

    /// Allocate a datapath buffer (registered) with this size and alignment.
    fn allocate(&self, size: usize, alignment: usize) -> Result<Self::DatapathPkt>;

    fn register_external_region(&mut self, metadata: &mut MmapMetadata) -> Result<()>;

    fn unregister_external_region(&mut self, metadata: &MmapMetadata) -> Result<()>;
}

pub fn high_timeout_at_start(received: usize) -> Duration {
    match received < 200 {
        true => Duration::new(10, 0),
        false => Duration::new(0, 1000000),
    }
}

pub fn no_retries_timeout(_received: usize) -> Duration {
    Duration::new(20, 0)
}

/// For applications that want to follow a simple open-loop request processing model at the client,
/// they can implement this trait that defines how the next message to be sent is produced,
/// how received messages are processed.
pub trait ClientSM {
    type Datapath: Datapath;

    fn server_ip(&self) -> Ipv4Addr;

    /// Generate next request to be sent and send it with the provided callback.
    fn get_next_msg(&mut self) -> Result<Option<(MsgID, &[u8])>>;

    /// What to do with a received request.
    fn process_received_msg(
        &mut self,
        sga: ReceivedPkt<<Self as ClientSM>::Datapath>,
        rtt: Duration,
    ) -> Result<()>;

    /// What to do when a particular message times out.
    fn msg_timeout_cb(&mut self, id: MsgID) -> Result<(MsgID, &[u8])>;

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn received_so_far(&self) -> usize;

    fn run_closed_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        num_pkts: u64,
        time_out: impl Fn(usize) -> Duration,
        server_ip: &Ipv4Addr,
        port: u16,
    ) -> Result<()> {
        let mut recved = 0;
        let addr_info = datapath.get_outgoing_addr_from_ip(server_ip, port)?;
        if recved >= num_pkts {
            return Ok(());
        }

        while let Some(msg) = self.get_next_msg()? {
            if recved >= num_pkts {
                break;
            }
            datapath.push_buf(msg, addr_info.clone())?;
            let recved_pkts = loop {
                let pkts = datapath.pop()?;
                if pkts.len() > 0 {
                    break pkts;
                }
                for id in datapath.timed_out(time_out(self.received_so_far()))?.iter() {
                    datapath.push_buf(self.msg_timeout_cb(*id)?, addr_info.clone())?;
                }
            };

            for (pkt, rtt) in recved_pkts.into_iter() {
                let msg_id = pkt.get_id();
                self.process_received_msg(pkt, rtt).wrap_err(format!(
                    "Error in processing received response for pkt {}.",
                    msg_id
                ))?;
                recved += 1;
            }
        }
        Ok(())
    }

    /// Run open loop client
    fn run_open_loop(
        &mut self,
        datapath: &mut Self::Datapath,
        schedule: &PacketSchedule,
        total_time: u64,
        time_out: impl Fn(usize) -> Duration,
        no_retries: bool,
        server_ip: &Ipv4Addr,
        port: u16,
    ) -> Result<()> {
        let addr_info = datapath.get_outgoing_addr_from_ip(server_ip, port)?;
        let mut spin_timer = SpinTimer::new(schedule.clone(), Duration::from_secs(total_time));

        while let Some(msg) = self.get_next_msg()? {
            if spin_timer.done() {
                tracing::debug!("Total time done");
                break;
            }
            // Send the next message
            datapath.push_buf(msg, addr_info.clone())?;

            spin_timer.wait(&mut || {
                let recved_pkts = datapath.pop()?;
                for (pkt, rtt) in recved_pkts.into_iter() {
                    let msg_id = pkt.get_id();
                    self.process_received_msg(pkt, rtt).wrap_err(format!(
                        "Error in processing received response for pkt {}.",
                        msg_id
                    ))?;
                }

                if !no_retries {
                    for id in datapath.timed_out(time_out(self.received_so_far()))?.iter() {
                        datapath.push_buf(self.msg_timeout_cb(*id)?, addr_info.clone())?;
                    }
                }
                Ok(())
            })?;
        }

        tracing::debug!("Finished sending");
        Ok(())
    }
}

/// For server applications that want to follow a simple state-machine request processing model.
pub trait ServerSM {
    type Datapath: Datapath;

    /// Process incoming message, possibly mutating internal state.
    /// Then send the next message with the given callback.
    ///
    /// Arguments:
    /// * sga - Something that implements ScatterGather + ReceivedPkt; used to query the received
    /// message id, received payload, and received message header information.
    /// * send_fn - A send callback to send a response to this request back.
    fn process_requests(
        &mut self,
        sga: Vec<(ReceivedPkt<<Self as ServerSM>::Datapath>, Duration)>,
        datapath: &mut Self::Datapath,
    ) -> Result<()>;

    /// Runs the state machine, which responds to requests in a single-threaded fashion.
    /// Loops on calling the datapath pop function, and then responds to each valid packet.
    /// Returns a result on error.
    ///
    /// Arguments:
    /// * datapath - Object that implements Datapath trait, representing a connection using the
    /// underlying networking stack.
    fn run_state_machine(&mut self, datapath: &mut Self::Datapath) -> Result<()> {
        // run profiler from here
        #[cfg(feature = "profiler")]
        perftools::profiler::reset();
        let mut _last_log: Instant = Instant::now();
        let mut _requests_processed = 0;
        loop {
            #[cfg(feature = "profiler")]
            demikernel::timer!("Run state machine loop");

            #[cfg(feature = "profiler")]
            {
                if _last_log.elapsed() > Duration::from_secs(5) {
                    let d = Instant::now() - _last_log;
                    tracing::info!(
                        "Server processed {} # of reqs since last dump at rate of {:.2} reqs/s",
                        _requests_processed,
                        _requests_processed as f64 / d.as_secs_f64()
                    );
                    perftools::profiler::write(&mut std::io::stdout(), Some(PROFILER_DEPTH))
                        .unwrap();
                    perftools::profiler::reset();
                    _requests_processed = 0;
                    _last_log = Instant::now();
                }
            }
            let pkts = datapath.pop()?;
            if pkts.len() > 0 {
                _requests_processed += pkts.len();
                // give ownership of the received packets to the app.
                self.process_requests(pkts, datapath)?;
            }
        }
    }

    /// Initializes any internal state with any datapath specific configuration,
    /// e.g., registering external memory.
    fn init(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn cleanup(&mut self, connection: &mut Self::Datapath) -> Result<()>;

    fn get_histograms(&self) -> Vec<Arc<Mutex<HistogramWrapper>>>;
}
