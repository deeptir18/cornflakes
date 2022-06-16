use super::datapath::{Datapath, ExposeMempoolID};
use color_eyre::eyre::{bail, Result, WrapErr};
use hashbrown::HashMap;
#[cfg(feature = "profiler")]
use perftools;
use std::collections::{BTreeMap, HashSet};

pub type MempoolID = u32;
pub fn align_to_pow2(x: usize) -> usize {
    if x & (x - 1) == 0 {
        return x + (x == 0) as usize;
    }
    let mut size = x;
    size -= 1;
    size |= size >> 1;
    size |= size >> 2;
    size |= size >> 4;
    size |= size >> 8;
    size |= size >> 16;
    size |= size >> 32;
    size += 1;
    size += (size == 0) as usize;
    size
}

pub fn align_up(x: usize, align_size: usize) -> usize {
    let divisor = x / align_size;
    if (divisor * align_size) < x {
        return (divisor + 1) * align_size;
    } else {
        assert!(divisor * align_size == x);
        return x;
    }
}

/// Trait that datapath's can implement to pr
pub trait DatapathMemoryPool {
    type DatapathImpl: Datapath;
    type RegistrationContext;
    /// Register the backing region behind this memory pool
    fn register(&mut self, registration_context: Self::RegistrationContext) -> Result<()>;

    /// Unregister the backing region behind this memory pool
    fn unregister(&mut self) -> Result<()>;

    /// Whether the entire backing region of the memory pool is registered
    fn is_registered(&self) -> bool;

    fn has_allocated(&self) -> bool;

    /// Is a specific buffer within bounds
    fn is_buf_within_bounds(&self, buf: &[u8]) -> bool;

    fn recover_metadata(
        &self,
        buf: <<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>;

    /// Recovers buffer into datapath metadata IF the buffer is registered and within bounds.
    /// MUST be called ONLY if the buffer is registered and within bounds.
    fn recover_buffer(
        &self,
        buf: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>;

    /// Allocate datapath buffer
    /// Given context to refer back to memory pool.
    fn alloc_data_buf(
        &self,
        context: MempoolID,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>;
}

/// Datapaths can use this struct as their allocator, given an implementation of a
/// DatapathMemoryPool.
#[derive(Debug, PartialEq, Eq)]
pub struct MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool + PartialEq + Eq + std::fmt::Debug,
{
    /// Map from pool size to list of currently allocated mempool IDs
    mempool_ids: HashMap<usize, HashSet<MempoolID>>,

    /// HashMap of IDs to actual mempool object
    mempools: HashMap<MempoolID, M>,

    /// Unique IDs assigned to mempools (assumes the number of mempools added won't overflow)
    next_id_to_allocate: MempoolID,

    /// Mempools that cannot be allocated from, but must be searched for recovery.
    recv_mempools: HashMap<MempoolID, M>,

    /// Mempools that the application cannot allocate from; does not have to be searched for
    /// recovery.
    tx_mempools: BTreeMap<usize, Vec<(MempoolID, M)>>,
}

impl<M> Default for MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool + PartialEq + Eq + std::fmt::Debug,
{
    fn default() -> Self {
        MemoryPoolAllocator {
            mempool_ids: HashMap::default(),
            mempools: HashMap::default(),
            next_id_to_allocate: 0,
            recv_mempools: HashMap::default(),
            tx_mempools: BTreeMap::default(),
        }
    }
}
impl<M> MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool + PartialEq + Eq + std::fmt::Debug,
{
    #[inline]
    pub fn add_mempool(&mut self, size: usize, handle: M) -> Result<MempoolID> {
        let aligned_size = align_to_pow2(size);
        tracing::info!("Adding mempool of size {}", aligned_size);
        match self.mempool_ids.get_mut(&aligned_size) {
            Some(mempool_ids_list) => {
                mempool_ids_list.insert(self.next_id_to_allocate);
                self.mempools.insert(self.next_id_to_allocate, handle);
                self.next_id_to_allocate += 1;
            }
            None => {
                let mut set = HashSet::default();
                set.insert(self.next_id_to_allocate);
                self.mempool_ids.insert(aligned_size, set);
                self.mempools.insert(self.next_id_to_allocate, handle);
                self.next_id_to_allocate += 1;
            }
        }
        Ok(self.next_id_to_allocate - 1)
    }

    #[inline]
    pub fn add_recv_mempool(&mut self, mempool: M) {
        self.recv_mempools.insert(self.next_id_to_allocate, mempool);
        self.next_id_to_allocate += 1;
    }

    #[inline]
    pub fn add_tx_mempool(&mut self, item_len: usize, mempool: M) {
        tracing::info!("Adding mempool of size {}", item_len);
        if self.tx_mempools.contains_key(&item_len) {
            let list = self.tx_mempools.get_mut(&item_len).unwrap();
            list.push((self.next_id_to_allocate, mempool));
        } else {
            self.tx_mempools
                .insert(item_len, vec![(self.next_id_to_allocate, mempool)]);
        }
        self.next_id_to_allocate += 1;
    }

    /// Registers the backing region behind the given mempool.
    /// If already registered, does nothing.
    #[inline]
    pub fn register(&mut self, mempool: MempoolID, context: M::RegistrationContext) -> Result<()> {
        match self.mempools.get_mut(&mempool) {
            Some(handle) => {
                handle.register(context)?;
            }
            None => {
                bail!(
                    "Mempool allocator does not contain mempool ID {:?}",
                    mempool
                );
            }
        }
        Ok(())
    }

    /// Unregisters the backing region behind the given mempool.
    /// If already unregistered, does nothing.
    #[inline]
    pub fn unregister(&mut self, mempool: MempoolID) -> Result<()> {
        match self.mempools.get_mut(&mempool) {
            Some(handle) => {
                handle.unregister()?;
            }
            None => {
                bail!(
                    "Mempool allocator does not contain mempool ID {:?}",
                    mempool
                );
            }
        }
        Ok(())
    }

    #[inline]
    pub fn allocate_tx_buffer(
        &self,
        buf_size: usize,
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>> {
        let align_size = align_to_pow2(buf_size);
        match self.tx_mempools.get(&align_size) {
            Some(mempools) => {
                for (mempool_id, mempool) in mempools.iter() {
                    match mempool.alloc_data_buf(*mempool_id)? {
                        Some(x) => {
                            return Ok(Some(x));
                        }
                        None => {}
                    }
                }
            }
            None => {}
        }
        for (_size, mempools) in self
            .tx_mempools
            .iter()
            .filter(|(size, _list)| *size >= &buf_size)
        {
            tracing::debug!("Looking for tx buffer to allocate {}", buf_size);
            for (mempool_id, mempool) in mempools.iter() {
                match mempool.alloc_data_buf(*mempool_id)? {
                    Some(x) => {
                        return Ok(Some(x));
                    }
                    None => {}
                }
            }
        }
        tracing::warn!(
            "Returning none: tx mempools :{:?}, align_size: {}, actual size: {}",
            self.tx_mempools,
            align_size,
            buf_size,
        );
        return Ok(None);
    }

    /// Allocates a datapath buffer (if a mempool is available).
    /// Size will be aligned to the next power of 2. If no buffers available in that mempool, will
    /// return None.
    /// No guarantees on whether the resulting datapath buffer is registered or not.
    #[inline]
    pub fn allocate_buffer(
        &self,
        buf_size: usize,
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>> {
        let align_size = align_to_pow2(buf_size);
        match self.mempool_ids.get(&align_size) {
            Some(mempools) => {
                for mempool_id in mempools.iter() {
                    let mempool = self.mempools.get(mempool_id).unwrap();
                    match mempool.alloc_data_buf(*mempool_id)? {
                        Some(x) => {
                            return Ok(Some(x));
                        }
                        None => {}
                    }
                }
                return Ok(None);
            }
            None => {
                tracing::debug!("Returning none");
                return Ok(None);
            }
        }
    }

    /// Returns metadata associated with this buffer, even if the buffer is not registered. Only
    /// searches through recv mempools and mempools with size aligned up to next power of 2.
    #[inline]
    pub fn recover_metadata(
        &self,
        buffer: &[u8],
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>>
    {
        #[cfg(feature = "profiler")]
        perftools::timer!("recover buffer func higher level allocator");
        // search through recv mempools first
        for (_id, mempool) in self.recv_mempools.iter() {
            if mempool.is_buf_within_bounds(buffer) {
                let metadata = mempool
                    .recover_buffer(buffer)
                    .wrap_err("unable to recover metadata")?;
                return Ok(Some(metadata));
            }
        }

        let align_size = align_to_pow2(buffer.len());
        match self.mempool_ids.get(&align_size) {
            Some(mempools) => {
                for mempool_id in mempools.iter() {
                    let mempool = self.mempools.get(mempool_id).unwrap();
                    if mempool.has_allocated() {
                        if mempool.is_buf_within_bounds(buffer) {
                            let metadata = mempool
                                .recover_buffer(buffer)
                                .wrap_err("unable to recover metadata")?;
                            return Ok(Some(metadata));
                        }
                    }
                }
                return Ok(None);
            }
            None => {
                return Ok(None);
            }
        }
    }

    /// Returns metadata associated with this buffer, if it exists, and the buffer is
    /// registered. Only looks at mempools with aligned size and receive mempools.
    #[inline]
    pub fn recover_buffer(
        &self,
        buffer: &[u8],
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>>
    {
        #[cfg(feature = "profiler")]
        perftools::timer!("recover buffer func higher level allocator");
        // search through recv mempools first
        for (_id, mempool) in self.recv_mempools.iter() {
            if mempool.is_buf_within_bounds(buffer) {
                if mempool.is_registered() {
                    let metadata = mempool
                        .recover_buffer(buffer)
                        .wrap_err("unable to recover metadata")?;
                    return Ok(Some(metadata));
                } else {
                    return Ok(None);
                }
            }
        }

        let align_size = align_to_pow2(buffer.len());
        match self.mempool_ids.get(&align_size) {
            Some(mempools) => {
                for mempool_id in mempools.iter() {
                    let mempool = self.mempools.get(mempool_id).unwrap();
                    if mempool.has_allocated() {
                        if mempool.is_buf_within_bounds(buffer) {
                            if mempool.is_registered() {
                                let metadata = mempool
                                    .recover_buffer(buffer)
                                    .wrap_err("unable to recover metadata")?;
                                return Ok(Some(metadata));
                            } else {
                                return Ok(None);
                            }
                        }
                    }
                }
                return Ok(None);
            }
            None => {
                return Ok(None);
            }
        }
    }

    /// Consumes a datapath buffer object and returns a corresponding datapath metadata object.
    ///
    /// Returns:
    /// Datapath metadata object IF corresponding metadata object was found within registered
    /// regions.
    /// None if buffer was not found within registered regions (and buffer is dropped).
    #[inline]
    pub fn get_metadata(
        &self,
        buf: <<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>>
    {
        let mempool_id = buf.get_mempool_id();
        match self.recv_mempools.get(&mempool_id) {
            Some(mempool) => {
                if mempool.is_registered() {
                    let metadata = mempool
                        .recover_metadata(buf)
                        .wrap_err("Unable to recover metadata")?;
                    return Ok(Some(metadata));
                } else {
                    return Ok(None);
                }
            }
            None => {}
        }
        match self.mempools.get(&mempool_id) {
            Some(mempool) => {
                if mempool.is_registered() {
                    let metadata = mempool
                        .recover_metadata(buf)
                        .wrap_err("Unable to recover metadata")?;
                    return Ok(Some(metadata));
                } else {
                    return Ok(None);
                }
            }
            None => {}
        }
        return Ok(None);
    }

    /// Checks whether a given datapath buffer is in registered memory or not.
    /// Checks only receive mempools and mempools with aligned to power of 2 size.
    #[inline]
    pub fn is_registered(&self, buf: &[u8]) -> bool {
        // search through recv mempools first
        for (_i, mempool) in self.recv_mempools.iter() {
            if mempool.is_buf_within_bounds(buf.as_ref()) {
                return mempool.is_registered();
            }
        }

        let align_size = align_to_pow2(buf.len());
        match self.mempool_ids.get(&align_size) {
            Some(mempools) => {
                for mempool_id in mempools.iter() {
                    let mempool = self.mempools.get(mempool_id).unwrap();
                    if mempool.is_buf_within_bounds(buf) {
                        return mempool.is_registered();
                    }
                }
                return false;
            }
            None => {
                return false;
            }
        }
    }

    #[inline]
    pub fn num_mempools_so_far(&self) -> u32 {
        self.next_id_to_allocate
    }

    #[inline]
    pub fn has_mempool(&self, size: usize) -> bool {
        return self.mempool_ids.contains_key(&size);
    }
}
