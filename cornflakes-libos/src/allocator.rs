use super::{datapath::Datapath, mem};
use ahash::AHashMap;
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

    /// Gets an iterator over 2MB pages in this mempool.
    fn get_2mb_pages(&self) -> Vec<usize>;

    fn get_4k_pages(&self) -> Vec<usize>;

    fn get_1g_pages(&self) -> Vec<usize>;

    /// Turns raw pointer into memory pool's metadata pointer
    fn turn_to_metadata(
        ptr: usize,
        buffer: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>;

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
    /// Map from pool size to list of currently allocated mempool IDs that applications can
    /// allocated from.
    mempool_ids: HashMap<usize, HashSet<MempoolID>>,

    /// HashMap of IDs to actual mempool object (receive mempools and those app can allocate
    /// from)
    mempools: HashMap<MempoolID, M>,

    /// Unique IDs assigned to mempools (assumes the number of mempools added won't overflow)
    next_id_to_allocate: MempoolID,

    /// Mempools that the application cannot allocate from; does not have to be searched for
    /// recovery.
    tx_mempools: BTreeMap<usize, Vec<(MempoolID, M)>>,

    /// Cache from allocated addresses -> corresponding metadata for 2mb pages.
    address_cache_2mb: AHashMap<usize, MempoolID>,

    /// Cache from allocated address -> corresponding mempool ID for 4k pages.
    address_cache_4k: AHashMap<usize, MempoolID>,

    /// Cache from allocated address -> corresponding mempool ID for 1G pages.
    address_cache_1g: AHashMap<usize, MempoolID>,
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
            tx_mempools: BTreeMap::default(),
            address_cache_2mb: AHashMap::default(),
            address_cache_4k: AHashMap::default(),
            address_cache_1g: AHashMap::default(),
        }
    }
}
impl<M> MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool + PartialEq + Eq + std::fmt::Debug,
{
    #[inline]
    fn find_mempool_id(&self, buf: &[u8]) -> Option<MempoolID> {
        match self
            .address_cache_2mb
            .get(&mem::closest_2mb_page(buf.as_ptr()))
        {
            Some(m) => {
                return Some(*m);
            }
            None => {}
        }
        match self
            .address_cache_4k
            .get(&mem::closest_4k_page(buf.as_ptr()))
        {
            Some(m) => {
                return Some(*m);
            }
            None => {}
        }
        match self
            .address_cache_1g
            .get(&mem::closest_1g_page(buf.as_ptr()))
        {
            Some(m) => {
                return Some(*m);
            }
            None => {}
        }
        return None;
    }

    #[inline]
    pub fn add_mempool(&mut self, size: usize, handle: M) -> Result<MempoolID> {
        // add the mempool into the address cache
        for page in handle.get_2mb_pages().into_iter() {
            self.address_cache_2mb
                .insert(page, self.next_id_to_allocate);
        }
        for page in handle.get_4k_pages().into_iter() {
            self.address_cache_4k.insert(page, self.next_id_to_allocate);
        }
        for page in handle.get_1g_pages().into_iter() {
            self.address_cache_1g.insert(page, self.next_id_to_allocate);
        }

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
        // add mempool to address cache
        for page in mempool.get_2mb_pages().iter() {
            self.address_cache_2mb
                .insert(*page, self.next_id_to_allocate);
        }
        for page in mempool.get_4k_pages().iter() {
            self.address_cache_4k
                .insert(*page, self.next_id_to_allocate);
        }
        for page in mempool.get_1g_pages().iter() {
            self.address_cache_1g
                .insert(*page, self.next_id_to_allocate);
        }

        self.mempools.insert(self.next_id_to_allocate, mempool);
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
    /// Adds buffer to address cache for possible recovery.
    #[inline]
    pub fn allocate_buffer(
        &mut self,
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

        match self.find_mempool_id(buffer) {
            Some(id) => {
                let mempool = self.mempools.get(&id).unwrap();
                let metadata = mempool
                    .recover_buffer(buffer)
                    .wrap_err("unable to recover metadata")?;
                return Ok(Some(metadata));
            }
            None => {
                return Ok(None);
            }
        }
        /*{
            #[cfg(feature = "profiler")]
            perftools::timer!("get cached metadata");

            match self.address_cache.get(&(buffer.as_ptr() as usize)) {
                Some(metadata_pointer) => {
                    return Ok(Some(M::turn_to_metadata(*metadata_pointer, buffer)?));
                }
                None => {}
            }
        }*/
        // search through recv mempools first
        /*for (_id, mempool) in self.recv_mempools.iter() {
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
        }*/
    }

    /// Checks whether a given datapath buffer is in registered memory or not.
    /// Checks only receive mempools and mempools with aligned to power of 2 size.
    #[inline]
    pub fn is_registered(&self, buf: &[u8]) -> bool {
        // search through recv mempools first
        match self.find_mempool_id(buf) {
            Some(id) => {
                let mempool = self.mempools.get(&id).unwrap();
                return mempool.is_registered();
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
