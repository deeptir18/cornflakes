use super::datapath::Datapath;
use color_eyre::eyre::{bail, Result, WrapErr};
use hashbrown::HashMap;
use itertools::Itertools;
use std::collections::HashSet;

pub type MempoolID = u32;

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
    fn alloc_data_buf(
        &mut self,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>;
}

/// Datapaths can use this struct as their allocator, given an implementation of a
/// DatapathMemoryPool.
pub struct MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool,
{
    /// Map from pool size to list of currently allocated mempool IDs
    mempool_ids: HashMap<usize, HashSet<MempoolID>>,

    /// HashMap of IDs to actual mempool object
    mempools: HashMap<MempoolID, M>,

    /// Unique IDs assigned to mempools (assumes the number of mempools added won't overflow)
    next_id_to_allocate: MempoolID,

    /// Mempools that cannot be allocated from, but must be searched for recovery.
    recv_mempools: Vec<M>,
}

impl<M> Default for MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool,
{
    fn default() -> Self {
        MemoryPoolAllocator {
            mempool_ids: HashMap::default(),
            mempools: HashMap::default(),
            next_id_to_allocate: 0,
            recv_mempools: Vec::default(),
        }
    }
}
impl<M> MemoryPoolAllocator<M>
where
    M: DatapathMemoryPool,
{
    pub fn add_mempool(&mut self, size: usize, handle: M) -> Result<MempoolID> {
        match self.mempool_ids.get_mut(&size) {
            Some(mempool_ids_list) => {
                mempool_ids_list.insert(self.next_id_to_allocate);
                self.mempools.insert(self.next_id_to_allocate, handle);
                self.next_id_to_allocate += 1;
            }
            None => {
                let mut set = HashSet::default();
                set.insert(self.next_id_to_allocate);
                self.mempool_ids.insert(size, set);
                self.mempools.insert(self.next_id_to_allocate, handle);
                self.next_id_to_allocate += 1;
            }
        }
        Ok(self.next_id_to_allocate - 1)
    }

    pub fn add_recv_mempool(&mut self, mempool: M) {
        self.recv_mempools.push(mempool);
        tracing::debug!("Pushing recv mempool into vector");
    }

    /// Registers the backing region behind the given mempool.
    /// If already registered, does nothing.
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

    /// Allocates a datapath buffer (if a mempool is available).
    /// Size is atleast buf_size.
    /// No guarantees on whether the resulting datapath buffer is registered or not.
    /// TODO: is there a more optimal way to keep track of this data structure
    pub fn allocate_buffer(
        &mut self,
        buf_size: usize,
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>> {
        let mempool_sizes: Vec<&usize> = self
            .mempool_ids
            .keys()
            .sorted()
            .filter(|size| {
                tracing::debug!("Size: {}, given size: {}", *size, buf_size);
                *size >= &buf_size
            })
            .collect();

        for size in mempool_sizes {
            for mempool_id in self.mempool_ids.get(size).unwrap() {
                let mempool = self.mempools.get_mut(mempool_id).unwrap();
                match mempool.alloc_data_buf()? {
                    Some(x) => {
                        tracing::debug!("Successfully allocated from mempool size {}", size);
                        return Ok(Some(x));
                    }
                    None => {}
                }
            }
        }
        return Ok(None);
    }

    /// Returns metadata associated with this buffer, if it exists.
    pub fn recover_buffer(
        &self,
        buffer: &[u8],
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>>
    {
        // search through recv mempools first
        for mempool in self.recv_mempools.iter() {
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

        let mempool_sizes: Vec<&usize> = self
            .mempool_ids
            .keys()
            .sorted()
            .filter(|size| *size >= &buffer.len())
            .collect();
        for size in mempool_sizes {
            for mempool_id in self.mempool_ids.get(size).unwrap() {
                let mempool = self.mempools.get(mempool_id).unwrap();
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

    /// Consumes a datapath buffer object and returns a corresponding datapath metadata object.
    ///
    /// Returns:
    /// Datapath metadata object IF corresponding metadata object was found within registered
    /// regions.
    /// None if buffer was not found within registered regions (and buffer is dropped).
    pub fn get_metadata(
        &self,
        buf: <<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<Option<<<M as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata>>
    {
        // search through recv mempools first
        for mempool in self.recv_mempools.iter() {
            if mempool.is_buf_within_bounds(buf.as_ref()) {
                if mempool.is_registered() {
                    let metadata = mempool
                        .recover_metadata(buf)
                        .wrap_err("unable to recover metadata")?;
                    return Ok(Some(metadata));
                } else {
                    return Ok(None);
                }
            }
        }

        let mempool_sizes: Vec<&usize> = self
            .mempool_ids
            .keys()
            .sorted()
            .filter(|size| *size >= &buf.as_ref().len())
            .collect();
        for size in mempool_sizes {
            for mempool_id in self.mempool_ids.get(size).unwrap() {
                let mempool = self.mempools.get(mempool_id).unwrap();
                if mempool.is_buf_within_bounds(buf.as_ref()) {
                    if mempool.is_registered() {
                        let metadata = mempool
                            .recover_metadata(buf)
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

    /// Checks whether a given datapath buffer is in registered memory or not.
    pub fn is_registered(&self, buf: &[u8]) -> bool {
        // search through recv mempools first
        for mempool in self.recv_mempools.iter() {
            if mempool.is_buf_within_bounds(buf.as_ref()) {
                if mempool.is_registered() {
                    return true;
                } else {
                    return false;
                }
            }
        }

        let mempool_sizes: Vec<&usize> = self
            .mempool_ids
            .keys()
            .sorted()
            .filter(|size| *size >= &buf.len())
            .collect();
        for size in mempool_sizes {
            for mempool_id in self.mempool_ids.get(size).unwrap() {
                let mempool = self.mempools.get(mempool_id).unwrap();
                if mempool.is_buf_within_bounds(buf) {
                    if mempool.is_registered() {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    pub fn num_mempools_so_far(&self) -> u32 {
        self.next_id_to_allocate
    }
}
