use super::super::{access, mlx5_bindings::*};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataMempool {
    mempool: *mut registered_mempool,
}

impl DataMempool {
    pub fn new(registered_mempool: *mut registered_mempool) -> Result<Self> {
        ensure!(
            !registered_mempool.is_null(),
            "Registered mempool pointer passed in is null.",
        );
        Ok(DataMempool {
            mempool: registered_mempool,
        })
    }

    #[inline]
    pub unsafe fn alloc(&self) -> Result<*mut mbuf> {
        let mbuf = allocate_data_and_metadata_mbuf(self.mempool);
        ensure!(!mbuf.is_null(), "Allocated mbuf from metadata pool is null");
        Ok(mbuf)
    }

    #[inline]
    pub unsafe fn item_size(&self) -> usize {
        access!(get_data_mempool(self.mempool), item_len) as _
    }

    #[inline]
    pub unsafe fn available(&self) -> usize {
        let data_pool = get_data_mempool(self.mempool);
        access!(data_pool, capacity, usize) - access!(data_pool, allocated, usize)
    }

    #[inline]
    pub unsafe fn is_within_bounds(&self, buf: &[u8]) -> bool {
        self.is_within_bounds_ptr(buf.as_ptr())
    }

    #[inline]
    pub unsafe fn is_within_bounds_ptr(&self, addr: *const u8) -> bool {
        let ptr = addr as usize;
        let data_pool = get_data_mempool(self.mempool);
        ptr >= access!(data_pool, buf, *const u8) as usize
            && ptr <= (access!(data_pool, buf, *const u8) as usize + access!(data_pool, len, usize))
    }

    /// Given an arbitrary data pointer, returns the metadata pointer.
    /// Assumes that the data pointer is within bounds of the memory pool.
    #[inline]
    pub unsafe fn recover_mbuf_metadata(&self, ptr: *const u8) -> *mut mbuf {
        let data_pool = get_data_mempool(self.mempool);
        let metadata_pool = get_metadata_mempool(self.mempool);
        let mempool_start = access!(data_pool, buf, *const u8) as usize;
        let item_len = access!(data_pool, item_len, usize);
        let offset_within_alloc = (ptr as usize - mempool_start) % item_len;
        let index = ((ptr as usize - offset_within_alloc) - mempool_start) / item_len;
        mbuf_at_index(metadata_pool, index)
    }
}

pub struct MemoryAllocator {
    /// keeps track of memory pools
    // TODO: keep track of memory pools and finding addresses via some better data structure (e.g.
    // an index?)
    mempools: HashMap<usize, Vec<DataMempool>>,
}

impl Default for MemoryAllocator {
    fn default() -> Self {
        MemoryAllocator {
            mempools: HashMap::default(),
        }
    }
}

impl MemoryAllocator {
    pub fn add_mempool(&mut self, obj_size: usize, alignment: usize) -> Result<()> {
        Ok(())
    }
}
