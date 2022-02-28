use super::{
    super::{access, mlx5_bindings::*},
    connection::Mlx5Buffer,
    sizes::align_up,
};
use color_eyre::eyre::{ensure, Result};
use hashbrown::HashMap;

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
    pub unsafe fn get_data_mempool(&self) -> *mut mempool {
        get_data_mempool(self.mempool)
    }

    #[inline]
    pub unsafe fn get_metadata_mempool(&self) -> *mut mempool {
        get_metadata_mempool(self.mempool)
    }

    #[inline]
    pub unsafe fn check_mempool(&self, mempool: *mut mempool) -> bool {
        get_data_mempool(self.mempool) == mempool
    }

    pub unsafe fn alloc_data(&mut self) -> Result<Option<Mlx5Buffer>> {
        let buf = alloc_data_buf(self.mempool);
        if buf.is_null() {
            return Ok(None);
        } else {
            return Ok(Some(Mlx5Buffer::new(buf, self.mempool, 0)));
        }
    }

    /*#[inline]
    pub unsafe fn item_len(&self) -> usize {
        access!(get_data_mempool(self.mempool), item_len) as _
    }

    #[inline]
    pub unsafe fn available(&self) -> usize {
        let data_pool = get_data_mempool(self.mempool);
        access!(data_pool, capacity, usize) - access!(data_pool, allocated, usize)
    }*/

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
    pub fn add_mempool(
        &mut self,
        item_len: usize,
        registered_mempool: *mut registered_mempool,
    ) -> Result<()> {
        let data_mempool = DataMempool::new(registered_mempool)?;
        if self.mempools.contains_key(&item_len) {
            let mempool_list = self.mempools.get_mut(&item_len).unwrap();
            mempool_list.push(data_mempool);
        } else {
            self.mempools.insert(item_len, vec![data_mempool]);
        }
        Ok(())
    }

    pub fn allocate_data_buffer(
        &mut self,
        size: usize,
        alignment: usize,
    ) -> Result<Option<Mlx5Buffer>> {
        let min_item_size = align_up(size, alignment);
        let mut mempool_size: usize = std::usize::MAX;
        for (item_len, _) in self.mempools.iter() {
            if *item_len < mempool_size && *item_len >= min_item_size {
                mempool_size = *item_len;
            }
        }
        if mempool_size == std::usize::MAX {
            return Ok(None);
        }
        for mempool in self.mempools.get_mut(&mempool_size).unwrap().iter_mut() {
            match unsafe { mempool.alloc_data()? } {
                Some(buf) => {
                    return Ok(Some(buf));
                }
                None => {}
            }
        }
        return Ok(None);
    }
}
