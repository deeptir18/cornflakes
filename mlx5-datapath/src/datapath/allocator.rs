use super::{
    super::{access, mlx5_bindings::*},
    connection::{MbufMetadata, Mlx5Buffer, Mlx5Connection},
    sizes::align_up,
};
use color_eyre::eyre::{bail, ensure, Result};
use cornflakes_libos::{allocator::DatapathMemoryPool, datapath::Datapath};
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

    /*#[inline]
    pub unsafe fn get_metadata_mempool(&self) -> *mut mempool {
        get_metadata_mempool(self.mempool)
    }*/

    /*#[inline]
    pub unsafe fn check_mempool(&self, mempool: *mut mempool) -> bool {
        get_data_mempool(self.mempool) == mempool
    }*/

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
}

impl DatapathMemoryPool for DataMempool {
    type DatapathImpl = Mlx5Connection;

    type RegistrationContext = *mut mlx5_per_thread_context;

    fn register(&mut self, registration_context: Self::RegistrationContext) -> Result<()> {
        unsafe {
            if register_memory_pool_from_thread(
                registration_context,
                self.mempool,
                ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _,
            ) != 0
            {
                bail!("Failed to register mempool");
            }
        }
        Ok(())
    }

    fn unregister(&mut self) -> Result<()> {
        unsafe {
            if deregister_memory_pool(self.mempool) != 0 {
                bail!("Failed to deregister memory pool");
            }
        }
        Ok(())
    }

    fn is_registered(&self) -> bool {
        unsafe {
            let data_mempool = self.get_data_mempool();
            if access!(data_mempool, lkey, i32) != -1 {
                return true;
            } else {
                return false;
            }
        }
    }

    fn is_buf_within_bounds(&self, buf: &[u8]) -> bool {
        let ptr = buf.as_ptr() as usize;
        let data_pool = unsafe { get_data_mempool(self.mempool) };
        unsafe {
            ptr >= access!(data_pool, buf, *const u8) as usize
                && ptr
                    <= (access!(data_pool, buf, *const u8) as usize
                        + access!(data_pool, len, usize))
        }
    }

    /// Recovers buffer into datapath metadata IF the buffer is registered and within bounds.
    /// MUST be called ONLY if the buffer is registered and within bounds.
    fn recover_buffer(
        &self,
        buf: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        let ptr = buf.as_ptr();
        let data_pool = unsafe { get_data_mempool(self.mempool) };
        let metadata_pool = unsafe { get_metadata_mempool(self.mempool) };
        let mempool_start = unsafe { access!(data_pool, buf, *const u8) as usize };
        let item_len = unsafe { access!(data_pool, item_len, usize) };
        let offset_within_alloc = (ptr as usize - mempool_start) % item_len;
        let index = ((ptr as usize - offset_within_alloc) - mempool_start) / item_len;
        let mbuf = unsafe { mbuf_at_index(metadata_pool, index) };
        let offset = ptr as usize - unsafe { access!(mbuf, buf_addr, *const u8) as usize };
        MbufMetadata::new(mbuf, offset, Some(buf.len()))
    }

    fn alloc_data_buf(
        &mut self,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>
    {
        let buffer = unsafe { alloc_data_buf(self.mempool) };
        if buffer.is_null() {
            return Ok(None);
        }
        Ok(Some(Mlx5Buffer::new(buffer, self.mempool, 0)))
    }
}
