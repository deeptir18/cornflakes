use super::{
    super::{access, mlx5_bindings::*},
    connection::{MbufMetadata, Mlx5Buffer, Mlx5Connection, Mlx5PerThreadContext},
    sizes,
};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    allocator::{DatapathMemoryPool, MempoolID},
    datapath::Datapath,
};
use std::boxed::Box;

#[derive(Debug, PartialEq, Eq)]
pub struct DataMempool {
    mempool_ptr: *mut [u8],
}

impl Drop for DataMempool {
    fn drop(&mut self) {
        // (a) drop pages behind mempool itself
        // (b) drop box allocated for registered mempool pointer
        unsafe {
            if custom_mlx5_deregister_and_free_registered_mempool(self.mempool()) != 0 {
                tracing::warn!(
                    "Failed to deregister and free backing mempool at {:?}",
                    self.mempool()
                );
            }
            tracing::warn!("Dropping data mempool {:?}", self.mempool_ptr);
            Box::from_raw(self.mempool_ptr);
        }
    }
}

impl DataMempool {
    fn mempool(&self) -> *mut registered_mempool {
        self.mempool_ptr as *mut registered_mempool
    }

    pub fn new_from_ptr(mempool_ptr: *mut [u8]) -> Self {
        DataMempool {
            mempool_ptr: mempool_ptr,
        }
    }

    pub fn new(
        mempool_params: &sizes::MempoolAllocationParams,
        per_thread_context: &Mlx5PerThreadContext,
    ) -> Result<Self> {
        let mempool_box =
            vec![0u8; unsafe { custom_mlx5_get_registered_mempool_size() } as _].into_boxed_slice();
        let mempool_ptr = Box::<[u8]>::into_raw(mempool_box);
        if unsafe {
            custom_mlx5_alloc_and_register_tx_pool(
                per_thread_context.get_context_ptr(),
                mempool_ptr as _,
                mempool_params.get_item_len() as _,
                mempool_params.get_num_items() as _,
                mempool_params.get_data_pgsize() as _,
                mempool_params.get_metadata_pgsize() as _,
                ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _,
            )
        } != 0
        {
            tracing::warn!(
                "Failed to register mempool with params {:?}",
                mempool_params
            );
            unsafe {
                Box::from_raw(mempool_ptr);
            }
            bail!("Failed register mempool with params {:?}", mempool_params);
        }
        Ok(DataMempool {
            mempool_ptr: mempool_ptr,
        })
    }

    #[inline]
    pub unsafe fn get_data_mempool(&self) -> *mut custom_mlx5_mempool {
        get_data_mempool(self.mempool())
    }

    /*#[inline]
    pub unsafe fn get_metadata_mempool(&self) -> *mut custom_mlx5_mempool {
        get_metadata_mempool(self.mempool)
    }*/

    /*#[inline]
    pub unsafe fn check_mempool(&self, mempool: *mut custom_mlx5_mempool) -> bool {
        get_data_mempool(self.mempool) == mempool
    }*/

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

    type RegistrationContext = *mut custom_mlx5_per_thread_context;

    fn register(&mut self, registration_context: Self::RegistrationContext) -> Result<()> {
        unsafe {
            if custom_mlx5_register_memory_pool_from_thread(
                registration_context,
                self.mempool(),
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
            if custom_mlx5_deregister_memory_pool(self.mempool()) != 0 {
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
        let data_pool = unsafe { get_data_mempool(self.mempool()) };
        unsafe {
            ptr >= access!(data_pool, buf, *const u8) as usize
                && ptr
                    <= (access!(data_pool, buf, *const u8) as usize
                        + access!(data_pool, len, usize))
        }
    }

    fn recover_metadata(
        &self,
        buf: <<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        self.recover_buffer(buf.as_ref())
    }

    /// Recovers buffer into datapath metadata IF the buffer is registered and within bounds.
    /// MUST be called ONLY if the buffer is registered and within bounds.
    fn recover_buffer(
        &self,
        buf: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        let ptr = buf.as_ptr();
        let data_pool = unsafe { get_data_mempool(self.mempool()) };
        let metadata_pool = unsafe { get_metadata_mempool(self.mempool()) };
        let mempool_start = unsafe { access!(data_pool, buf, *const u8) as usize };
        let item_len = unsafe { access!(data_pool, item_len, usize) };
        let offset_within_alloc = (ptr as usize - mempool_start) % item_len;
        let index = ((ptr as usize - offset_within_alloc) - mempool_start) / item_len;
        let mbuf = unsafe { custom_mlx5_mbuf_at_index(metadata_pool, index) };
        let offset = ptr as usize - unsafe { access!(mbuf, buf_addr, *const u8) as usize };
        MbufMetadata::new(mbuf, offset, Some(buf.len()))
    }

    fn alloc_data_buf(
        &self,
        context: MempoolID,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>
    {
        let buffer = unsafe { alloc_data_buf(self.mempool()) };
        if buffer.is_null() {
            return Ok(None);
        }
        tracing::debug!(
            item_len = unsafe { access!(get_data_mempool(self.mempool()), item_len, usize) },
            buffer =? buffer,
            "Allocating from mempool with item len"
        );
        Ok(Some(Mlx5Buffer::new(buffer, self.mempool(), 0, context)))
    }
}
