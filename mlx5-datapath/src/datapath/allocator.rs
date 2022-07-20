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
    #[inline]
    fn mempool(&self) -> *mut registered_mempool {
        self.mempool_ptr as *mut registered_mempool
    }

    #[inline]
    pub fn new_from_ptr(mempool_ptr: *mut [u8]) -> Self {
        tracing::info!(
            "New mempool at ptr from ptr: {:?}, data_mempool ptr: {:?}",
            mempool_ptr,
            unsafe { get_data_mempool(mempool_ptr as *mut registered_mempool) }
        );
        DataMempool {
            mempool_ptr: mempool_ptr,
        }
    }

    #[inline]
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
        tracing::info!(
            "New mempool at ptr: {:?}, data_mempool ptr: {:?}",
            mempool_ptr,
            unsafe { get_data_mempool(mempool_ptr as *mut registered_mempool) }
        );
        Ok(DataMempool {
            mempool_ptr: mempool_ptr,
        })
    }

    #[inline]
    pub unsafe fn get_data_mempool(&self) -> *mut custom_mlx5_mempool {
        get_data_mempool(self.mempool())
    }

    #[inline]
    pub unsafe fn recover_metadata_mbuf(
        &self,
        ptr: *const u8,
    ) -> (*mut ::std::os::raw::c_void, usize, usize) {
        let mempool = self.mempool();
        let data_pool = get_data_mempool(mempool);
        let mempool_start = access!(data_pool, buf, usize);
        let item_len = access!(data_pool, item_len, usize);
        let offset_within_alloc = ptr as usize - mempool_start;
        let index =
            (offset_within_alloc & !(item_len - 1)) >> access!(data_pool, log_item_len, usize);
        let data_ptr = (mempool_start + (index << access!(data_pool, log_item_len, usize)))
            as *mut std::os::raw::c_void;
        (data_ptr, index, ptr as usize - data_ptr as usize)
    }
}

impl DatapathMemoryPool for DataMempool {
    type DatapathImpl = Mlx5Connection;

    type RegistrationContext = *mut custom_mlx5_per_thread_context;

    #[inline]
    fn get_2mb_pages(&self) -> Vec<usize> {
        let mempool = self.mempool();
        let data_pool = unsafe { get_data_mempool(mempool) };
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != cornflakes_libos::mem::PGSIZE_2MB {
            return vec![];
        }
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    #[inline]
    fn get_4k_pages(&self) -> Vec<usize> {
        let mempool = self.mempool();
        let data_pool = unsafe { get_data_mempool(mempool) };
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != cornflakes_libos::mem::PGSIZE_4KB {
            return vec![];
        }
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    #[inline]
    fn get_1g_pages(&self) -> Vec<usize> {
        let mempool = self.mempool();
        let data_pool = unsafe { get_data_mempool(mempool) };
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != cornflakes_libos::mem::PGSIZE_1GB {
            return vec![];
        }
        tracing::info!("In get 1g pages");
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    #[inline]
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

    #[inline]
    fn unregister(&mut self) -> Result<()> {
        unsafe {
            if custom_mlx5_deregister_memory_pool(self.mempool()) != 0 {
                bail!("Failed to deregister memory pool");
            }
        }
        Ok(())
    }

    #[inline]
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

    #[inline(always)]
    fn has_allocated(&self) -> bool {
        unsafe { access!(get_data_mempool(self.mempool()), allocated, usize) >= 1 }
    }

    #[inline]
    fn is_buf_within_bounds(&self, buf: &[u8]) -> bool {
        let ptr = buf.as_ptr() as usize;
        let data_pool = unsafe { get_data_mempool(self.mempool()) };
        unsafe {
            access!(data_pool, allocated, usize) >= 1
                && ptr >= access!(data_pool, buf, *const u8) as usize
                && ptr
                    < (access!(data_pool, buf, *const u8) as usize + access!(data_pool, len, usize))
        }
    }

    #[inline]
    fn recover_metadata(
        &self,
        buf: <<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        self.recover_buffer(buf.as_ref())
    }

    /// Recovers buffer into datapath metadata IF the buffer is registered and within bounds.
    /// MUST be called ONLY if the buffer is registered and within bounds.
    #[inline]
    fn recover_buffer(
        &self,
        buf: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        let (data_ptr, index, offset) = unsafe { self.recover_metadata_mbuf(buf.as_ptr()) };

        {
            Ok(MbufMetadata::new(
                data_ptr,
                self.mempool(),
                index,
                offset,
                buf.len(),
            ))
        }
    }

    #[inline]
    fn alloc_data_buf(
        &self,
        context: MempoolID,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>
    {
        let data = unsafe { custom_mlx5_mempool_alloc(self.get_data_mempool()) };
        if data.is_null() {
            return Ok(None);
        }
        // recover the ref count index
        let index = unsafe { custom_mlx5_mempool_find_index(self.get_data_mempool(), data) };
        if index == -1 {
            unsafe {
                custom_mlx5_mempool_free(self.get_data_mempool(), data);
            }
            return Ok(None);
        }
        Ok(Some(Mlx5Buffer::new(
            data,
            self.mempool(),
            index as usize,
            0,
            context,
        )))
    }
}
