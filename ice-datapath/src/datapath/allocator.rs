use super::{
    super::{access, dpdk_bindings, ice_bindings},
    connection::{IceBuffer, IceConnection, IceCustomMetadata, IceMetadata},
    sizes,
};

use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    allocator::{DatapathMemoryPool, MempoolID},
    datapath::Datapath,
};
use std::boxed::Box;

#[derive(Debug, Eq, PartialEq)]
pub struct IceMempool {
    mempool_ptr: Option<*mut [u8]>,
    dpdk_ptr: Option<*mut dpdk_bindings::rte_mempool>,
}

impl Drop for IceMempool {
    fn drop(&mut self) {
        unsafe {
            match self.dpdk_ptr {
                Some(x) => {
                    dpdk_bindings::rte_mempool_free(x as *mut dpdk_bindings::rte_mempool);
                }
                None => {
                    ice_bindings::custom_ice_mempool_destroy(self.mempool_as_ice());
                    let _ = unsafe { Box::from_raw(self.mempool_ptr.unwrap()) };
                }
            }
        }
    }
}

impl IceMempool {
    #[inline]
    fn mempool_as_ice(&self) -> *mut ice_bindings::custom_ice_mempool {
        self.mempool_ptr.unwrap() as *mut ice_bindings::custom_ice_mempool
    }

    #[inline]
    pub fn new_from_dpdk_ptr(mempool_ptr: *mut dpdk_bindings::rte_mempool) -> Self {
        IceMempool {
            mempool_ptr: None,
            dpdk_ptr: Some(mempool_ptr),
        }
    }

    #[inline]
    pub fn new(
        mempool_params: &sizes::MempoolAllocationParams,
        use_atomic_ops: bool,
    ) -> Result<Self> {
        let mempool_box = vec![0u8; unsafe { ice_bindings::custom_ice_get_mempool_size() } as _]
            .into_boxed_slice();
        let atomic_ops: u32 = match use_atomic_ops {
            true => 1,
            false => 0,
        };
        let mempool_ptr = Box::<[u8]>::into_raw(mempool_box);
        if unsafe {
            ice_bindings::custom_ice_mempool_create(
                mempool_ptr as _,
                mempool_params.get_item_len() as _,
                mempool_params.get_num_items() as _,
                mempool_params.get_data_pgsize() as _,
                atomic_ops,
            )
        } != 0
        {
            tracing::warn!("Failed to create mempool with params {:?}", mempool_params);
            unsafe {
                let _ = Box::from_raw(mempool_ptr);
            }
            bail!("Failed create mempool with params {:?}", mempool_params);
        }
        tracing::info!("New mempool at ptr: {:?}", mempool_ptr,);
        Ok(IceMempool {
            mempool_ptr: Some(mempool_ptr),
            dpdk_ptr: None,
        })
    }

    #[inline]
    pub unsafe fn recover_metadata_mbuf(
        &self,
        ptr: *const u8,
    ) -> (*mut ::std::os::raw::c_void, usize, usize) {
        let mempool = self.mempool_as_ice();
        let mempool_start = access!(mempool, buf, usize);
        let item_len = access!(mempool, item_len, usize);
        let offset_within_alloc = ptr as usize - mempool_start;
        let index =
            (offset_within_alloc & !(item_len - 1)) >> access!(mempool, log_item_len, usize);
        let data_ptr = (mempool_start + (index << access!(mempool, log_item_len, usize)))
            as *mut std::os::raw::c_void;
        (data_ptr, index, ptr as usize - data_ptr as usize)
    }
}

impl DatapathMemoryPool for IceMempool {
    type DatapathImpl = IceConnection;
    type RegistrationContext = ();

    #[inline]
    fn get_2mb_pages(&self) -> Vec<usize> {
        if let Some(_) = self.dpdk_ptr {
            return vec![];
        }
        let data_pool = self.mempool_as_ice();
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
    fn get_4k_pages(&self) -> Vec<usize> {
        if let Some(_) = self.dpdk_ptr {
            return vec![];
        }
        let data_pool = self.mempool_as_ice();
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
        if let Some(_) = self.dpdk_ptr {
            return vec![];
        }
        let data_pool = self.mempool_as_ice();
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != cornflakes_libos::mem::PGSIZE_1GB {
            return vec![];
        }
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    #[inline]
    fn register(&mut self, _registration_context: Self::RegistrationContext) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn unregister(&mut self) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn is_registered(&self) -> bool {
        return true;
    }

    #[inline(always)]
    fn has_allocated(&self) -> bool {
        if let Some(_) = self.dpdk_ptr {
            unimplemented!();
        } else {
            unsafe { access!(self.mempool_as_ice(), allocated, usize) >= 1 }
        }
    }

    /// Recovers buffer into datapath metadata IF the buffer is registered and within bounds.
    /// MUST be called ONLY if the buffer is registered and within bounds.
    #[inline]
    fn recover_buffer(
        &self,
        buf: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        // only recovers if !dpdk
        if let Some(_) = self.dpdk_ptr {
            bail!("Can only recover buffer if not dpdk mempool");
        }
        let (data_ptr, index, offset) = unsafe { self.recover_metadata_mbuf(buf.as_ptr()) };

        {
            Ok(IceMetadata::Ice(IceCustomMetadata::new(
                data_ptr,
                self.mempool_as_ice(),
                index,
                offset,
                buf.len(),
            )))
        }
    }

    #[inline]
    fn recover_metadata(
        &self,
        buf: <<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        self.recover_buffer(buf.as_ref())
    }

    #[inline]
    fn alloc_data_buf(
        &self,
        _context: MempoolID,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>
    {
        let data = unsafe { ice_bindings::custom_ice_mempool_alloc(self.mempool_as_ice()) };
        if data.is_null() {
            return Ok(None);
        }
        // recover the ref count index
        let index =
            unsafe { ice_bindings::custom_ice_mempool_find_index(self.mempool_as_ice(), data) };
        if index == -1 {
            // TODO: why is this clause here?
            unsafe {
                ice_bindings::custom_ice_mempool_destroy(self.mempool_as_ice());
            }
            tracing::debug!("Couldn't find index");
            return Ok(None);
        }
        Ok(Some(IceBuffer::new(
            data,
            self.mempool_as_ice(),
            index as usize,
        )))
    }
}
