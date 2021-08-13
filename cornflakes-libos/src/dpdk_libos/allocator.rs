use super::{super::dpdk_call, dpdk_bindings::*, wrapper::get_mempool_memzone_area};
use color_eyre::eyre::{ensure, Result, WrapErr};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct MempoolInfo {
    handle: *mut rte_mempool,
    start: usize,
    size: usize,
}

impl MempoolInfo {
    pub fn new(handle: *mut rte_mempool) -> Result<Self> {
        let (start, size) = get_mempool_memzone_area(handle).wrap_err(format!(
            "Not able to find memzone area for mempool: {:?}",
            handle
        ))?;

        Ok(MempoolInfo {
            handle: handle,
            start: start,
            size: size,
        })
    }

    pub fn within_bounds(&self, buf: &[u8]) -> bool {
        (buf.as_ptr() as usize > self.start) && (buf.as_ptr() as usize) < (self.start + self.size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct MempoolAllocator {}
