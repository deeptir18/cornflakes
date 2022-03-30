use super::super::dpdk_bindings::*;
use color_eyre::eyre::{bail, Result};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryAllocator {}

impl MemoryAllocator {
    pub fn num_mempools_so_far(&self) -> usize {
        unimplemented!();
    }

    pub fn add_mempool(&mut self, mempool: *mut rte_mempool, value_size: usize) -> Result<()> {
        unimplemented!();
    }
}
