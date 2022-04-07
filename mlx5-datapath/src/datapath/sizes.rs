//! This modules contains sizes used to initialize mempools with different item sizes, and helper
//! functions to determine optimal mempool pages, and page sizes.
//! We assume metadata objects are all 64 bytes long, such that:
//! 32768 (2^13) metadata objects fit in a single 2MB page.
//! 64 (2^8) metadata objects fit in a single 4KB page.
//! Since data and metadata are laid out separately:
//! Each "mempool" consists of a data mempool and a metadata mempool
//! with the SAME amount of items.
//! We also want to not waste memory / ensure that metadatas
//! use the entire page they are allocated on (either a 2MB page or 4K page).
//! So for any given item size, there must be either:
//! - multiples of 64 (for 4K metadata pages) items
//! - multiples of 32768 (for 2MB metadata pages) items
//! This means, the number of items must be aligned to either:
//! - 64
//! - 32768
//! So, given the following:
//! - Item size
//! - Minimum number of items
//! - Page size for metadata mbufs
//! - Page size for data mbufs
//! We can calculate the correct number of items.

use color_eyre::eyre::{bail, Result};
use cornflakes_libos::mem;

const METADATA_SIZE: usize = 64;
pub const RX_MEMPOOL_METADATA_PGSIZE: usize = mem::PGSIZE_2MB;
pub const RX_MEMPOOL_DATA_PGSIZE: usize = mem::PGSIZE_2MB;
pub const RX_MEMPOOL_DATA_LEN: usize = 8192;
pub const RX_MEMPOOL_MIN_NUM_ITEMS: usize = 8192;

pub fn align_up(x: usize, align_size: usize) -> usize {
    // find value aligned up to align_size
    let divisor = x / align_size;
    if (divisor * align_size) < x {
        return (divisor + 1) * align_size;
    } else {
        assert!(divisor * align_size == x);
        return x;
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct MempoolAllocationParams {
    metadata_pgsize: usize,
    item_len: usize,
    data_pgsize: usize,
    num_items: usize,
    num_metadata_pages: usize,
    num_data_pages: usize,
}

impl MempoolAllocationParams {
    pub fn get_item_len(&self) -> usize {
        self.item_len
    }

    pub fn get_num_items(&self) -> usize {
        self.num_items
    }

    pub fn get_data_pgsize(&self) -> usize {
        self.data_pgsize
    }

    pub fn get_metadata_pgsize(&self) -> usize {
        self.metadata_pgsize
    }

    pub fn new(
        min_items: usize,
        metadata_pgsize: usize,
        data_pgsize: usize,
        item_size: usize,
    ) -> Result<Self> {
        // check that metadata_pgsize and data_pgsize are proper page sizes
        if metadata_pgsize != mem::PGSIZE_4KB
            && metadata_pgsize != mem::PGSIZE_2MB
            && metadata_pgsize != mem::PGSIZE_1GB
        {
            bail!(
                "Metadata pgsize provided: {} not 4KB, 2MB, or 1GB",
                metadata_pgsize
            );
        }
        if data_pgsize != mem::PGSIZE_4KB
            && data_pgsize != mem::PGSIZE_2MB
            && data_pgsize != mem::PGSIZE_1GB
        {
            bail!("Data pgsize provided: {} not 4KB, 2MB, or 1GB", data_pgsize);
        }

        if data_pgsize % item_size != 0 {
            bail!(
                "Item size provided: {} not aligned to pgsize: {}",
                item_size,
                data_pgsize
            );
        }

        // calculate alignment number of objects
        let metadata_items_per_page = metadata_pgsize / METADATA_SIZE;
        let data_items_per_page = data_pgsize / item_size;
        let alignment_size = align_up(
            std::cmp::min(metadata_items_per_page, data_items_per_page),
            std::cmp::max(metadata_items_per_page, data_items_per_page),
        );

        // align the minimum number of objets up
        let num_items = align_up(min_items, alignment_size);

        // calculate the number of data pages and metadata pages accordingly
        let num_metadata_pages = num_items / alignment_size;
        let num_data_pages = num_items / data_items_per_page;

        Ok(MempoolAllocationParams {
            metadata_pgsize: metadata_pgsize,
            item_len: item_size,
            data_pgsize: data_pgsize,
            num_items: num_items,
            num_metadata_pages: num_metadata_pages,
            num_data_pages: num_data_pages,
        })
    }
}
