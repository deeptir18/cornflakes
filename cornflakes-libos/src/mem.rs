//! This module contains ways to interface with custom memory allocation/registration.
//! How we will exactly achieve that, I am not sure yet.
//! It seems like standard library containers don't allow custom allocators yet.
use color_eyre::eyre::{bail, Result};
use std::slice;
pub const PAGESIZE: usize = 4096;
const PGSHIFT_4KB: usize = 12;
const PGSHIFT_2MB: usize = 21;
const PGSHIFT_1GB: usize = 20;
pub const PGSIZE_4KB: usize = 1 << PGSHIFT_4KB;
pub const PGSIZE_2MB: usize = 1 << PGSHIFT_2MB;
pub const PGSIZE_1GB: usize = 1 << PGSHIFT_1GB;
const PGMASK_4KB: usize = PGSIZE_4KB - 1;
const PGMASK_2MB: usize = PGSIZE_2MB - 1;
const PGMASK_1GB: usize = PGSIZE_1GB - 1;

#[inline]
fn pgn4kb(off: usize) -> usize {
    off >> PGSHIFT_4KB
}

#[inline]
fn pgoff4kb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_4KB
}

#[inline]
pub fn pgn2mb(off: usize) -> usize {
    off >> PGSHIFT_2MB
}

#[inline]
fn pgoff2mb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_2MB
}

fn pgn1gb(off: usize) -> usize {
    off >> PGSHIFT_1GB
}

fn pgoff1gb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_1GB
}

#[inline]
pub fn closest_1g_page(addr: *const u8) -> usize {
    let off = pgoff1gb(addr);
    addr as usize - off
}

#[inline]
pub fn closest_4k_page(addr: *const u8) -> usize {
    let off = pgoff4kb(addr);
    addr as usize - off
}

#[inline]
pub fn closest_2mb_page(addr: *const u8) -> usize {
    let off = pgoff2mb(addr);
    addr as usize - off
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum PageSize {
    PG4KB,
    PG2MB,
    PG1GB,
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct MmapMetadata {
    pub ptr: *const u8,
    pub length: usize,
    paddrs: Vec<usize>,
    lkey: u32,
    ibv_mr: *mut ::std::os::raw::c_void,
    pagesize: PageSize,
}

impl MmapMetadata {
    pub fn new(_num_pages: usize) -> Result<MmapMetadata> {
        // TODO: remove all instances of this struct from the code
        unimplemented!();
    }

    #[cfg(test)]
    pub fn test_mmap(ptr: *const u8, length: usize, paddrs: Vec<usize>, lkey: u32) -> MmapMetadata {
        MmapMetadata {
            ptr: ptr,
            length: length,
            pagesize: PageSize::PG2MB,
            paddrs: paddrs,
            lkey: lkey,
            ibv_mr: ptr::null_mut(),
        }
    }

    pub fn get_pagesize(&self) -> usize {
        match self.pagesize {
            PageSize::PG4KB => PGSIZE_4KB,
            PageSize::PG2MB => PGSIZE_2MB,
            PageSize::PG1GB => PGSIZE_1GB,
        }
    }

    pub fn num_pages(&self) -> usize {
        self.length / self.get_pagesize()
    }

    pub fn get_lkey(&self) -> u32 {
        self.lkey
    }

    pub fn set_lkey(&mut self, lkey: u32) {
        self.lkey = lkey;
    }

    pub fn get_paddrs(&self) -> &Vec<usize> {
        &self.paddrs
    }

    pub fn set_paddrs(&mut self, paddrs: Vec<usize>) {
        self.paddrs = paddrs;
    }

    pub fn get_ibv_mr(&self) -> *mut ::std::os::raw::c_void {
        self.ibv_mr
    }

    pub fn set_ibv_mr(&mut self, ibv_mr: *mut ::std::os::raw::c_void) {
        self.ibv_mr = ibv_mr;
    }

    pub fn addr_within_range(&self, addr: *const u8) -> bool {
        unsafe { self.ptr <= addr && addr <= (self.ptr.offset(self.length as isize)) }
    }

    pub fn get_physaddr(&self, addr: *const u8) -> Result<usize> {
        if !self.addr_within_range(addr) {
            bail!(
                "Virtual addr: {:?} not in range ({:?}, length {})",
                addr,
                self.ptr,
                self.length
            );
        }
        match self.pagesize {
            PageSize::PG4KB => {
                let page_number = pgn4kb(addr as usize - self.ptr as usize);
                Ok(self.paddrs[page_number] + pgoff4kb(addr))
            }
            PageSize::PG2MB => {
                let page_number = pgn2mb(addr as usize - self.ptr as usize);
                Ok(self.paddrs[page_number] + pgoff2mb(addr))
            }
            PageSize::PG1GB => {
                let page_number = pgn1gb(addr as usize - self.ptr as usize);
                Ok(self.paddrs[page_number] + pgoff1gb(addr))
            }
        }
    }

    pub fn get_full_buf(&mut self) -> Result<&mut [u8]> {
        Ok(unsafe { slice::from_raw_parts_mut(self.ptr as _, self.length) })
    }

    pub fn free_mmap(&mut self) {
        unimplemented!();
    }
}

// now: how to build a simple block allocator from these huge pages
