//! This module contains ways to interface with custom memory allocation/registration.
//! How we will exactly achieve that, I am not sure yet.
//! It seems like standard library containers don't allow custom allocators yet.
use super::dpdk_bindings::mmap_huge;
use color_eyre::eyre::{bail, Result, WrapErr};
use memmap::MmapMut;
use std::{ptr, slice};
pub const PAGESIZE: usize = 4096;
const PGSHIFT_4KB: usize = 12;
const PGSHIFT_2MB: usize = 21;
const PGSHIFT_1GB: usize = 20;
const PGSIZE_4KB: usize = 1 << PGSHIFT_4KB;
const PGSIZE_2MB: usize = 1 << PGSHIFT_2MB;
const PGSIZE_1GB: usize = 1 << PGSHIFT_1GB;
const PGMASK_4KB: usize = PGSIZE_4KB - 1;
const PGMASK_2MB: usize = PGSIZE_2MB - 1;
const PGMASK_1GB: usize = PGSIZE_1GB - 1;

fn pgn4kb(off: usize) -> usize {
    off >> PGSHIFT_4KB
}

fn pgoff4kb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_4KB
}

pub fn pgn2mb(off: usize) -> usize {
    off >> PGSHIFT_2MB
}

fn pgoff2mb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_2MB
}

fn pgn1gb(off: usize) -> usize {
    off >> PGSHIFT_1GB
}

fn pgoff1gb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_1GB
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
    pub fn from_mmap(mmap: &mut MmapMut) -> MmapMetadata {
        MmapMetadata {
            ptr: mmap.as_ptr(),
            length: mmap.len(),
            pagesize: PageSize::PG2MB,
            paddrs: vec![],
            lkey: 0,
            ibv_mr: ptr::null_mut(),
        }
    }

    #[cfg(test)]
    pub fn test_mmap(ptr: *const u8, length: usize, paddrs: Vec<usize>, lkey: i32) -> MmapMetadata {
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
}

pub fn mmap_new(num_pages: usize) -> Result<(MmapMetadata, MmapMut)> {
    let mut mmap = MmapMut::map_anon(num_pages * PAGESIZE).wrap_err(format!(
        "Not able to anonymously allocate {} pages.",
        num_pages
    ))?;
    Ok((MmapMetadata::from_mmap(&mut mmap), mmap))
}

pub fn mmap_manual(num_pages: usize) -> Result<MmapMetadata> {
    let mut addr: *mut ::std::os::raw::c_void = ptr::null_mut();
    let ret = unsafe { mmap_huge(num_pages, &mut addr as _) };
    if ret != 0 {
        bail!("Failed to mmap huge.");
    }
    Ok(MmapMetadata {
        ptr: addr as *const u8,
        length: PGSIZE_2MB * num_pages,
        pagesize: PageSize::PG2MB,
        paddrs: Vec::default(),
        lkey: 0,
        ibv_mr: ptr::null_mut(),
    })
}
