//! This module contains ways to interface with custom memory allocation/registration.
//! How we will exactly achieve that, I am not sure yet.
//! It seems like standard library containers don't allow custom allocators yet.

use color_eyre::eyre::{Result, WrapErr};
use memmap::MmapMut;

pub const PAGESIZE: usize = 4096;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct MmapMetadata {
    pub ptr: *const u8,
    pub length: usize,
}

impl MmapMetadata {
    pub fn from_mmap(mmap: &mut MmapMut) -> MmapMetadata {
        MmapMetadata {
            ptr: mmap.as_ptr(),
            length: mmap.len(),
        }
    }

    pub fn in_range(&self, addr: *const u8) -> bool {
        unsafe { self.ptr <= addr && addr <= (self.ptr.offset(self.length as isize)) }
    }
}

pub fn mmap_new(num_pages: usize) -> Result<(MmapMetadata, MmapMut)> {
    let mut mmap = MmapMut::map_anon(num_pages * PAGESIZE).wrap_err(format!(
        "Not able to anonymously allocate {} pages.",
        num_pages
    ))?;
    Ok((MmapMetadata::from_mmap(&mut mmap), mmap))
}
