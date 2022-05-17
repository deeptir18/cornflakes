pub mod dynamic_hdr;
pub mod dynamic_rcsga_hdr;
pub mod dynamic_sga_hdr;
pub mod fixed_hdr;
pub mod rc_dynamic_hdr;

use byteorder::{ByteOrder, LittleEndian};
use std::slice;

fn align_up(x: usize, align_size: usize) -> usize {
    // find value aligned up to align_size
    let divisor = x / align_size;
    if (divisor * align_size) < x {
        return (divisor + 1) * align_size;
    } else {
        assert!(divisor * align_size == x);
        return x;
    }
}

pub struct ForwardPointer<'a>(&'a [u8; 8]);

impl<'a> ForwardPointer<'a> {
    pub fn get_size(&self) -> u32 {
        LittleEndian::read_u32(&self.0[0..4])
    }

    pub fn get_offset(&self) -> u32 {
        LittleEndian::read_u32(&self.0[4..8])
    }
}

pub struct MutForwardPointer<'a>(&'a mut [u8]);

impl<'a> MutForwardPointer<'a> {
    pub fn write_size(&mut self, size: u32) {
        tracing::debug!("Writing size {} at {:?}", size, self.0.as_ptr());
        LittleEndian::write_u32(&mut self.0[0..4], size);
    }

    pub fn write_offset(&mut self, off: u32) {
        LittleEndian::write_u32(&mut self.0[4..8], off);
    }
}
pub struct ObjectRef(pub *const u8);

impl ObjectRef {
    pub fn get_size(&self) -> usize {
        unsafe { LittleEndian::read_u32(slice::from_raw_parts(self.0, 4)) as usize }
    }

    pub fn get_offset(&self) -> usize {
        unsafe { LittleEndian::read_u32(slice::from_raw_parts(self.0.offset(4), 4)) as usize }
    }

    pub fn write_size(&mut self, size: usize) {
        unsafe {
            LittleEndian::write_u32(slice::from_raw_parts_mut(self.0 as _, 4), size as u32);
        }
    }

    pub fn write_offset(&mut self, offset: usize) {
        unsafe {
            LittleEndian::write_u32(
                slice::from_raw_parts_mut(self.0.offset(4) as _, 4),
                offset as u32,
            );
        }
    }
}
