pub mod dynamic_hdr;
pub mod fixed_hdr;
pub mod rc_dynamic_hdr;

use byteorder::{ByteOrder, LittleEndian};
use std::slice;

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
