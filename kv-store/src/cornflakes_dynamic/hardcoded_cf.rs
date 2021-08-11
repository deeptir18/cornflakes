use byteorder::{ByteOrder, LittleEndian};
use color_eyre::eyre::{ensure, Result};
use cornflakes_codegen::utils::rc_dynamic_hdr::HeaderRepr;
use cornflakes_codegen::utils::rc_dynamic_hdr::*;
use cornflakes_libos::{Datapath, PtrAttributes, RcCornPtr, ReceivedPkt};
use std::{, slice};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct GetReq<'a, D>
where
    D: Datapath,
{
    bitmap: [u8; 8],
    id: u32,
    key: CFString<'a, D>,
}

impl<'a, D> Default for GetReq<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        GetReq {
            bitmap: [0u8; Self::BITMAP_SIZE],
            id: 0,
            key: CFString::default(),
        }
    }
}

impl<'a, D> GetReq<'a, D>
where
    D: Datapath,
{
    const BITMAP_SIZE: usize = 8;

    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const KEY_BITMAP_IDX: usize = 1;

    pub fn new() -> Self {
        GetReq {
            bitmap: [0u8; Self::BITMAP_SIZE],
            id: 0,
            key: CFString::default(),
        }
    }

    pub fn has_id(&self) -> bool {
        self.bitmap[Self::ID_BITMAP_IDX] == 1
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn set_id(&mut self, field: u32) {
        self.bitmap[Self::ID_BITMAP_IDX] = 1;
        self.id = field;
    }

    pub fn has_key(&self) -> bool {
        self.bitmap[Self::KEY_BITMAP_IDX] == 1
    }

    pub fn get_key(&self) -> CFString<'a, D> {
        self.key.clone()
    }

    pub fn set_key(&mut self, field: CFString<'a, D>) {
        self.bitmap[Self::KEY_BITMAP_IDX] = 1;
        self.key = field;
    }
}

impl<'a, D> HeaderRepr<'a, D> for GetReq<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEY_BITMAP_IDX] as usize * self.key.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEY_BITMAP_IDX] as usize * CFString::<D>::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(Self::BITMAP_SIZE as isize) };

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_key() {
            ret.append(&mut self.key.inner_serialize(cur_header_ptr, copy_func, 0)?);
        }

        Ok(ret)
    }

    fn inner_deserialize(
        &mut self,
        pkt: &'a ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        ensure!(
            size >= Self::BITMAP_SIZE,
            "Buffer size passed to inner_deserialize not large enough for bitmap"
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, Self::BITMAP_SIZE)?;

        let mut cur_header_offset = Self::BITMAP_SIZE;

        if bitmap_slice[Self::ID_BITMAP_IDX] == 1 {
            self.bitmap[Self::ID_BITMAP_IDX] = 1;
            self.id = LittleEndian::read_u32(
                pkt.contiguous_slice(relative_offset + cur_header_offset, Self::ID_HEADER_SIZE)?,
            );
            cur_header_offset += Self::ID_HEADER_SIZE;
        }
        if bitmap_slice[Self::KEY_BITMAP_IDX] == 1 {
            self.bitmap[Self::KEY_BITMAP_IDX] = 1;
            self.key.inner_deserialize(
                pkt,
                relative_offset + cur_header_offset,
                RcBytes::<D>::CONSTANT_HEADER_SIZE,
            )?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResp<D>
where
    D: Datapath,
{
    bitmap: [u8; 8],
    id: u32,
    val: RcBytes<D>,
}

impl<D> Default for GetResp<D>
where
    D: Datapath,
{
    fn default() -> Self {
        GetResp {
            bitmap: [0u8; Self::BITMAP_SIZE],
            id: 0,
            val: RcBytes::default(),
        }
    }
}

impl<D> GetResp<D>
where
    D: Datapath,
{
    const BITMAP_SIZE: usize = 8;

    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const VAL_BITMAP_IDX: usize = 1;

    pub fn new() -> Self {
        GetResp {
            bitmap: [0u8; Self::BITMAP_SIZE],
            id: 0,
            val: RcBytes::default(),
        }
    }

    pub fn has_id(&self) -> bool {
        self.bitmap[Self::ID_BITMAP_IDX] == 1
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn set_id(&mut self, field: u32) {
        self.bitmap[Self::ID_BITMAP_IDX] = 1;
        self.id = field;
    }

    pub fn has_val(&self) -> bool {
        self.bitmap[Self::VAL_BITMAP_IDX] == 1
    }

    pub fn get_val(&self) -> RcBytes<D> {
        self.val.clone()
    }

    pub fn set_val(&mut self, field: RcBytes<D>) {
        self.bitmap[Self::VAL_BITMAP_IDX] = 1;
        self.val = field;
    }
}

impl<'a, D> HeaderRepr<'a, D> for GetResp<D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::VAL_BITMAP_IDX] as usize * self.val.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        Self::BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::VAL_BITMAP_IDX] as usize * RcBytes::<D>::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        _offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                Self::BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(Self::BITMAP_SIZE as isize) };

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_val() {
            ret.append(&mut self.val.inner_serialize(cur_header_ptr, copy_func, 0)?);
        }

        Ok(ret)
    }

    fn inner_deserialize(
        &mut self,
        pkt: &'a ReceivedPkt<D>,
        relative_offset: usize,
        size: usize,
    ) -> Result<()> {
        assert!(size >= Self::BITMAP_SIZE);
        let bitmap_slice = pkt.contiguous_slice(relative_offset, Self::BITMAP_SIZE)?;

        let mut cur_header_offset = Self::BITMAP_SIZE;

        if bitmap_slice[Self::ID_BITMAP_IDX] == 1 {
            self.bitmap[Self::ID_BITMAP_IDX] = 1;
            self.id = LittleEndian::read_u32(
                pkt.contiguous_slice(relative_offset + cur_header_offset, Self::ID_HEADER_SIZE)?,
            );
            cur_header_offset += Self::ID_HEADER_SIZE;
        }
        if bitmap_slice[Self::VAL_BITMAP_IDX] == 1 {
            self.bitmap[Self::VAL_BITMAP_IDX] = 1;
            self.val.inner_deserialize(
                pkt,
                relative_offset + cur_header_offset,
                RcBytes::<D>::CONSTANT_HEADER_SIZE,
            )?;
        }
        Ok(())
    }
}
