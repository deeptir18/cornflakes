use byteorder::{ByteOrder, LittleEndian};
use color_eyre::eyre::{ensure, Result};
use cornflakes_codegen::utils::rc_dynamic_hdr::*;
use cornflakes_codegen::utils::{rc_dynamic_hdr::HeaderRepr, ObjectRef};
use cornflakes_libos::{CfBuf, Datapath, RcCornPtr, ReceivedPkt};
use std::{marker::PhantomData, slice};

const GETREQ_BITMAP_SIZE: usize = 8;
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
            bitmap: [0u8; GETREQ_BITMAP_SIZE],
            id: 0,
            key: CFString::default(),
        }
    }
}

impl<'a, D> GetReq<'a, D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const KEY_BITMAP_IDX: usize = 1;

    pub fn new() -> Self {
        GetReq {
            bitmap: [0u8; GETREQ_BITMAP_SIZE],
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

    pub fn set_key(&mut self, field: &'a str) {
        self.bitmap[Self::KEY_BITMAP_IDX] = 1;
        self.key = CFString::new(field);
    }
}

impl<'a, D> HeaderRepr<'a, D> for GetReq<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        GETREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEY_BITMAP_IDX] as usize * self.key.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        GETREQ_BITMAP_SIZE
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
                GETREQ_BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(GETREQ_BITMAP_SIZE as isize) };

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
            size >= GETREQ_BITMAP_SIZE,
            "Buffer size passed to inner_deserialize not large enough for bitmap"
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, GETREQ_BITMAP_SIZE)?;

        let mut cur_header_offset = GETREQ_BITMAP_SIZE;

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
                CFString::<D>::CONSTANT_HEADER_SIZE,
            )?;
        }

        Ok(())
    }
}

pub const GETRESP_BITMAP_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResp<'a, D>
where
    D: Datapath,
{
    bitmap: [u8; GETRESP_BITMAP_SIZE],
    id: u32,
    val: CFBytes<'a, D>,
}

impl<'a, D> Default for GetResp<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        GetResp {
            bitmap: [0u8; GETRESP_BITMAP_SIZE],
            id: 0,
            val: CFBytes::default(),
        }
    }
}

impl<'a, D> GetResp<'a, D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const VAL_BITMAP_IDX: usize = 1;

    pub fn new() -> Self {
        GetResp {
            bitmap: [0u8; GETRESP_BITMAP_SIZE],
            id: 0,
            val: CFBytes::default(),
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

    pub fn get_val(&self) -> CFBytes<'a, D> {
        self.val.clone()
    }

    pub fn set_val_rc(&mut self, field: CfBuf<D>) {
        self.bitmap[Self::VAL_BITMAP_IDX] = 1;
        self.val = CFBytes::Rc(field);
    }

    pub fn set_val(&mut self, field: &'a [u8]) {
        self.bitmap[Self::VAL_BITMAP_IDX] = 1;
        self.val = CFBytes::Raw(field);
    }
}

impl<'a, D> HeaderRepr<'a, D> for GetResp<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        GETRESP_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::VAL_BITMAP_IDX] as usize * self.val.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        GETRESP_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::VAL_BITMAP_IDX] as usize * CFBytes::<D>::CONSTANT_HEADER_SIZE
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
                GETRESP_BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(GETRESP_BITMAP_SIZE as isize) };

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_val() {
            ret.append(&mut self.val.inner_serialize(
                cur_header_ptr,
                copy_func,
                CFBytes::<D>::CONSTANT_HEADER_SIZE,
            )?);
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
            size >= GETRESP_BITMAP_SIZE,
            "Size {} not big enough for bitmap size {}",
            size,
            GETRESP_BITMAP_SIZE
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, GETRESP_BITMAP_SIZE)?;

        let mut cur_header_offset = GETRESP_BITMAP_SIZE;

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
                CFBytes::<D>::CONSTANT_HEADER_SIZE,
            )?;
        }
        Ok(())
    }
}

pub const PUTREQ_BITMAP_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutReq<'a, D>
where
    D: Datapath,
{
    bitmap: [u8; PUTREQ_BITMAP_SIZE],
    id: u32,
    key: CFString<'a, D>,
    val: CFBytes<'a, D>,
}

impl<'a, D> Default for PutReq<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        PutReq {
            bitmap: [0u8; PUTREQ_BITMAP_SIZE],
            id: 0,
            key: CFString::default(),
            val: CFBytes::default(),
        }
    }
}

impl<'a, D> PutReq<'a, D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const KEY_BITMAP_IDX: usize = 1;

    const VAL_BITMAP_IDX: usize = 2;

    pub fn new() -> Self {
        PutReq {
            bitmap: [0u8; PUTREQ_BITMAP_SIZE],
            id: 0,
            key: CFString::default(),
            val: CFBytes::default(),
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

    pub fn set_key(&mut self, field: &'a str) {
        self.bitmap[Self::KEY_BITMAP_IDX] = 1;
        self.key = CFString::new(field);
    }

    pub fn has_val(&self) -> bool {
        self.bitmap[Self::VAL_BITMAP_IDX] == 1
    }

    pub fn get_val(&self) -> CFBytes<'a, D> {
        self.val.clone()
    }

    pub fn set_val_rc(&mut self, field: CfBuf<D>) {
        self.bitmap[Self::VAL_BITMAP_IDX] = 1;
        self.val = CFBytes::Rc(field);
    }

    pub fn set_val(&mut self, field: &'a [u8]) {
        self.bitmap[Self::VAL_BITMAP_IDX] = 1;
        self.val = CFBytes::Raw(field);
    }
}

impl<'a, D> HeaderRepr<'a, D> for PutReq<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        PUTREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEY_BITMAP_IDX] as usize * self.key.total_header_size()
            + self.bitmap[Self::VAL_BITMAP_IDX] as usize * self.val.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        PUTREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEY_BITMAP_IDX] as usize * CFString::<D>::CONSTANT_HEADER_SIZE
            + self.bitmap[Self::VAL_BITMAP_IDX] as usize * CFBytes::<D>::CONSTANT_HEADER_SIZE
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
                GETRESP_BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(GETRESP_BITMAP_SIZE as isize) };

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_key() {
            ret.append(&mut self.key.inner_serialize(
                cur_header_ptr,
                copy_func,
                CFString::<D>::CONSTANT_HEADER_SIZE,
            )?);

            cur_header_ptr =
                unsafe { cur_header_ptr.offset(CFString::<D>::CONSTANT_HEADER_SIZE as isize) };
        }

        if self.has_val() {
            ret.append(&mut self.val.inner_serialize(
                cur_header_ptr,
                copy_func,
                CFBytes::<D>::CONSTANT_HEADER_SIZE,
            )?);
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
            size >= PUTREQ_BITMAP_SIZE,
            "Size {} not big enough for bitmap size {}",
            size,
            PUTREQ_BITMAP_SIZE
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, PUTREQ_BITMAP_SIZE)?;

        let mut cur_header_offset = PUTREQ_BITMAP_SIZE;

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
                CFString::<D>::CONSTANT_HEADER_SIZE,
            )?;
            cur_header_offset += CFString::<D>::CONSTANT_HEADER_SIZE;
        }
        if bitmap_slice[Self::VAL_BITMAP_IDX] == 1 {
            self.bitmap[Self::VAL_BITMAP_IDX] = 1;
            self.val.inner_deserialize(
                pkt,
                relative_offset + cur_header_offset,
                CFBytes::<D>::CONSTANT_HEADER_SIZE,
            )?;
        }
        Ok(())
    }
}

pub const PUTRESP_BITMAP_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResp<D>
where
    D: Datapath,
{
    bitmap: [u8; PUTRESP_BITMAP_SIZE],
    id: u32,
    _marker: PhantomData<D>,
}

impl<D> Default for PutResp<D>
where
    D: Datapath,
{
    fn default() -> Self {
        PutResp {
            bitmap: [0u8; PUTRESP_BITMAP_SIZE],
            id: 0,
            _marker: PhantomData,
        }
    }
}

impl<D> PutResp<D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    pub fn new() -> Self {
        PutResp {
            bitmap: [0u8; PUTRESP_BITMAP_SIZE],
            id: 0,
            _marker: PhantomData,
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
}

impl<'a, D> HeaderRepr<'a, D> for PutResp<D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        PUTRESP_BITMAP_SIZE + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
    }

    fn dynamic_header_offset(&self) -> usize {
        PUTRESP_BITMAP_SIZE + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
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
        let ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                PUTRESP_BITMAP_SIZE,
            );
        }
        let cur_header_ptr = unsafe { header_ptr.offset(PUTRESP_BITMAP_SIZE as isize) };

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );
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
            size >= PUTRESP_BITMAP_SIZE,
            "Size {} not big enough for bitmap size {}",
            size,
            PUTRESP_BITMAP_SIZE
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, PUTRESP_BITMAP_SIZE)?;

        let cur_header_offset = PUTRESP_BITMAP_SIZE;

        if bitmap_slice[Self::ID_BITMAP_IDX] == 1 {
            self.bitmap[Self::ID_BITMAP_IDX] = 1;
            self.id = LittleEndian::read_u32(
                pkt.contiguous_slice(relative_offset + cur_header_offset, Self::ID_HEADER_SIZE)?,
            );
        }
        Ok(())
    }
}

const GETMREQ_BITMAP_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMReq<'a, D>
where
    D: Datapath,
{
    bitmap: [u8; GETMREQ_BITMAP_SIZE],
    id: u32,
    keys: VariableList<'a, CFString<'a, D>, D>,
}

impl<'a, D> Default for GetMReq<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        GetMReq {
            bitmap: [0u8; GETMREQ_BITMAP_SIZE],
            id: 0,
            keys: VariableList::default(),
        }
    }
}

impl<'a, D> GetMReq<'a, D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const KEYS_BITMAP_IDX: usize = 1;

    pub fn new() -> Self {
        GetMReq {
            bitmap: [0u8; GETMREQ_BITMAP_SIZE],
            id: 0,
            keys: VariableList::default(),
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

    pub fn has_keys(&self) -> bool {
        self.bitmap[Self::KEYS_BITMAP_IDX] == 1
    }

    pub fn get_keys(&self) -> &VariableList<'a, CFString<'a, D>, D> {
        &self.keys
    }

    pub fn set_keys(&mut self, field: VariableList<'a, CFString<'a, D>, D>) {
        self.bitmap[Self::KEYS_BITMAP_IDX] = 1;
        self.keys = field;
    }

    pub fn get_mut_keys(&mut self) -> &mut VariableList<'a, CFString<'a, D>, D> {
        &mut self.keys
    }

    pub fn init_keys(&mut self, num: usize) {
        self.bitmap[Self::KEYS_BITMAP_IDX] = 1;
        self.keys = VariableList::init(num);
    }
}

impl<'a, D> HeaderRepr<'a, D> for GetMReq<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        GETMREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEYS_BITMAP_IDX] as usize * self.keys.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        GETMREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEYS_BITMAP_IDX] as usize
                * VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                GETMREQ_BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(GETMREQ_BITMAP_SIZE as isize) };
        let cur_dynamic_ptr = unsafe { header_ptr.offset(self.dynamic_header_offset() as isize) };
        let cur_dynamic_offset = offset + self.dynamic_header_offset();

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_keys() {
            let mut list_field_ref = ObjectRef(cur_header_ptr as _);
            list_field_ref.write_size(self.keys.len());
            list_field_ref.write_offset(cur_dynamic_offset);
            ret.append(&mut self.keys.inner_serialize(
                cur_dynamic_ptr,
                copy_func,
                cur_dynamic_offset,
            )?);
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
            size >= GETMREQ_BITMAP_SIZE,
            "Not enough space to deserialize bitmap"
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, GETMREQ_BITMAP_SIZE)?;
        let mut cur_header_offset = GETMREQ_BITMAP_SIZE;

        if bitmap_slice[Self::ID_BITMAP_IDX] == 1 {
            self.bitmap[Self::ID_BITMAP_IDX] = 1;
            self.id = LittleEndian::read_u32(
                pkt.contiguous_slice(relative_offset + cur_header_offset, Self::ID_HEADER_SIZE)?,
            );
            cur_header_offset += Self::ID_HEADER_SIZE;
        }

        if bitmap_slice[Self::KEYS_BITMAP_IDX] == 1 {
            self.bitmap[Self::KEYS_BITMAP_IDX] = 1;
            let list_header_slice = pkt.contiguous_slice(
                relative_offset + cur_header_offset,
                VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE,
            )?;
            let list_ref = ObjectRef(list_header_slice.as_ptr());
            self.keys
                .inner_deserialize(pkt, list_ref.get_offset(), list_ref.get_size())?;
        }

        Ok(())
    }
}

const GETMRESP_BITMAP_SIZE: usize = 8;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMResp<'a, D>
where
    D: Datapath,
{
    bitmap: [u8; GETMRESP_BITMAP_SIZE],
    id: u32,
    vals: VariableList<'a, CFBytes<'a, D>, D>,
}

impl<'a, D> Default for GetMResp<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        GetMResp {
            bitmap: [0u8; GETMRESP_BITMAP_SIZE],
            id: 0,
            vals: VariableList::default(),
        }
    }
}

impl<'a, D> GetMResp<'a, D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const VALUES_BITMAP_IDX: usize = 1;

    pub fn new() -> Self {
        GetMResp {
            bitmap: [0u8; GETMRESP_BITMAP_SIZE],
            id: 0,
            vals: VariableList::default(),
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

    pub fn has_vals(&self) -> bool {
        self.bitmap[Self::VALUES_BITMAP_IDX] == 1
    }

    pub fn get_vals(&self) -> &VariableList<'a, CFBytes<'a, D>, D> {
        &self.vals
    }

    pub fn set_vals(&mut self, field: VariableList<'a, CFBytes<'a, D>, D>) {
        self.bitmap[Self::VALUES_BITMAP_IDX] = 1;
        self.vals = field;
    }

    pub fn get_mut_vals(&mut self) -> &mut VariableList<'a, CFBytes<'a, D>, D> {
        &mut self.vals
    }
    pub fn init_vals(&mut self, num: usize) {
        self.vals = VariableList::init(num);
        self.bitmap[Self::VALUES_BITMAP_IDX] = 1;
    }
}

impl<'a, D> HeaderRepr<'a, D> for GetMResp<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        GETMRESP_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::VALUES_BITMAP_IDX] as usize * self.vals.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        GETMRESP_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::VALUES_BITMAP_IDX] as usize
                * VariableList::<CFBytes<D>, D>::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                GETMRESP_BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(GETMRESP_BITMAP_SIZE as isize) };
        let cur_dynamic_ptr = unsafe { header_ptr.offset(self.dynamic_header_offset() as isize) };
        let cur_dynamic_offset = offset + self.dynamic_header_offset();

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_vals() {
            let mut list_field_ref = ObjectRef(cur_header_ptr as _);
            list_field_ref.write_size(self.vals.len());
            list_field_ref.write_offset(cur_dynamic_offset);
            ret.append(&mut self.vals.inner_serialize(
                cur_dynamic_ptr,
                copy_func,
                cur_dynamic_offset,
            )?);
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
            size >= GETMRESP_BITMAP_SIZE,
            "Not enough space to deserialize bitmap"
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, GETMRESP_BITMAP_SIZE)?;
        let mut cur_header_offset = GETMRESP_BITMAP_SIZE;

        if bitmap_slice[Self::ID_BITMAP_IDX] == 1 {
            self.bitmap[Self::ID_BITMAP_IDX] = 1;
            self.id = LittleEndian::read_u32(
                pkt.contiguous_slice(relative_offset + cur_header_offset, Self::ID_HEADER_SIZE)?,
            );
            cur_header_offset += Self::ID_HEADER_SIZE;
        }

        if bitmap_slice[Self::VALUES_BITMAP_IDX] == 1 {
            self.bitmap[Self::VALUES_BITMAP_IDX] = 1;
            let list_slice = pkt.contiguous_slice(
                relative_offset + cur_header_offset,
                VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE,
            )?;
            let list_ref = ObjectRef(list_slice.as_ptr());
            self.vals
                .inner_deserialize(pkt, list_ref.get_offset(), list_ref.get_size())?;
        }

        Ok(())
    }
}

const PUTMREQ_BITMAP_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutMReq<'a, D>
where
    D: Datapath,
{
    bitmap: [u8; PUTMREQ_BITMAP_SIZE],
    id: u32,
    keys: VariableList<'a, CFString<'a, D>, D>,
    vals: VariableList<'a, CFBytes<'a, D>, D>,
}

impl<'a, D> Default for PutMReq<'a, D>
where
    D: Datapath,
{
    fn default() -> Self {
        PutMReq {
            bitmap: [0u8; PUTMREQ_BITMAP_SIZE],
            id: 0,
            keys: VariableList::default(),
            vals: VariableList::default(),
        }
    }
}

impl<'a, D> PutMReq<'a, D>
where
    D: Datapath,
{
    const ID_BITMAP_IDX: usize = 0;
    const ID_HEADER_SIZE: usize = 4;

    const KEYS_BITMAP_IDX: usize = 1;

    const VALS_BITMAP_IDX: usize = 2;

    pub fn new() -> Self {
        PutMReq {
            bitmap: [0u8; PUTMREQ_BITMAP_SIZE],
            id: 0,
            keys: VariableList::default(),
            vals: VariableList::default(),
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

    pub fn has_keys(&self) -> bool {
        self.bitmap[Self::KEYS_BITMAP_IDX] == 1
    }

    pub fn get_keys(&self) -> &VariableList<'a, CFString<'a, D>, D> {
        &self.keys
    }

    pub fn set_keys(&mut self, field: VariableList<'a, CFString<'a, D>, D>) {
        self.bitmap[Self::KEYS_BITMAP_IDX] = 1;
        self.keys = field;
    }

    pub fn get_mut_keys(&mut self) -> &mut VariableList<'a, CFString<'a, D>, D> {
        &mut self.keys
    }
    pub fn init_keys(&mut self, num: usize) {
        self.keys = VariableList::init(num);
        self.bitmap[Self::KEYS_BITMAP_IDX] = 1;
    }

    pub fn has_vals(&self) -> bool {
        self.bitmap[Self::VALS_BITMAP_IDX] == 1
    }

    pub fn get_vals(&self) -> &VariableList<'a, CFBytes<'a, D>, D> {
        &self.vals
    }

    pub fn set_vals(&mut self, field: VariableList<'a, CFBytes<'a, D>, D>) {
        self.bitmap[Self::VALS_BITMAP_IDX] = 1;
        self.vals = field;
    }

    pub fn get_mut_vals(&mut self) -> &mut VariableList<'a, CFBytes<'a, D>, D> {
        &mut self.vals
    }
    pub fn init_val(&mut self, num: usize) {
        self.vals = VariableList::init(num);
        self.bitmap[Self::VALS_BITMAP_IDX] = 1;
    }
}

impl<'a, D> HeaderRepr<'a, D> for PutMReq<'a, D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    fn dynamic_header_size(&self) -> usize {
        PUTMREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEYS_BITMAP_IDX] as usize * self.keys.total_header_size()
            + self.bitmap[Self::VALS_BITMAP_IDX] as usize * self.vals.total_header_size()
    }

    fn dynamic_header_offset(&self) -> usize {
        PUTMREQ_BITMAP_SIZE
            + self.bitmap[Self::ID_BITMAP_IDX] as usize * Self::ID_HEADER_SIZE
            + self.bitmap[Self::KEYS_BITMAP_IDX] as usize
                * VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE
            + self.bitmap[Self::VALS_BITMAP_IDX] as usize
                * VariableList::<CFBytes<D>, D>::CONSTANT_HEADER_SIZE
    }

    fn inner_serialize(
        &self,
        header_ptr: *mut u8,
        copy_func: unsafe fn(
            dst: *mut ::std::os::raw::c_void,
            src: *const ::std::os::raw::c_void,
            len: usize,
        ),
        offset: usize,
    ) -> Result<Vec<(RcCornPtr<'a, D>, *mut u8)>> {
        let mut ret: Vec<(RcCornPtr<'a, D>, *mut u8)> = Vec::default();
        unsafe {
            copy_func(
                header_ptr as _,
                self.bitmap.as_ptr() as _,
                PUTMREQ_BITMAP_SIZE,
            );
        }
        let mut cur_header_ptr = unsafe { header_ptr.offset(PUTMREQ_BITMAP_SIZE as isize) };
        let mut cur_dynamic_ptr =
            unsafe { header_ptr.offset(self.dynamic_header_offset() as isize) };
        let mut cur_dynamic_offset = offset + self.dynamic_header_offset();

        if self.has_id() {
            LittleEndian::write_u32(
                unsafe { slice::from_raw_parts_mut(cur_header_ptr as _, Self::ID_HEADER_SIZE) },
                self.id,
            );

            cur_header_ptr = unsafe { cur_header_ptr.offset(Self::ID_HEADER_SIZE as isize) };
        }

        if self.has_keys() {
            let mut list_field_ref = ObjectRef(cur_header_ptr as _);
            list_field_ref.write_size(self.keys.len());
            list_field_ref.write_offset(cur_dynamic_offset);
            ret.append(&mut self.keys.inner_serialize(
                cur_dynamic_ptr,
                copy_func,
                cur_dynamic_offset,
            )?);

            cur_header_ptr = unsafe {
                cur_header_ptr.offset(VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE as isize)
            };
            cur_dynamic_ptr =
                unsafe { cur_dynamic_ptr.offset(self.keys.dynamic_header_size() as isize) };
            cur_dynamic_offset += self.keys.dynamic_header_size();
        }

        if self.has_vals() {
            let mut list_field_ref = ObjectRef(cur_header_ptr as _);
            list_field_ref.write_size(self.vals.len());
            list_field_ref.write_offset(cur_dynamic_offset);
            ret.append(&mut self.vals.inner_serialize(
                cur_dynamic_ptr,
                copy_func,
                cur_dynamic_offset,
            )?);
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
            size >= PUTMREQ_BITMAP_SIZE,
            "Not enough space to deserialize bitmap"
        );
        let bitmap_slice = pkt.contiguous_slice(relative_offset, PUTMREQ_BITMAP_SIZE)?;
        let mut cur_header_offset = PUTMREQ_BITMAP_SIZE;

        if bitmap_slice[Self::ID_BITMAP_IDX] == 1 {
            self.bitmap[Self::ID_BITMAP_IDX] = 1;
            self.id = LittleEndian::read_u32(
                pkt.contiguous_slice(relative_offset + cur_header_offset, Self::ID_HEADER_SIZE)?,
            );
            cur_header_offset += Self::ID_HEADER_SIZE;
        }
        if bitmap_slice[Self::KEYS_BITMAP_IDX] == 1 {
            self.bitmap[Self::KEYS_BITMAP_IDX] = 1;
            let list_slice = pkt.contiguous_slice(
                relative_offset + cur_header_offset,
                VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE,
            )?;
            let list_ref = ObjectRef(list_slice.as_ptr());
            self.keys
                .inner_deserialize(pkt, list_ref.get_offset(), list_ref.get_size())?;
            cur_header_offset += VariableList::<CFString<D>, D>::CONSTANT_HEADER_SIZE;
        }
        if bitmap_slice[Self::VALS_BITMAP_IDX] == 1 {
            self.bitmap[Self::VALS_BITMAP_IDX] = 1;
            let list_slice = pkt.contiguous_slice(
                relative_offset + cur_header_offset,
                VariableList::<CFBytes<D>, D>::CONSTANT_HEADER_SIZE,
            )?;
            let list_ref = ObjectRef(list_slice.as_ptr());
            self.vals
                .inner_deserialize(pkt, list_ref.get_offset(), list_ref.get_size())?;
        }

        Ok(())
    }
}
