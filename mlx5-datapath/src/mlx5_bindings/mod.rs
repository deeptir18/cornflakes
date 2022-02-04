#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/mlx5_bindings.rs"));

#[link(name = "inlined")]
extern "C" {
    fn alloc_buf_(mempool: *mut mempool) -> *mut ::std::os::raw::c_void;

    fn alloc_metadata_(mempool: *mut mempool) -> *mut mbuf;

    fn init_metadata_(
        mbuf: *mut mbuf,
        buf: *mut ::std::os::raw::c_void,
        data_mempool: *mut mempool,
        metadata_mempool: *mut mempool,
        data_len: usize,
        offset: usize,
    );

    fn get_data_mempool_(mempool: *mut registered_mempool) -> *mut mempool;

    fn get_metadata_mempool_(mempool: *mut registered_mempool) -> *mut mempool;

    fn mbuf_offset_ptr_(mbuf: *mut mbuf, off: usize) -> *mut ::std::os::raw::c_void;

    fn mbuf_refcnt_read_(mbuf: *mut mbuf) -> u16;

    fn mbuf_refcnt_update_or_free_(mbuf: *mut mbuf, change: i16);

    fn mbuf_free_(mbuf: *mut mbuf);

    fn mbuf_at_index_(mempool: *mut mempool, index: usize) -> *mut mbuf;
}

#[inline]
pub unsafe fn alloc_buf(mempool: *mut mempool) -> *mut ::std::os::raw::c_void {
    alloc_buf_(mempool)
}

#[inline]
pub unsafe fn alloc_metadata(mempool: *mut mempool) -> *mut mbuf {
    alloc_metadata_(mempool)
}

#[inline]
pub unsafe fn init_metadata(
    mbuf: *mut mbuf,
    buf: *mut ::std::os::raw::c_void,
    data_mempool: *mut mempool,
    metadata_mempool: *mut mempool,
    data_len: usize,
    offset: usize,
) {
    init_metadata_(mbuf, buf, data_mempool, metadata_mempool, data_len, offset);
}

#[inline]
pub unsafe fn get_data_mempool(mempool: *mut registered_mempool) -> *mut mempool {
    get_data_mempool_(mempool)
}

#[inline]
pub unsafe fn get_metadata_mempool(mempool: *mut registered_mempool) -> *mut mempool {
    get_metadata_mempool_(mempool)
}

pub unsafe fn mbuf_offset_ptr(mbuf: *mut mbuf, off: usize) -> *mut ::std::os::raw::c_void {
    mbuf_offset_ptr_(mbuf, off)
}

#[inline]
pub unsafe fn mbuf_refcnt_read(mbuf: *mut mbuf) -> u16 {
    mbuf_refcnt_read_(mbuf)
}

#[inline]
pub unsafe fn mbuf_refcnt_update_or_free(mbuf: *mut mbuf, change: i16) {
    mbuf_refcnt_update_or_free_(mbuf, change);
}

#[inline]
pub unsafe fn mbuf_free(mbuf: *mut mbuf) {
    mbuf_free_(mbuf);
}

#[inline]
pub unsafe fn mbuf_at_index(mempool: *mut mempool, index: usize) -> *mut mbuf {
    mbuf_at_index_(mempool, index)
}
