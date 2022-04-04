#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/mlx5_bindings.rs"));

#[link(name = "mlx5inlined")]
extern "C" {
    fn free_mbuf_(mbuf: *mut mbuf);

    fn ns_to_cycles_(a: u64) -> u64;

    fn cycles_to_ns_(a: u64) -> u64;

    fn current_cycles_() -> u64;

    fn get_recv_mempool_(context: *mut mlx5_per_thread_context) -> *mut registered_mempool;

    fn alloc_data_buf_(mempool: *mut registered_mempool) -> *mut ::std::os::raw::c_void;

    fn alloc_metadata_(
        mempool: *mut registered_mempool,
        data_buf: *mut ::std::os::raw::c_void,
    ) -> *mut mbuf;

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

    fn mempool_free_(item: *mut ::std::os::raw::c_void, mempool: *mut mempool);

    fn mbuf_at_index_(mempool: *mut mempool, index: usize) -> *mut mbuf;

    fn mlx5_rte_memcpy_(
        dst: *mut ::std::os::raw::c_void,
        src: *const ::std::os::raw::c_void,
        n: usize,
    );

    fn fill_in_hdrs_(
        hdr_buffer: *mut ::std::os::raw::c_void,
        hdr: *const ::std::os::raw::c_void,
        id: u32,
        data_len: usize,
    );

    fn completion_start_(context: *mut mlx5_per_thread_context) -> *mut transmission_info;

    fn dpseg_start_(
        context: *mut mlx5_per_thread_context,
        inline_off: usize,
    ) -> *mut mlx5_wqe_data_seg;

    fn flip_headers_mlx5_(metadata_mbuf: *mut mbuf);

}

#[inline]
pub unsafe fn free_mbuf(mbuf: *mut mbuf) {
    free_mbuf_(mbuf);
}

#[inline]
pub unsafe fn ns_to_cycles(a: u64) -> u64 {
    ns_to_cycles_(a)
}

#[inline]
pub unsafe fn cycles_to_ns(a: u64) -> u64 {
    cycles_to_ns_(a)
}

#[inline]
pub unsafe fn current_cycles() -> u64 {
    current_cycles_()
}

#[inline]
pub unsafe fn get_recv_mempool(context: *mut mlx5_per_thread_context) -> *mut registered_mempool {
    get_recv_mempool_(context)
}

#[inline]
pub unsafe fn alloc_data_buf(mempool: *mut registered_mempool) -> *mut ::std::os::raw::c_void {
    alloc_data_buf_(mempool)
}

#[inline]
pub unsafe fn alloc_metadata(
    mempool: *mut registered_mempool,
    data_buf: *mut ::std::os::raw::c_void,
) -> *mut mbuf {
    alloc_metadata_(mempool, data_buf)
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
pub unsafe fn mempool_free(item: *mut ::std::os::raw::c_void, mempool: *mut mempool) {
    mempool_free_(item, mempool);
}

#[inline]
pub unsafe fn mbuf_at_index(mempool: *mut mempool, index: usize) -> *mut mbuf {
    mbuf_at_index_(mempool, index)
}

#[inline]
pub unsafe fn mlx5_rte_memcpy(
    dst: *mut ::std::os::raw::c_void,
    src: *const ::std::os::raw::c_void,
    n: usize,
) {
    mlx5_rte_memcpy_(dst, src, n);
}

#[inline]
pub unsafe fn fill_in_hdrs(
    hdr_buffer: *mut ::std::os::raw::c_void,
    hdr: *const ::std::os::raw::c_void,
    id: u32,
    data_len: usize,
) {
    fill_in_hdrs_(hdr_buffer, hdr, id, data_len);
}

#[inline]
pub unsafe fn completion_start(context: *mut mlx5_per_thread_context) -> *mut transmission_info {
    completion_start_(context)
}

#[inline]
pub unsafe fn dpseg_start(
    context: *mut mlx5_per_thread_context,
    inline_off: usize,
) -> *mut mlx5_wqe_data_seg {
    dpseg_start_(context, inline_off)
}

#[inline]
pub unsafe fn flip_headers(metadata_mbuf: *mut mbuf) {
    flip_headers_mlx5_(metadata_mbuf);
}
