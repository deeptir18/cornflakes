#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/mlx5_bindings.rs"));

#[link(name = "mlx5inlined")]
extern "C" {
    fn custom_mlx5_free_mbuf_(mbuf: *mut custom_mlx5_mbuf);

    fn ns_to_cycles_(a: u64) -> u64;

    fn cycles_to_ns_(a: u64) -> u64;

    fn current_cycles_() -> u64;

    fn custom_mlx5_get_recv_mempool_(
        context: *mut custom_mlx5_per_thread_context,
    ) -> *mut registered_mempool;

    fn alloc_data_buf_(mempool: *mut registered_mempool) -> *mut ::std::os::raw::c_void;

    fn alloc_metadata_(
        mempool: *mut registered_mempool,
        data_buf: *mut ::std::os::raw::c_void,
    ) -> *mut custom_mlx5_mbuf;

    fn init_metadata_(
        mbuf: *mut custom_mlx5_mbuf,
        buf: *mut ::std::os::raw::c_void,
        data_mempool: *mut custom_mlx5_mempool,
        metadata_mempool: *mut custom_mlx5_mempool,
        data_len: usize,
        offset: usize,
    );

    fn get_data_mempool_(mempool: *mut registered_mempool) -> *mut custom_mlx5_mempool;

    fn get_metadata_mempool_(mempool: *mut registered_mempool) -> *mut custom_mlx5_mempool;

    fn custom_mlx5_mbuf_offset_ptr_(
        mbuf: *mut custom_mlx5_mbuf,
        off: usize,
    ) -> *mut ::std::os::raw::c_void;

    fn custom_mlx5_mbuf_refcnt_read_(mbuf: *mut custom_mlx5_mbuf) -> u16;

    fn custom_mlx5_mbuf_refcnt_update_or_free_(mbuf: *mut custom_mlx5_mbuf, change: i16);

    fn custom_mlx5_mbuf_free_(mbuf: *mut custom_mlx5_mbuf);

    fn custom_mlx5_mempool_free_(
        item: *mut ::std::os::raw::c_void,
        mempool: *mut custom_mlx5_mempool,
    );

    fn custom_mlx5_mbuf_at_index_(
        mempool: *mut custom_mlx5_mempool,
        index: usize,
    ) -> *mut custom_mlx5_mbuf;

    fn mlx5_rte_memcpy_(
        dst: *mut ::std::os::raw::c_void,
        src: *const ::std::os::raw::c_void,
        n: usize,
    );

    fn custom_mlx5_fill_in_hdrs_(
        hdr_buffer: *mut ::std::os::raw::c_void,
        hdr: *const ::std::os::raw::c_void,
        id: u32,
        data_len: usize,
    );

    fn custom_mlx5_completion_start_(
        context: *mut custom_mlx5_per_thread_context,
    ) -> *mut custom_mlx5_transmission_info;

    fn custom_mlx5_dpseg_start_(
        context: *mut custom_mlx5_per_thread_context,
        inline_off: usize,
    ) -> *mut mlx5_wqe_data_seg;

    fn flip_headers_mlx5_(metadata_mbuf: *mut custom_mlx5_mbuf);

}

#[inline]
pub unsafe fn custom_mlx5_free_mbuf(mbuf: *mut custom_mlx5_mbuf) {
    custom_mlx5_free_mbuf_(mbuf);
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
pub unsafe fn get_recv_mempool(
    context: *mut custom_mlx5_per_thread_context,
) -> *mut registered_mempool {
    custom_mlx5_get_recv_mempool_(context)
}

#[inline]
pub unsafe fn alloc_data_buf(mempool: *mut registered_mempool) -> *mut ::std::os::raw::c_void {
    alloc_data_buf_(mempool)
}

#[inline]
pub unsafe fn alloc_metadata(
    mempool: *mut registered_mempool,
    data_buf: *mut ::std::os::raw::c_void,
) -> *mut custom_mlx5_mbuf {
    alloc_metadata_(mempool, data_buf)
}

#[inline]
pub unsafe fn init_metadata(
    mbuf: *mut custom_mlx5_mbuf,
    buf: *mut ::std::os::raw::c_void,
    data_mempool: *mut custom_mlx5_mempool,
    metadata_mempool: *mut custom_mlx5_mempool,
    data_len: usize,
    offset: usize,
) {
    init_metadata_(mbuf, buf, data_mempool, metadata_mempool, data_len, offset);
}

#[inline]
pub unsafe fn get_data_mempool(mempool: *mut registered_mempool) -> *mut custom_mlx5_mempool {
    get_data_mempool_(mempool)
}

#[inline]
pub unsafe fn get_metadata_mempool(mempool: *mut registered_mempool) -> *mut custom_mlx5_mempool {
    get_metadata_mempool_(mempool)
}

pub unsafe fn custom_mlx5_mbuf_offset_ptr(
    mbuf: *mut custom_mlx5_mbuf,
    off: usize,
) -> *mut ::std::os::raw::c_void {
    custom_mlx5_mbuf_offset_ptr_(mbuf, off)
}

#[inline]
pub unsafe fn custom_mlx5_mbuf_refcnt_read(mbuf: *mut custom_mlx5_mbuf) -> u16 {
    custom_mlx5_mbuf_refcnt_read_(mbuf)
}

#[inline]
pub unsafe fn custom_mlx5_mbuf_refcnt_update_or_free(mbuf: *mut custom_mlx5_mbuf, change: i16) {
    custom_mlx5_mbuf_refcnt_update_or_free_(mbuf, change);
}

#[inline]
pub unsafe fn custom_mlx5_mbuf_free(mbuf: *mut custom_mlx5_mbuf) {
    custom_mlx5_mbuf_free_(mbuf);
}

#[inline]
pub unsafe fn custom_mlx5_mempool_free(
    item: *mut ::std::os::raw::c_void,
    mempool: *mut custom_mlx5_mempool,
) {
    custom_mlx5_mempool_free_(item, mempool);
}

#[inline]
pub unsafe fn custom_mlx5_mbuf_at_index(
    mempool: *mut custom_mlx5_mempool,
    index: usize,
) -> *mut custom_mlx5_mbuf {
    custom_mlx5_mbuf_at_index_(mempool, index)
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
    custom_mlx5_fill_in_hdrs_(hdr_buffer, hdr, id, data_len);
}

#[inline]
pub unsafe fn custom_mlx5_completion_start(
    context: *mut custom_mlx5_per_thread_context,
) -> *mut custom_mlx5_transmission_info {
    custom_mlx5_completion_start_(context)
}

#[inline]
pub unsafe fn custom_mlx5_dpseg_start(
    context: *mut custom_mlx5_per_thread_context,
    inline_off: usize,
) -> *mut mlx5_wqe_data_seg {
    custom_mlx5_dpseg_start_(context, inline_off)
}

#[inline]
pub unsafe fn flip_headers(metadata_mbuf: *mut custom_mlx5_mbuf) {
    flip_headers_mlx5_(metadata_mbuf);
}
