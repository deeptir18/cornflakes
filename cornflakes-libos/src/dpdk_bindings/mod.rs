#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/dpdk_bindings.rs"));

#[link(name = "inlined")]
extern "C" {
    fn mmap_huge_(
        num_pages: usize,
        addr: *mut *mut ::std::os::raw::c_void,
        paddrs: *mut usize,
    ) -> ::std::os::raw::c_int;

    fn munmap_huge_(addr: *mut ::std::os::raw::c_void, pgsize: usize, num_pages: usize);
    fn register_custom_extbuf_ops_() -> ::std::os::raw::c_int;
    fn set_custom_extbuf_ops_(mempool: *mut rte_mempool) -> ::std::os::raw::c_int;
    fn rte_mempool_count_(mempool: *mut rte_mempool) -> ::std::os::raw::c_int;
    fn rte_pktmbuf_refcnt_update_(packet: *mut rte_mbuf, val: i16);
    fn rte_pktmbuf_refcnt_set_(packet: *mut rte_mbuf, val: u16);
    fn rte_pktmbuf_refcnt_get_(packet: *mut rte_mbuf) -> u16;
    fn rte_pktmbuf_free_(packet: *mut rte_mbuf);
    fn rte_pktmbuf_alloc_(mp: *mut rte_mempool) -> *mut rte_mbuf;
    fn rte_eth_tx_burst_(
        port_id: u16,
        queue_id: u16,
        tx_pkts: *mut *mut rte_mbuf,
        nb_pkts: u16,
    ) -> u16;
    fn rte_eth_rx_burst_(
        port_id: u16,
        queue_id: u16,
        rx_pkts: *mut *mut rte_mbuf,
        nb_pkts: u16,
    ) -> u16;
    fn rte_errno_() -> ::std::os::raw::c_int;
    fn rte_get_timer_cycles_() -> u64;
    fn rte_get_timer_hz_() -> u64;
    fn rte_pktmbuf_attach_extbuf_(
        m: *mut rte_mbuf,
        buf_addr: *mut ::std::os::raw::c_void,
        buf_iova: rte_iova_t,
        buf_len: u16,
        shinfo: *mut rte_mbuf_ext_shared_info,
    );
    pub fn general_free_cb_(addr: *mut ::std::os::raw::c_void, opaque: *mut ::std::os::raw::c_void);
    fn rte_memcpy_(dst: *mut ::std::os::raw::c_void, src: *const ::std::os::raw::c_void, n: usize);
    fn rte_dev_dma_map_(
        device_id: u16,
        addr: *mut ::std::os::raw::c_void,
        iova: u64,
        len: size_t,
    ) -> ::std::os::raw::c_int;
    fn rte_dev_dma_unmap_(
        device_id: u16,
        addr: *mut ::std::os::raw::c_void,
        iova: u64,
        len: size_t,
    ) -> ::std::os::raw::c_int;

    fn custom_init_(
        mp: *mut rte_mempool,
        opaque_arg: *mut ::std::os::raw::c_void,
        m: *mut ::std::os::raw::c_void,
        i: u32,
    );

    fn custom_init_priv_(
        mp: *mut rte_mempool,
        opaque_arg: *mut ::std::os::raw::c_void,
        m: *mut ::std::os::raw::c_void,
        i: u32,
    );

    fn set_lkey_(packet: *mut rte_mbuf, lkey: u32);

    fn set_lkey_not_present_(packet: *mut rte_mbuf);

    fn set_refers_to_another_(packet: *mut rte_mbuf, val: u16);

    fn make_ip_(a: u8, b: u8, c: u8, d: u8) -> u32;

    fn fill_in_packet_header_(
        mbuf: *mut rte_mbuf,
        my_eth: *mut rte_ether_addr,
        dst_eth: *mut rte_ether_addr,
        my_ip: u32,
        dst_ip: u32,
        udp_port: u16,
        message_size: usize,
    ) -> usize;

    fn parse_packet_(
        mbuf: *mut rte_mbuf,
        payload_len: *mut usize,
        our_eth: *mut rte_ether_addr,
        our_ip: u32,
    ) -> bool;

    fn switch_headers_(rx_mbuf: *mut rte_mbuf, tx_mbuf: *mut rte_mbuf, payload_length: usize);

    fn shinfo_init_(
        extmem_addr: *mut ::std::os::raw::c_void,
        buf_len: *mut u16,
    ) -> *mut rte_mbuf_ext_shared_info;

    fn eth_dev_configure_(port_id: u16, rx_rings: u16, tx_rings: u16);

    fn loop_in_c_(
        port_id: u16,
        my_eth: *mut rte_ether_addr,
        my_ip: u32,
        rx_bufs: *mut *mut rte_mbuf,
        tx_bufs: *mut *mut rte_mbuf,
        secondary_bufs: *mut *mut rte_mbuf,
        mbuf_pool: *mut rte_mempool,
        header_mbuf_pool: *mut rte_mempool,
        extbuf_mempool: *mut rte_mempool,
        num_mbufs: usize,
        split_payload: usize,
        zero_copy: bool,
        use_external: bool,
        shinfo: *mut rte_mbuf_ext_shared_info,
        ext_mem_addr: *mut ::std::os::raw::c_void,
    ) -> ::std::os::raw::c_int;

    fn copy_payload_(
        src_mbuf: *mut rte_mbuf,
        src_offset: usize,
        dst_mbuf: *mut rte_mbuf,
        dst_offset: usize,
        len: usize,
    );

    fn mem_lookup_page_phys_addrs_(
        addr: *mut ::std::os::raw::c_void,
        len: usize,
        pgsize: usize,
        paddrs: *mut usize,
    ) -> ::std::os::raw::c_int;
}

#[cfg(feature = "mlx5")]
#[link(name = "rte_net_mlx5")]
extern "C" {
    fn rte_pmd_mlx5_get_dyn_flag_names();

    fn rte_pmd_mlx5_manual_reg_mr(
        port_id: u8,
        addr: *mut ::std::os::raw::c_void,
        length: usize,
        lkey_out: *mut u32,
    ) -> *mut ::std::os::raw::c_void;

    fn rte_pmd_mlx5_manual_dereg_mr(ibv_mr: *mut ::std::os::raw::c_void);
}

#[cfg(feature = "mlx5")]
#[inline(never)]
pub fn load_mlx5_driver() {
    if std::env::var("DONT_SET_THIS").is_ok() {
        unsafe {
            rte_pmd_mlx5_get_dyn_flag_names();
        }
    }
}

#[inline]
pub unsafe fn munmap_huge(addr: *mut ::std::os::raw::c_void, pgsize: usize, num_pages: usize) {
    munmap_huge_(addr, pgsize, num_pages);
}

#[inline]
pub unsafe fn mmap_huge(
    num_pages: usize,
    addr: *mut *mut ::std::os::raw::c_void,
    paddrs: *mut usize,
) -> ::std::os::raw::c_int {
    mmap_huge_(num_pages, addr, paddrs)
}

#[inline]
pub unsafe fn register_custom_extbuf_ops() -> ::std::os::raw::c_int {
    register_custom_extbuf_ops_()
}

#[inline]
pub unsafe fn set_custom_extbuf_ops(mempool: *mut rte_mempool) -> ::std::os::raw::c_int {
    set_custom_extbuf_ops_(mempool)
}

#[inline]
pub unsafe fn rte_mempool_count(mempool: *mut rte_mempool) -> ::std::os::raw::c_int {
    rte_mempool_count_(mempool)
}

#[inline]
pub unsafe fn rte_pktmbuf_refcnt_update(packet: *mut rte_mbuf, val: i16) {
    rte_pktmbuf_refcnt_update_(packet, val);
}

#[inline]
pub unsafe fn rte_pktmbuf_refcnt_set(packet: *mut rte_mbuf, val: u16) {
    rte_pktmbuf_refcnt_set_(packet, val);
}

#[inline]
pub unsafe fn rte_pktmbuf_refcnt_read(packet: *mut rte_mbuf) -> u16 {
    rte_pktmbuf_refcnt_get_(packet)
}

#[inline]
pub unsafe fn rte_pktmbuf_free(packet: *mut rte_mbuf) {
    rte_pktmbuf_free_(packet)
}

#[inline]
pub unsafe fn rte_pktmbuf_alloc(mp: *mut rte_mempool) -> *mut rte_mbuf {
    rte_pktmbuf_alloc_(mp)
}

#[inline]
pub unsafe fn rte_eth_tx_burst(
    port_id: u16,
    queue_id: u16,
    tx_pkts: *mut *mut rte_mbuf,
    nb_pkts: u16,
) -> u16 {
    rte_eth_tx_burst_(port_id, queue_id, tx_pkts, nb_pkts)
}

#[inline]
pub unsafe fn rte_eth_rx_burst(
    port_id: u16,
    queue_id: u16,
    rx_pkts: *mut *mut rte_mbuf,
    nb_pkts: u16,
) -> u16 {
    rte_eth_rx_burst_(port_id, queue_id, rx_pkts, nb_pkts)
}

#[inline]
pub unsafe fn rte_errno() -> ::std::os::raw::c_int {
    rte_errno_()
}

#[inline]
pub unsafe fn rte_get_timer_cycles() -> u64 {
    rte_get_timer_cycles_()
}

#[inline]
pub unsafe fn rte_get_timer_hz() -> u64 {
    rte_get_timer_hz_()
}

#[inline]
pub unsafe fn rte_pktmbuf_attach_extbuf(
    m: *mut rte_mbuf,
    buf_addr: *mut ::std::os::raw::c_void,
    buf_iova: rte_iova_t,
    buf_len: u16,
    shinfo: *mut rte_mbuf_ext_shared_info,
) {
    rte_pktmbuf_attach_extbuf_(m, buf_addr, buf_iova, buf_len, shinfo);
}

#[inline]
pub unsafe fn rte_memcpy_wrapper(
    dst: *mut ::std::os::raw::c_void,
    src: *const ::std::os::raw::c_void,
    n: usize,
) {
    rte_memcpy_(dst, src, n);
}

#[inline]
pub unsafe fn rte_dev_dma_map_wrapper(
    device_id: u16,
    addr: *mut ::std::os::raw::c_void,
    iova: u64,
    len: size_t,
) -> ::std::os::raw::c_int {
    rte_dev_dma_map_(device_id, addr, iova, len)
}

#[inline]
pub unsafe fn rte_dev_dma_unmap_wrapper(
    device_id: u16,
    addr: *mut ::std::os::raw::c_void,
    iova: u64,
    len: size_t,
) -> ::std::os::raw::c_int {
    rte_dev_dma_unmap_(device_id, addr, iova, len)
}

#[inline]
pub unsafe fn custom_init() -> unsafe extern "C" fn(
    mp: *mut rte_mempool,
    opaque_arg: *mut ::std::os::raw::c_void,
    m: *mut ::std::os::raw::c_void,
    i: u32,
) {
    custom_init_
}

#[inline]
pub unsafe fn custom_init_priv() -> unsafe extern "C" fn(
    mp: *mut rte_mempool,
    opaque_arg: *mut ::std::os::raw::c_void,
    m: *mut ::std::os::raw::c_void,
    i: u32,
) {
    custom_init_priv_
}

#[inline]
pub unsafe fn set_lkey(packet: *mut rte_mbuf, key: u32) {
    set_lkey_(packet, key);
}

#[inline]
pub unsafe fn set_lkey_not_present(packet: *mut rte_mbuf) {
    set_lkey_not_present_(packet);
}

#[inline]
pub unsafe fn set_refers_to_another(packet: *mut rte_mbuf, val: u16) {
    set_refers_to_another_(packet, val);
}

#[inline]
pub unsafe fn make_ip(a: u8, b: u8, c: u8, d: u8) -> u32 {
    make_ip_(a, b, c, d)
}

#[inline]
pub unsafe fn fill_in_packet_header(
    mbuf: *mut rte_mbuf,
    my_eth: *mut rte_ether_addr,
    dst_eth: *mut rte_ether_addr,
    my_ip: u32,
    dst_ip: u32,
    udp_port: u16,
    message_size: usize,
) -> usize {
    fill_in_packet_header_(mbuf, my_eth, dst_eth, my_ip, dst_ip, udp_port, message_size)
}

#[inline]
pub unsafe fn parse_packet(
    mbuf: *mut rte_mbuf,
    our_eth: *mut rte_ether_addr,
    our_ip: u32,
) -> (bool, usize) {
    let mut payload_len: usize = 0;
    let valid = parse_packet_(mbuf, &mut payload_len as _, our_eth, our_ip);
    (valid, payload_len)
}

#[inline]
pub unsafe fn switch_headers(
    rx_mbuf: *mut rte_mbuf,
    tx_mbuf: *mut rte_mbuf,
    payload_length: usize,
) {
    switch_headers_(rx_mbuf, tx_mbuf, payload_length);
}

#[inline]
pub unsafe fn shinfo_init(
    extmem_addr: *mut ::std::os::raw::c_void,
    buf_len: *mut u16,
) -> *mut rte_mbuf_ext_shared_info {
    shinfo_init_(extmem_addr, buf_len)
}

#[inline]
pub unsafe fn eth_dev_configure(port_id: u16, rx_rings: u16, tx_rings: u16) {
    eth_dev_configure_(port_id, rx_rings, tx_rings);
}

#[inline]
pub unsafe fn loop_in_c(
    port_id: u16,
    my_eth: *mut rte_ether_addr,
    my_ip: u32,
    rx_bufs: *mut *mut rte_mbuf,
    tx_bufs: *mut *mut rte_mbuf,
    secondary_bufs: *mut *mut rte_mbuf,
    mbuf_pool: *mut rte_mempool,
    header_mbuf_pool: *mut rte_mempool,
    extbuf_mempool: *mut rte_mempool,
    num_mbufs: usize,
    split_payload: usize,
    zero_copy: bool,
    use_external: bool,
    shinfo: *mut rte_mbuf_ext_shared_info,
    ext_mem_addr: *mut ::std::os::raw::c_void,
) -> u32 {
    return loop_in_c_(
        port_id,
        my_eth,
        my_ip,
        rx_bufs,
        tx_bufs,
        secondary_bufs,
        mbuf_pool,
        header_mbuf_pool,
        extbuf_mempool,
        num_mbufs,
        split_payload,
        zero_copy,
        use_external,
        shinfo,
        ext_mem_addr,
    ) as u32;
}

#[inline]
pub unsafe fn copy_payload(
    src_mbuf: *mut rte_mbuf,
    src_offset: usize,
    dst_mbuf: *mut rte_mbuf,
    dst_offset: usize,
    len: usize,
) {
    copy_payload_(src_mbuf, src_offset, dst_mbuf, dst_offset, len);
}

#[cfg(feature = "mlx5")]
#[inline(never)]
pub unsafe fn mlx5_manual_reg_mr_callback(
    port_id: u8,
    addr: *mut ::std::os::raw::c_void,
    length: usize,
    lkey_out: *mut u32,
) -> *mut ::std::os::raw::c_void {
    tracing::debug!(
        "Calling reg mr on {:?}, length {}, lkey: {:?}",
        addr,
        length,
        lkey_out
    );
    rte_pmd_mlx5_manual_reg_mr(port_id, addr, length, lkey_out)
}

#[cfg(feature = "mlx5")]
#[inline(never)]
pub unsafe fn mlx5_manual_dereg_mr_callback(ibv_mr: *mut ::std::os::raw::c_void) {
    rte_pmd_mlx5_manual_dereg_mr(ibv_mr)
}
