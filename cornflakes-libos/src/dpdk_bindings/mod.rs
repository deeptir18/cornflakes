#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/dpdk_bindings.rs"));

#[link(name = "inlined")]
extern "C" {
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

}

#[cfg(feature = "mlx5")]
#[link(name = "rte_net_mlx5")]
extern "C" {
    fn rte_pmd_mlx5_get_dyn_flag_names();
}

#[inline(never)]
pub fn load_mlx5_driver() {
    if std::env::var("DONT_SET_THIS").is_ok() {
        unsafe {
            rte_pmd_mlx5_get_dyn_flag_names();
        }
    }
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
