#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/dpdk_ice_bindings.rs"));

#[link(name = "dpdkiceinlined")]
extern "C" {
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
    fn custom_init_priv_(
        mempool: *mut rte_mempool,
        opaque: *mut ::std::os::raw::c_void,
        m: *mut std::os::raw::c_void,
        m_idx: u32,
    );
    fn eth_dev_configure_ice_(port_id: u16, rx_rings: u16, tx_rings: u16);

    fn rte_pktmbuf_free_(packet: *mut rte_mbuf);

    fn rte_pktmbuf_refcnt_update_or_free_(packet: *mut rte_mbuf, change: i16);

    fn rte_errno_() -> ::std::os::raw::c_int;

    fn rte_pktmbuf_refcnt_set_(pkt: *mut rte_mbuf, refcnt: u16);
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
pub unsafe fn custom_init_priv() -> unsafe extern "C" fn(
    mp: *mut rte_mempool,
    opaque_arg: *mut ::std::os::raw::c_void,
    m: *mut ::std::os::raw::c_void,
    i: u32,
) {
    custom_init_priv_
}

#[inline]
pub unsafe fn eth_dev_configure_ice(port_id: u16, rx_rings: u16, tx_rings: u16) {
    eth_dev_configure_ice_(port_id, rx_rings, tx_rings);
}

#[inline]
pub unsafe fn rte_pktmbuf_free(pkt: *mut rte_mbuf) {
    rte_pktmbuf_free_(pkt);
}

#[inline]
pub unsafe fn rte_pktmbuf_refcnt_update_or_free(pkt: *mut rte_mbuf, change: i16) {
    rte_pktmbuf_refcnt_update_or_free_(pkt, change);
}

#[inline]
pub unsafe fn rte_errno() -> ::std::os::raw::c_int {
    rte_errno_()
}

#[inline]
pub unsafe fn rte_pktmbuf_refcnt_set(pkt: *mut rte_mbuf, refcnt: u16) {
    rte_pktmbuf_refcnt_set_(pkt, refcnt);
}
