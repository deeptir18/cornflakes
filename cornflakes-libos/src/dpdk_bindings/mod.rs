#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

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
