#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/dpdk_ice_bindings.rs"));

#[link(name = "dpdkiceinlined")]
extern "C" {
    fn rte_pktmbuf_free_(packet: *mut rte_mbuf);
}

#[inline]
pub unsafe fn rte_pktmbuf_free(pkt: *mut rte_mbuf) {
    rte_pktmbuf_free_(pkt);
}
