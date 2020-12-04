use super::{
    super::{dpdk_call, dpdk_check_not_failed, dpdk_ok, utils, MsgID},
    dpdk_bindings::*,
    dpdk_check, dpdk_error,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use std::{
    ffi::CString,
    fs::read_to_string,
    mem::{size_of, MaybeUninit},
    path::Path,
    ptr, slice,
    time::Duration,
};
use tracing::{debug, info, warn};
use yaml_rust::{Yaml, YamlLoader};

/// Constants related to DPDK
const NUM_MBUFS: u16 = 8191;
const MBUF_CACHE_SIZE: u16 = 250;
const RX_RING_SIZE: u16 = 2048;
const TX_RING_SIZE: u16 = 2048;
/*#define IP_DEFTTL  64   /* from RFC 1340. */
#define IP_VERSION 0x40
#define IP_HDRLEN  0x05 /* default IP header length == five 32-bits words. */
#define IP_VHL_DEF (IP_VERSION | IP_HDRLEN)*/
// TODO: figure out how to turn jumbo frames on and of
const RX_PACKET_LEN: u32 = 9216;
const MBUF_BUF_SIZE: u32 = RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM;

/// RX and TX Prefetch, Host, and Write-back threshold values should be
/// carefully set for optimal performance. Consult the network
/// controller's datasheet and supporting DPDK documentation for guidance
/// on how these parameters should be set.
const RX_PTHRESH: u8 = 8;
const RX_HTHRESH: u8 = 8;
const RX_WTHRESH: u8 = 0;

/// These default values are optimized for use with the Intel(R) 82599 10 GbE
/// Controller and the DPDK ixgbe PMD. Consider using other values for other
/// network controllers and/or network drivers.
const TX_PTHRESH: u8 = 0;
const TX_HTHRESH: u8 = 0;
const TX_WTHRESH: u8 = 0;

fn dpdk_eal_init(config_path: &str) -> Result<()> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let mut args = vec![];
    let mut ptrs = vec![];
    let yaml = &yamls[0];
    match yaml["dpdk"].as_hash() {
        Some(map) => {
            let eal_init = match map.get(&Yaml::from_str("eal_init")) {
                Some(list) => list,
                None => {
                    bail!("Yaml config dpdk has no eal_init entry");
                }
            };
            for entry in eal_init.as_vec().unwrap() {
                //let entry_str = std::str::from_utf8(entry).unwrap();
                let s = CString::new(entry.as_str().unwrap()).unwrap();
                ptrs.push(s.as_ptr() as *mut u8);
                args.push(s);
            }
        }

        None => {
            bail!("Yaml config file has no entry dpdk");
        }
    }
    debug!("DPDK init args: {:?}", args);
    dpdk_check_not_failed!(rte_eal_init(ptrs.len() as i32, ptrs.as_ptr() as *mut _));
    Ok(())
}

fn wait_for_link_status_up(port_id: u16) -> Result<()> {
    let sleep_duration_ms = Duration::from_millis(100);
    let retry_count: u32 = 90;

    let mut link: MaybeUninit<rte_eth_link> = MaybeUninit::zeroed();
    for _i in 0..retry_count {
        dpdk_ok!(rte_eth_link_get_nowait(port_id, link.as_mut_ptr()));
        let link = unsafe { link.assume_init() };
        if ETH_LINK_UP == link.link_status() as u32 {
            let duplex = if link.link_duplex() as u32 == ETH_LINK_FULL_DUPLEX {
                "full"
            } else {
                "half"
            };
            info!(
                "Port {} Link Up - speed {} Mbps - {} duplex",
                port_id, link.link_speed, duplex
            );
            return Ok(());
        }
        dpdk_call!(rte_delay_us_block(sleep_duration_ms.as_micros() as u32));
    }
    bail!("Link never came up");
}

fn initialize_dpdk_port(port_id: u16, mbuf_pool: *mut rte_mempool) -> Result<()> {
    assert!(dpdk_call!(rte_eth_dev_is_valid_port(port_id)) == 1);
    let rx_rings: u16 = 1;
    let tx_rings: u16 = 2;
    let nb_rxd = RX_RING_SIZE;
    let nb_txd = TX_RING_SIZE;

    let mut mtu: u16 = 0;
    let mut dev_info: MaybeUninit<rte_eth_dev_info> = MaybeUninit::zeroed();
    dpdk_ok!(rte_eth_dev_info_get(port_id, dev_info.as_mut_ptr()));
    dpdk_ok!(rte_eth_dev_set_mtu(port_id, RX_PACKET_LEN as u16));
    dpdk_ok!(rte_eth_dev_get_mtu(port_id, &mut mtu));
    info!("Dev info MTU: {}", mtu);

    let mut port_conf: MaybeUninit<rte_eth_conf> = MaybeUninit::zeroed();
    unsafe {
        (*port_conf.as_mut_ptr()).rxmode.max_rx_pkt_len = RX_PACKET_LEN;
        (*port_conf.as_mut_ptr()).rxmode.offloads =
            DEV_RX_OFFLOAD_JUMBO_FRAME as u64 | DEV_RX_OFFLOAD_TIMESTAMP as u64;
        (*port_conf.as_mut_ptr()).txmode.offloads = DEV_TX_OFFLOAD_MULTI_SEGS as u64;
        (*port_conf.as_mut_ptr()).txmode.mq_mode = rte_eth_rx_mq_mode_ETH_MQ_RX_NONE;
    }

    let mut rx_conf: MaybeUninit<rte_eth_rxconf> = MaybeUninit::zeroed();
    unsafe {
        (*rx_conf.as_mut_ptr()).rx_thresh.pthresh = RX_PTHRESH;
        (*rx_conf.as_mut_ptr()).rx_thresh.hthresh = RX_HTHRESH;
        (*rx_conf.as_mut_ptr()).rx_thresh.wthresh = RX_WTHRESH;
        (*rx_conf.as_mut_ptr()).rx_free_thresh = 32;
    }

    let mut tx_conf: MaybeUninit<rte_eth_txconf> = MaybeUninit::zeroed();
    unsafe {
        (*tx_conf.as_mut_ptr()).tx_thresh.pthresh = TX_PTHRESH;
        (*tx_conf.as_mut_ptr()).tx_thresh.hthresh = TX_HTHRESH;
        (*tx_conf.as_mut_ptr()).tx_thresh.wthresh = TX_WTHRESH;
    }

    // configure the ethernet device
    dpdk_ok!(rte_eth_dev_configure(
        port_id,
        rx_rings,
        tx_rings,
        port_conf.as_mut_ptr()
    ));

    // TODO: from demikernel code: what does this do?
    // dpdk_ok!(rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd));

    let socket_id =
        dpdk_check_not_failed!(rte_eth_dev_socket_id(port_id), "Port id is out of range") as u32;

    // allocate and set up 1 RX queue per Ethernet port
    for i in 0..rx_rings {
        dpdk_ok!(rte_eth_rx_queue_setup(
            port_id,
            i,
            nb_rxd,
            socket_id,
            rx_conf.as_mut_ptr(),
            mbuf_pool
        ));
    }

    for i in 0..tx_rings {
        dpdk_ok!(rte_eth_tx_queue_setup(
            port_id,
            i,
            nb_txd,
            socket_id,
            tx_conf.as_mut_ptr()
        ));
    }

    // start the ethernet port
    dpdk_ok!(rte_eth_dev_start(port_id));

    // dpdk_ok!(rte_eth_promiscuous_enable(port_id));

    // disable rx/tx flow control
    // TODO: why?

    let mut fc_conf: MaybeUninit<rte_eth_fc_conf> = MaybeUninit::zeroed();
    dpdk_ok!(rte_eth_dev_flow_ctrl_get(port_id, fc_conf.as_mut_ptr()));
    unsafe {
        (*fc_conf.as_mut_ptr()).mode = rte_eth_fc_mode_RTE_FC_NONE;
    }
    dpdk_ok!(rte_eth_dev_flow_ctrl_set(port_id, fc_conf.as_mut_ptr()));

    wait_for_link_status_up(port_id)?;

    Ok(())
}

/// Returns a mempool meant for attaching external databuffers: no buffers are allocated for packet
/// data except the rte_mbuf data structures themselves.
///
/// Arguments
/// * name - A string slice with the intended name of the mempool.
/// * nb_ports - A u16 with the number of valid DPDK ports.
fn init_extbuf_mempool(name: &str, nb_ports: u16) -> Result<*mut rte_mempool> {
    let name = CString::new(name)?;
    let elt_size: u32 = size_of::<rte_mbuf>() as u32;
    let mbuf_pool = dpdk_call!(rte_mempool_create_empty(
        name.as_ptr(),
        (NUM_MBUFS * nb_ports) as u32,
        elt_size,
        0,
        0, // TODO: should there be a private data structure here?
        rte_socket_id() as i32,
        0
    ));
    if mbuf_pool.is_null() {
        bail!("Mempool created with rte_mempool_create_empty is null.");
    }

    // set ops name
    dpdk_call!(rte_mempool_set_ops_byname(
        mbuf_pool,
        name.as_ptr(),
        ptr::null_mut()
    ));

    // initialize any private data (right now there is none)
    dpdk_call!(rte_pktmbuf_pool_init(mbuf_pool, ptr::null_mut()));

    Ok(mbuf_pool)
}

/// Initializes DPDK EAL and ports given a yaml-config file.
/// Returns two mempools:
/// (1) One that allocates mbufs with `MBUF_BUF_SIZE` of buffer space.
/// (2) One that allocates empty mbuf structs, meant for attaching external data buffers.
///
/// Arguments:
/// * config_path: - A string slice that holds the path to a config file with DPDK initialization.
/// information.
pub fn dpdk_init(config_path: &str) -> Result<(*mut rte_mempool, *mut rte_mempool)> {
    // EAL initialization
    dpdk_eal_init(config_path).wrap_err("EAL initialization failed.")?;

    let nb_ports = dpdk_call!(rte_eth_dev_count_avail());
    if nb_ports <= 0 {
        bail!("DPDK INIT: No ports available.");
    }
    info!(
        "DPDK reports that {} ports (interfaces) are available",
        nb_ports
    );

    // create an mbuf pool to register the rx queues
    let name = CString::new("default_mbuf_pool")?;
    let mbuf_pool = dpdk_call!(rte_pktmbuf_pool_create(
        name.as_ptr(),
        (NUM_MBUFS * nb_ports) as u32,
        MBUF_CACHE_SIZE as u32,
        0,
        MBUF_BUF_SIZE as u16,
        rte_socket_id() as i32,
    ));
    assert!(!mbuf_pool.is_null());

    let owner = RTE_ETH_DEV_NO_OWNER as u64;
    let mut p = dpdk_call!(rte_eth_find_next_owned_by(0, owner)) as u16;
    while p < RTE_MAX_ETHPORTS as u16 {
        initialize_dpdk_port(p, mbuf_pool)?;
        p = dpdk_call!(rte_eth_find_next_owned_by(p + 1, owner)) as u16;
    }

    if dpdk_call!(rte_lcore_count()) > 1 {
        warn!("Too many lcores enabled. Only 1 used.");
    }

    let extbuf_mempool = init_extbuf_mempool("extbuf_pool", nb_ports)
        .wrap_err("Unable to init mempool for attaching external buffers")?;

    Ok((mbuf_pool, extbuf_mempool))
}

/// Returns the result of a mutable ptr to an rte_mbuf allocated from a particular mempool.
///
/// Arguments:
/// * mempool - *mut rte_mempool where packet should be allocated from.
pub fn alloc_mbuf(mempool: *mut rte_mempool) -> Result<*mut rte_mbuf> {
    let mbuf = dpdk_call!(rte_pktmbuf_alloc(mempool));
    if mbuf.is_null() {
        bail!("Allocated null mbuf from rte_pktmbuf_alloc.");
    }
    Ok(mbuf)
}

/// Takes an rte_mbuf, header information, and adds:
/// (1) An Ethernet header
/// (2) An Ipv4 header
/// (3) A Udp header
///
/// Arguments:
/// * pkt - The rte_mbuf where header information will be filled in.
/// * header_info - Struct that contains information about udp, ethernet, and ipv4 headers.
/// * data_len - The payload size, as these headers depend on knowing the size of the upcoming
/// payloads.
pub fn fill_in_header(
    pkt: *mut rte_mbuf,
    header_info: &utils::HeaderInfo,
    data_len: usize,
    id: MsgID,
) -> Result<()> {
    let write_ptr = unsafe { ((*pkt).buf_addr as *mut u8).offset((*pkt).data_off as isize) };
    let eth_hdr_slice = unsafe {
        slice::from_raw_parts_mut(
            ((*pkt).buf_addr as *mut u8).offset((*pkt).data_off as isize),
            utils::ETHERNET2_HEADER2_SIZE,
        )
    };
    let ipv4_hdr_slice = unsafe {
        slice::from_raw_parts_mut(
            ((*pkt).buf_addr as *mut u8)
                .offset((*pkt).data_off as isize + utils::ETHERNET2_HEADER2_SIZE as isize),
            utils::IPV4_HEADER2_SIZE,
        )
    };
    let udp_hdr_slice = unsafe {
        slice::from_raw_parts_mut(
            ((*pkt).buf_addr as *mut u8).offset(
                (*pkt).data_off as isize
                    + utils::ETHERNET2_HEADER2_SIZE as isize
                    + utils::IPV4_HEADER2_SIZE as isize,
            ),
            utils::UDP_HEADER2_SIZE,
        )
    };
    let id_hdr_slice = unsafe {
        slice::from_raw_parts_mut(
            ((*pkt).buf_addr as *mut u8).offset(
                (*pkt).data_off as isize
                    + utils::ETHERNET2_HEADER2_SIZE as isize
                    + utils::IPV4_HEADER2_SIZE as isize
                    + utils::UDP_HEADER2_SIZE as isize,
            ),
            4,
        )
    };

    utils::write_udp_hdr(header_info, udp_hdr_slice, data_len)?;
    utils::write_ipv4_hdr(
        header_info,
        ipv4_hdr_slice,
        data_len + utils::UDP_HEADER2_SIZE,
    )?;
    utils::write_eth_hdr(header_info, eth_hdr_slice)?;

    // Write per-packet-id in
    utils::write_pkt_id(id, id_hdr_slice)?;

    Ok(())
}

pub fn get_my_macaddr(port_id: u16) -> Result<rte_ether_addr> {
    let mut ether_addr: MaybeUninit<rte_ether_addr> = MaybeUninit::zeroed();
    dpdk_ok!(rte_eth_macaddr_get(port_id, ether_addr.as_mut_ptr()));
    let ether_addr = unsafe { ether_addr.assume_init() };
    Ok(ether_addr)
}
