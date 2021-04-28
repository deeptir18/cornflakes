#![allow(unused_assignments)]

use super::{
    super::{
        dpdk_call, dpdk_check_not_failed, dpdk_ok, mbuf_slice, mem, utils, CornType, MsgID,
        PtrAttributes, ScatterGather,
    },
    dpdk_bindings::*,
    dpdk_check, dpdk_error, dpdk_utils,
};
use color_eyre::eyre::{bail, Result, WrapErr};
use hashbrown::HashMap;
use std::{
    ffi::CString,
    mem::{size_of, zeroed, MaybeUninit},
    ptr, slice,
    time::Duration,
};
use tracing::{debug, info, warn};

/// Constants related to DPDK
pub const NUM_MBUFS: u16 = 8191;
pub const MBUF_CACHE_SIZE: u16 = 250;
const RX_RING_SIZE: u16 = 2048;
const TX_RING_SIZE: u16 = 2048;
pub const MAX_SCATTERS: usize = 32;
pub const RECEIVE_BURST_SIZE: u16 = 32;

// TODO: figure out how to turn jumbo frames on and of
pub const RX_PACKET_LEN: u32 = 9216;
// pub const MBUF_BUF_SIZE: u32 = RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM;
pub const MBUF_BUF_SIZE: u32 = RX_PACKET_LEN + RTE_PKTMBUF_HEADROOM;

pub const RTE_PKTMBUF_TAILROOM: u32 = 120; // with priv size of 8, this is padding
pub const MBUF_POOL_HDR_SIZE: usize = 64;
pub const MBUF_PRIV_SIZE: usize = 8;
pub const MBUF_MEMPOOL_PADDING: usize = 128;
pub const TOTAL_MEMPOOL_MBUF_SIZE: usize = MBUF_POOL_HDR_SIZE
    + MBUF_BUF_SIZE as usize
    + MBUF_PRIV_SIZE
    + RTE_PKTMBUF_TAILROOM as usize
    + MBUF_MEMPOOL_PADDING;

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

/// Wrapper around a linked lists of mbufs,
/// for constructing mbufs to send with rte_eth_tx_burst.
pub struct Pkt {
    /// Vector of mbuf pointers.
    //mbufs: Vec<*mut rte_mbuf>,
    /// Number of scatter-gather segments.
    /// Should be the same as `self.mbufs.len()` or `self.data_offsets.len()`, after filling in the
    /// packet.
    /// Num filed so far.
    num_filled: usize,
}

impl Pkt {
    /// Initialize a Pkt data structure, with a given number of segments.
    /// Allocates space in the `mbufs` and `data_offsets` fields according to the size.
    ///
    /// Arguments:
    /// * size - usize which represents the number of scatter-gather segments that will be in this
    /// packet.
    pub fn init(_size: usize) -> Pkt {
        Pkt {
            //mbufs: Vec::with_capacity(size),
            num_filled: 0,
        }
    }

    /// Initializes and configures a Pkt data structure from a given scatter-gather data structure.
    /// Will allocate the mbufs from the given mempools, and will add the header information shown.
    ///
    /// Arguments:
    /// * sga - Object that implements the ScatterGather trait, to be represented by this data
    /// structure.
    /// * header_mempool - Mempool used to allocate the mbuf in the front of the mbuf linked list,
    /// containing the header.
    /// * extbuf_mempool - Mempool used to allocate any mbufs after the first mbuf, which contain
    /// pointers to borrowed external memory.
    /// * header_info - Header information to fill in at the front of the first mbuf, for the
    /// entire packet (currently ethernet, ipv4 and udp packet headers).
    /// * shared_info - Shared info map (including freeing function) for any potential external
    /// memory that maps the index in the sga to the necessary shared info.
    pub fn construct_from_sga(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        sga: &impl ScatterGather,
        header_mempool: *mut rte_mempool,
        extbuf_mempool: *mut rte_mempool,
        header_info: &utils::HeaderInfo,
        default_memzone: (usize, usize),
        external_regions: &Vec<mem::MmapMetadata>,
    ) -> Result<()> {
        // TODO: change this back
        // 1: allocate and add header mbuf
        let header_mbuf =
            alloc_mbuf(header_mempool).wrap_err("Unable to allocate mbuf from mempool.")?;
        self.add_mbuf(
            mbufs,
            pkt_id,
            header_mbuf,
            Some((header_info, sga.data_len(), sga.get_id())),
        )
        .wrap_err("Unable to initialize and add header mbuf.")?;

        // 2: allocate and add mbufs for external mbufs
        for i in 1..sga.num_borrowed_segments() + 1 {
            let mbuf = alloc_mbuf(extbuf_mempool)
                .wrap_err("Unable to allocate externally allocated mbuf.")?;
            self.add_mbuf(mbufs, pkt_id, mbuf, None).wrap_err(format!(
                "Unable to add externally allocated mbuf for segment {}.",
                i
            ))?;
        }

        // 3: copy the payloads from the sga into the mbufs
        self.copy_sga_payloads(mbufs, pkt_id, sga, default_memzone, external_regions)
            .wrap_err("Error in copying payloads from sga to mbufs.")?;

        tracing::debug!(header_mempool_avail =? dpdk_call!(rte_mempool_count(header_mempool)), extbuf_mempool_avail =? dpdk_call!(rte_mempool_count(extbuf_mempool)), "Number available in header_mempool, in extbuf_mempool");

        Ok(())
    }

    /// To test the Pkt data structure, this function allows constructing a Pkt from a vec of
    /// already allocated `*mut rte_mbuf` pointers (not allocated from an actual mempool).
    ///
    /// Arguments:
    /// * sga - Object that implements the ScatterGather trait, to be represented by this data
    /// structure.
    /// * mbufs - Vec of *mut rte_mbuf pointers, pointing to fake buffers, to emulate having been
    /// allocated from an actual mempool.
    /// * header_info - Header information to fill in at the front of the first mbuf, for the
    /// entire packet (currently ethernet, ipv4 and udp packet headers).
    /// * shared_info - Shared info map for registered memory regions.
    #[cfg(test)]
    pub fn construct_from_test_sga(
        &mut self,
        sga: &impl ScatterGather,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        header_info: &utils::HeaderInfo,
        default_memzone: (usize, usize),
        external_regions: &Vec<mem::MmapMetadata>,
    ) -> Result<()> {
        let header_mbuf = mbufs[0][pkt_id];
        self.add_mbuf(
            mbufs,
            pkt_id,
            header_mbuf,
            Some((header_info, sga.data_len(), sga.get_id())),
        )
        .wrap_err("Unable to initialize and add header mbuf.")?;

        for i in 1..sga.num_borrowed_segments() + 1 {
            let mbuf = mbufs[i][pkt_id];
            debug!("Adding a mbuf: {:?}", i);
            self.add_mbuf(mbufs, pkt_id, mbuf, None).wrap_err(format!(
                "Unable to add externally allocated mbuf for segment {}.",
                i
            ))?;
        }

        // 3: copy the payloads from the sga into the mbufs
        self.copy_sga_payloads(mbufs, pkt_id, sga, default_memzone, external_regions)
            .wrap_err("Error in copying payloads from sga to mbufs.")?;

        Ok(())
    }

    /// Initializes the given (already allocated) mbuf pointer.
    /// If a header packet, adds in the header information at the beginning of the packet as well.
    ///
    /// Arguments:
    /// * mbuf - pointing to the mbuf we want to initialize.
    /// * header_info - Option containing the header information, payload size, and MsgID, if we are initializing the first
    /// packet.
    fn add_mbuf(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        mbuf: *mut rte_mbuf,
        header_info: Option<(&utils::HeaderInfo, usize, MsgID)>,
    ) -> Result<()> {
        assert!(!mbuf.is_null());
        unsafe {
            (*mbuf).data_len = 0;
            (*mbuf).pkt_len = 0;
            debug!("Setting next as null");
            (*mbuf).next = ptr::null_mut();
            (*mbuf).nb_segs = 1;
        }

        match header_info {
            Some((info, payload_len, id)) => {
                let data_offset = fill_in_header(mbuf, info, payload_len, id)
                    .wrap_err("unable to fill header info.")?;
                unsafe {
                    (*mbuf).data_len += data_offset as u16;
                    (*mbuf).pkt_len += data_offset as u32;
                }
            }
            None => {}
        }

        if self.num_filled >= 1 {
            let last_mbuf = mbufs[self.num_filled - 1][pkt_id];
            unsafe {
                (*last_mbuf).next = mbuf;
            }
        }

        mbufs[self.num_filled][pkt_id] = mbuf;
        debug!("Mbuf array len is {}", self.num_filled);
        self.num_filled += 1;
        Ok(())
    }

    /// Copies the given sga's payloads into the Pkt's mbufs.
    /// The mbufs must already be initialized.
    ///
    /// Arguments:
    /// * sga - Object that implements the scatter-gather trait to copy payloads from.
    /// * shared_info - Shared info map for registered memory regions.
    fn copy_sga_payloads(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        sga: &impl ScatterGather,
        default_memzone: (usize, usize),
        external_regions: &Vec<mem::MmapMetadata>,
    ) -> Result<()> {
        let mut current_attached_idx = 0;
        for i in 0..sga.num_segments() {
            let cornptr = sga.index(i);
            // any attached mbufs will start at index 1 (1 after header)
            match cornptr.buf_type() {
                CornType::Registered => {
                    // TODO: uncomment this
                    current_attached_idx += 1;
                    self.set_external_payload(
                        mbufs,
                        pkt_id,
                        current_attached_idx,
                        cornptr.as_ref(),
                        default_memzone,
                        external_regions,
                    )
                    .wrap_err("Failed to set external payload into pkt list.")?;
                }
                CornType::Normal => {
                    if current_attached_idx > 0 {
                        bail!("Sga cannot have owned buffers after borrowed buffers; all owned buffers must be at the front.");
                    }
                    // copy this payload into the head buffer
                    self.copy_payload(mbufs, pkt_id, current_attached_idx, cornptr.as_ref())
                        .wrap_err("Failed to copy sga owned entry {} into pkt list.")?;
                }
            }
        }
        Ok(())
    }

    /// Copies the payload into the mbuf at index idx.
    fn copy_payload(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        idx: usize,
        buf: &[u8],
    ) -> Result<()> {
        assert!(idx == 0);
        let data_offset = unsafe { (*mbufs[idx][pkt_id]).data_len as usize };
        if (buf.len() + data_offset) > RX_PACKET_LEN as usize {
            bail!("Cannot set payload of size {}, as data offset is {}: mbuf would be too large, and limit is {}.", buf.len(), data_offset, RX_PACKET_LEN as usize);
        }
        let mbuf_buffer = mbuf_slice!(mbufs[idx][pkt_id], data_offset, buf.len());
        // run rte_memcpy, as an alternate to rust's copy
        dpdk_call!(rte_memcpy_wrapper(
            mbuf_buffer.as_mut_ptr() as _,
            buf.as_ptr() as _,
            buf.len()
        ));
        // mbuf_buffer.copy_from_slice(buf);
        tracing::debug!("[memcpy] Adding {:?} to buf", buf.len());
        unsafe {
            // update the data_len of this mbuf.
            (*mbufs[idx][pkt_id]).data_len += buf.len() as u16;
            // update the pkt len of the entire mbuf.
            (*mbufs[0][pkt_id]).pkt_len += buf.len() as u32;
        }
        Ok(())
    }

    /// Set external payload in packet list.
    fn set_external_payload(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        idx: usize,
        buf: &[u8],
        default_memzone: (usize, usize),
        external_regions: &Vec<mem::MmapMetadata>,
    ) -> Result<()> {
        debug!("The mbuf idx we're changing: {}", idx);
        // check whether the payload is within the default memzone,
        // or in an external region
        if (buf.as_ptr() as usize) > default_memzone.0
            && (buf.as_ptr() as usize) < default_memzone.0 + default_memzone.1
        {
            tracing::debug!("Default memzone: {:?}", default_memzone);
            debug!("Within memzone check");
            // recover the original mbuf this data points to
            let ptr_int = buf.as_ptr() as usize;
            let ptr_offset = ptr_int - default_memzone.0;
            let offset_within_alloc = ptr_offset % TOTAL_MEMPOOL_MBUF_SIZE;

            tracing::debug!("Pointer offset: {:?}", ptr_offset);
            if offset_within_alloc
                < (MBUF_POOL_HDR_SIZE + RTE_PKTMBUF_HEADROOM as usize + MBUF_PRIV_SIZE)
            {
                bail!(
                    "Data pointer within allocation header: {:?} in {:?}",
                    offset_within_alloc,
                    (MBUF_POOL_HDR_SIZE + RTE_PKTMBUF_HEADROOM as usize + MBUF_PRIV_SIZE)
                );
            }

            if offset_within_alloc > (MBUF_POOL_HDR_SIZE + MBUF_BUF_SIZE as usize + MBUF_PRIV_SIZE)
            {
                bail!(
                    "Data pointer within allocation tailroom: {:?} in {:?}",
                    offset_within_alloc,
                    MBUF_POOL_HDR_SIZE + MBUF_BUF_SIZE as usize + MBUF_PRIV_SIZE
                );
            }
            let data_off = (offset_within_alloc
                - (MBUF_POOL_HDR_SIZE + RTE_PKTMBUF_HEADROOM as usize + MBUF_PRIV_SIZE))
                as u16;
            debug!(
                "Original ptr: {}, offset within alloc: {}, resulting data off: {}",
                ptr_int, offset_within_alloc, data_off,
            );

            let original_mbuf_ptr = (ptr_int - offset_within_alloc) as *mut rte_mbuf;
            tracing::debug!("Pointer to original mbuf ptr: {:?}", original_mbuf_ptr);
            // increase ref count of this mbuf
            dpdk_call!(rte_pktmbuf_refcnt_update(original_mbuf_ptr, 1));
            unsafe {
                (*mbufs[idx][pkt_id]).buf_iova = (*original_mbuf_ptr).buf_iova;
                (*mbufs[idx][pkt_id]).buf_addr = buf.as_ptr().offset(-1 * data_off as isize) as _;
                (*mbufs[idx][pkt_id]).buf_len = buf.len() as _;
                (*mbufs[idx][pkt_id]).data_off = data_off
            }
            // set lkey of packet to be -1
            dpdk_call!(set_lkey_not_present(mbufs[idx][pkt_id]));
            dpdk_call!(set_refers_to_another(mbufs[idx][pkt_id], 1));
        // copy over the physical address
        } else {
            for region in external_regions.iter() {
                if region.addr_within_range(buf.as_ptr()) {
                    debug!("Found region that matches");
                    dpdk_call!(rte_pktmbuf_refcnt_set(mbufs[idx][pkt_id], 1));
                    dpdk_call!(set_lkey(mbufs[idx][pkt_id], region.get_lkey()));
                    dpdk_call!(set_refers_to_another(mbufs[idx][pkt_id], 0));
                    unsafe {
                        (*mbufs[idx][pkt_id]).buf_iova = region.get_physaddr(buf.as_ptr())? as _;
                        (*mbufs[idx][pkt_id]).buf_addr = buf.as_ptr() as _;
                        (*mbufs[idx][pkt_id]).buf_len = buf.len() as _;
                        (*mbufs[idx][pkt_id]).data_off = 0;
                        debug!(iova =? (*mbufs[idx][pkt_id]).buf_iova, addr =? (*mbufs[idx][pkt_id]).buf_addr, len =? (*mbufs[idx][pkt_id]).buf_len, "Set packet fields.");
                    }
                }
            }
        }
        // update general packet metadata
        tracing::debug!("Adding {:?} to buf", buf.len());
        unsafe {
            (*mbufs[idx][pkt_id]).data_len = buf.len() as u16;
            (*mbufs[0][pkt_id]).pkt_len += buf.len() as u32;
            (*mbufs[0][pkt_id]).nb_segs += 1;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn num_entries(&self) -> usize {
        self.num_filled
    }
}

fn dpdk_eal_init(eal_init: Vec<String>) -> Result<()> {
    let mut args = vec![];
    let mut ptrs = vec![];
    for entry in eal_init.iter() {
        let s = CString::new(entry.as_str()).unwrap();
        ptrs.push(s.as_ptr() as *mut u8);
        args.push(s);
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
    let tx_rings: u16 = 1;
    let nb_rxd = RX_RING_SIZE;
    let nb_txd = TX_RING_SIZE;

    /*let mut mtu: u16 = 0;
    let mut dev_info: MaybeUninit<rte_eth_dev_info> = MaybeUninit::zeroed();
    dpdk_ok!(rte_eth_dev_info_get(port_id, dev_info.as_mut_ptr()));
    dpdk_ok!(rte_eth_dev_set_mtu(port_id, RX_PACKET_LEN as u16));
    dpdk_ok!(rte_eth_dev_get_mtu(port_id, &mut mtu));
    info!("Dev info MTU: {}", mtu);*/

    /*let mut port_conf: MaybeUninit<rte_eth_conf> = MaybeUninit::zeroed();
    unsafe {
        (*port_conf.as_mut_ptr()).rxmode.max_rx_pkt_len = RX_PACKET_LEN;
        (*port_conf.as_mut_ptr()).rxmode.offloads = DEV_RX_OFFLOAD_JUMBO_FRAME as u64;
        //(*port_conf.as_mut_ptr()).rxmode.mq_mode = rte_eth_rx_mq_mode_ETH_MQ_RX_RSS;
        //(*port_conf.as_mut_ptr()).rx_adv_conf.rss_conf.rss_hf =
        //    ETH_RSS_IP as u64 | (*dev_info.as_mut_ptr()).flow_type_rss_offloads;
        (*port_conf.as_mut_ptr()).txmode.offloads = DEV_TX_OFFLOAD_MULTI_SEGS as u64;
        (*port_conf.as_mut_ptr()).txmode.mq_mode = rte_eth_rx_mq_mode_ETH_MQ_RX_NONE;
    }*/

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
    /*dpdk_ok!(rte_eth_dev_configure(
        port_id,
        rx_rings,
        tx_rings,
        port_conf.as_mut_ptr()
    ));*/
    dpdk_call!(eth_dev_configure(port_id, rx_rings, tx_rings));

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

    let mut mbp_priv_uninit: MaybeUninit<rte_pktmbuf_pool_private> = MaybeUninit::zeroed();
    unsafe {
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_data_room_size = 0;
        // TODO: should the priv size be more?
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_priv_size = MBUF_PRIV_SIZE as _;
    }

    // actual data size is empty (just mbuf and priv data)
    let elt_size: u32 = size_of::<rte_mbuf>() as u32 + MBUF_PRIV_SIZE as u32;

    tracing::debug!(elt_size, "Trying to init extbuf mempool with elt_size");
    let mbuf_pool = dpdk_call!(rte_mempool_create_empty(
        name.as_ptr(),
        (NUM_MBUFS * nb_ports) as u32,
        elt_size,
        // MBUF_CACHE_SIZE.into(),
        0,
        MBUF_PRIV_SIZE as u32,
        rte_socket_id() as i32,
        0,
    ));
    if mbuf_pool.is_null() {
        bail!("Mempool created with rte_mempool_create_empty is null.");
    }

    // register new ops table
    // TODO: pass in a string to represent the name of the ops being registered
    dpdk_ok!(register_custom_extbuf_ops());
    dpdk_ok!(set_custom_extbuf_ops(mbuf_pool));

    // initialize any private data (right now there is none)
    dpdk_call!(rte_pktmbuf_pool_init(
        mbuf_pool,
        mbp_priv_uninit.as_mut_ptr() as _
    ));

    // allocate the mempool
    // on error free the mempool
    if dpdk_call!(rte_mempool_populate_default(mbuf_pool)) != (NUM_MBUFS * nb_ports) as i32 {
        dpdk_call!(rte_mempool_free(mbuf_pool));
        bail!("Not able to initialize extbuf mempool: Failed on rte_mempool_populate_default.");
    }

    // initialize each mbuf
    let num = dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(rte_pktmbuf_init),
        ptr::null_mut()
    ));
    assert!(num == (NUM_MBUFS * nb_ports) as u32);

    // initialize private data
    if dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(custom_init_priv()),
        ptr::null_mut()
    )) != (NUM_MBUFS * nb_ports) as u32
    {
        dpdk_call!(rte_mempool_free(mbuf_pool));
        bail!("Not able to initialize private data in extbuf pool: failed on custom_init_priv.");
    }

    Ok(mbuf_pool)
}

/// Frees the given mempool.
/// Arguments:
/// * mempool - *mut rte_mempool to free.
pub fn free_mempool(mempool: *mut rte_mempool) {
    dpdk_call!(rte_mempool_free(mempool));
}

/// Initializes DPDK ports, and memory pools.
/// Returns two mempools:
/// (1) One that allocates mbufs with `MBUF_BUF_SIZE` of buffer space.
/// (2) One that allocates empty mbuf structs, meant for attaching external data buffers.
fn dpdk_init_helper() -> Result<(*mut rte_mempool, *mut rte_mempool, u16)> {
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
        MBUF_PRIV_SIZE as u16,
        MBUF_BUF_SIZE as u16,
        rte_socket_id() as i32,
    ));
    assert!(!mbuf_pool.is_null());

    // initialize private data of mempool: set all lkeys to -1
    if dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(custom_init_priv()),
        ptr::null_mut()
    )) != (NUM_MBUFS * nb_ports) as u32
    {
        dpdk_call!(rte_mempool_free(mbuf_pool));
        bail!("Not able to initialize private data in extbuf pool.");
    }

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

    Ok((mbuf_pool, extbuf_mempool, nb_ports))
}

pub fn get_mempool_memzone_area(mbuf_pool: *mut rte_mempool) -> Result<(usize, usize)> {
    let mut ret: Vec<(usize, usize)> = vec![];
    extern "C" fn mz_cb(
        _mp: *mut rte_mempool,
        opaque: *mut ::std::os::raw::c_void,
        memhdr: *mut rte_mempool_memhdr,
        _mem_idx: ::std::os::raw::c_uint,
    ) {
        let mr = unsafe { &mut *(opaque as *mut Vec<(usize, usize)>) };
        let (addr, len) = unsafe { ((*memhdr).addr, (*memhdr).len) };
        mr.push((addr as usize, len as usize));
    }
    /*let mut op: Vec<usize> = vec![];

    // struct rte_mempool *mp, void *opaque_arg, void *m, unsigned i
    unsafe extern "C" fn check_each_mbuf(
        mp: *mut rte_mempool,
        mut opaque: *mut ::std::os::raw::c_void,
        m: *mut ::std::os::raw::c_void,
        idx: u32,
    ) {
        let mbuf = m as *mut rte_mbuf;
        let mr = unsafe { &mut *(opaque as *mut Vec<usize>) };
        let op = match idx > 0 {
            true => {
                let len = mr.len();
                mr[len - 1]
            }
            false => 0,
        };
        tracing::debug!(
            "idx: {}, mbuf addr: {:?}, opaque (last): {:?}, dif: {:?}",
            idx,
            mbuf as usize,
            op,
            (mbuf as usize - op)
        );
        mr.push(mbuf as usize);
    }
    dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(check_each_mbuf),
        &mut op as *mut _ as *mut ::std::os::raw::c_void
    ));*/

    dpdk_call!(rte_mempool_mem_iter(
        mbuf_pool,
        Some(mz_cb),
        &mut ret as *mut _ as *mut ::std::os::raw::c_void
    ));

    ret.sort();
    if ret.is_empty() {
        bail!("Allocated mbuf pool has no memory regions");
    }

    let (base_addr, base_len) = ret[0];
    let mut cur_addr = base_addr + base_len;

    for &(addr, len) in &ret[1..] {
        if addr != cur_addr {
            bail!("Non-contiguous memory regions {} vs. {}", cur_addr, addr);
        }
        cur_addr += len;
    }
    let total_len = cur_addr - base_addr;

    // check assumptions about memory layout for the mbufs (needed later for pointer arithmetic)
    let mut sz: rte_mempool_objsz = unsafe { zeroed() };
    let elt_size =
        size_of::<rte_mbuf>() as usize + RX_PACKET_LEN as usize + MBUF_PRIV_SIZE as usize;
    let flags = 0;
    dpdk_call!(rte_mempool_calc_obj_size(
        elt_size as u32,
        flags,
        &mut sz as *mut _
    ));
    tracing::debug!(
        total_size = sz.total_size,
        elt_size = sz.elt_size,
        header = sz.header_size,
        trailer = sz.trailer_size,
        "size of mbuf in mempool"
    );
    assert_eq!(sz.header_size, 64); // size of actual mbuf struct
    assert_eq!(
        sz.elt_size as usize,
        RX_PACKET_LEN as usize + RTE_PKTMBUF_HEADROOM as usize + MBUF_PRIV_SIZE as usize,
    ); // private data size and data buffer size (with headroom)
       /*assert_eq!(
           sz.trailer_size,
           RTE_PKTMBUF_TAILROOM + MBUF_PRIV_SIZE as u32
       );*/
    Ok((base_addr, total_len))
}

/// Initializes DPDK EAL and ports, and memory pools given a yaml-config file.
/// Returns two mempools:
/// (1) One that allocates mbufs with `MBUF_BUF_SIZE` of buffer space.
/// (2) One that allocates empty mbuf structs, meant for attaching external data buffers.
///
/// Arguments:
/// * config_path: - A string slice that holds the path to a config file with DPDK initialization.
/// information.
pub fn dpdk_init(config_path: &str) -> Result<(*mut rte_mempool, *mut rte_mempool, u16)> {
    // EAL initialization
    let eal_init = dpdk_utils::parse_eal_init(config_path)?;
    dpdk_eal_init(eal_init).wrap_err("EAL initialization failed.")?;

    // init ports, mempools
    dpdk_init_helper()
}

/// Returns the result of a mutable ptr to an rte_mbuf allocated from a particular mempool.
///
/// Arguments:
/// * mempool - *mut rte_mempool where packet should be allocated from.
#[inline]
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
#[inline]
pub fn fill_in_header(
    pkt: *mut rte_mbuf,
    header_info: &utils::HeaderInfo,
    data_len: usize,
    id: MsgID,
) -> Result<usize> {
    let eth_hdr_slice = mbuf_slice!(pkt, 0, utils::ETHERNET2_HEADER2_SIZE);

    let ipv4_hdr_slice = mbuf_slice!(pkt, utils::ETHERNET2_HEADER2_SIZE, utils::IPV4_HEADER2_SIZE);

    let udp_hdr_slice = mbuf_slice!(
        pkt,
        utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE,
        utils::UDP_HEADER2_SIZE
    );

    let id_hdr_slice = mbuf_slice!(
        pkt,
        utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE + utils::UDP_HEADER2_SIZE,
        4
    );

    utils::write_udp_hdr(header_info, udp_hdr_slice, data_len)?;

    utils::write_ipv4_hdr(
        header_info,
        ipv4_hdr_slice,
        data_len + utils::UDP_HEADER2_SIZE,
    )?;
    utils::write_eth_hdr(header_info, eth_hdr_slice)?;

    // Write per-packet-id in
    utils::write_pkt_id(id, id_hdr_slice)?;
    Ok(utils::TOTAL_HEADER_SIZE)
}

/// Returns mac address given the port id.
#[inline]
pub fn get_my_macaddr(port_id: u16) -> Result<rte_ether_addr> {
    let mut ether_addr: MaybeUninit<rte_ether_addr> = MaybeUninit::zeroed();
    dpdk_ok!(rte_eth_macaddr_get(port_id, ether_addr.as_mut_ptr()));
    let ether_addr = unsafe { ether_addr.assume_init() };
    Ok(ether_addr)
}

/// Sends the specified linked list of mbufs.
/// Returns () if the packets were sent successfully.
/// Returns an error if they were not sent for some reason.
///
/// Arguments:
/// * port_id - u16 - port_id corresponding to the ethernet device.
/// * queue_id - u16 - index of the transmit queue through which output packets will be sent. Must
/// be in the range of queue ids configured via rte_eth_dev_configure().
/// * tx_pkts - Address of an array of nb_pkts pointers to rte_mbuf data structures which represent
/// the output packets.
/// * nb_pkts - Maximum packets to transmit.
#[inline]
pub fn tx_burst(
    port_id: u16,
    queue_id: u16,
    tx_pkts: *mut *mut rte_mbuf,
    nb_pkts: u16,
) -> Result<()> {
    let mut num_sent: u16 = 0;
    while num_sent < nb_pkts {
        // TODO: should this be in a tight loop?
        num_sent = dpdk_call!(rte_eth_tx_burst(port_id, queue_id, tx_pkts, nb_pkts));
    }
    Ok(())
}

/// Tries to receive a packet on the given transmit queue for the given ethernet advice.
/// Returns a Vec of (MsgID,
/// Frees any invalid packets.
/// On error, bails out.
///
/// Arguments:
/// * port_id - u16 - port_id corresponding to the ethernet device.
/// * queue_id - u16 - index of the receive queue through which received packets will be sent. Must
/// be in the range of queue ids configured via rte_eth_dev_configure().
/// * tx_pkts - Address of an array, of size nb_pkts, of rte_mbuf data structure pointers, to put
/// the received packets.
/// * nb_pkts - Maximum burst size to receive.
#[inline]
pub fn rx_burst(
    port_id: u16,
    queue_id: u16,
    rx_pkts: *mut *mut rte_mbuf,
    nb_pkts: u16,
    my_addr_info: &utils::AddressInfo,
) -> Result<HashMap<usize, (MsgID, utils::AddressInfo)>> {
    let mut valid_packets: HashMap<usize, (MsgID, utils::AddressInfo)> = HashMap::new();
    let num_received = dpdk_call!(rte_eth_rx_burst(port_id, queue_id, rx_pkts, nb_pkts));
    for i in 0..num_received {
        let pkt = unsafe { *rx_pkts.offset(i as isize) };
        match check_valid_packet(pkt, my_addr_info) {
            Some((id, hdr)) => {
                valid_packets.insert(i as usize, (id, hdr));
            }
            None => {
                dpdk_call!(rte_pktmbuf_free(pkt));
            }
        }
    }

    Ok(valid_packets)
}

/// Checks if the payload in the received mbuf is valid.
/// This filters for:
/// (1) packets with the right destination eth addr
/// (2) packets with the protocol UDP in the ip header, and the right destination IP address.
/// (3) packets with the right destination udp port in the udp header.
/// Returns the msg ID, and header info for the parse packet.
///
/// Arguments:
/// pkt - *mut rte_mbuf : pointer to rte_mbuf to check validity for.
#[inline]
fn check_valid_packet(
    pkt: *mut rte_mbuf,
    my_addr_info: &utils::AddressInfo,
) -> Option<(MsgID, utils::AddressInfo)> {
    let eth_hdr_slice = mbuf_slice!(pkt, 0, utils::ETHERNET2_HEADER2_SIZE);
    let src_eth = match utils::check_eth_hdr(eth_hdr_slice, &my_addr_info.ether_addr) {
        Ok((eth, _)) => eth,
        Err(_) => {
            return None;
        }
    };

    let ipv4_hdr_slice = mbuf_slice!(pkt, utils::ETHERNET2_HEADER2_SIZE, utils::IPV4_HEADER2_SIZE);

    let src_ip = match utils::check_ipv4_hdr(ipv4_hdr_slice, &my_addr_info.ipv4_addr) {
        Ok((ip, _)) => ip,
        Err(_) => {
            return None;
        }
    };

    let udp_hdr_slice = mbuf_slice!(
        pkt,
        utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE,
        utils::UDP_HEADER2_SIZE
    );

    let src_port = match utils::check_udp_hdr(udp_hdr_slice, my_addr_info.udp_port) {
        Ok((port, _)) => port,
        Err(_) => {
            return None;
        }
    };

    let id_hdr_slice = mbuf_slice!(
        pkt,
        utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE + utils::UDP_HEADER2_SIZE,
        4
    );

    let msg_id = utils::parse_msg_id(id_hdr_slice);

    Some((msg_id, (utils::AddressInfo::new(src_port, src_ip, src_eth))))
}

/// Frees the mbuf, returns it to it's original mempool.
/// Arguments:
/// * pkt - *mut rte_mbuf to free.
#[inline]
pub fn free_mbuf(pkt: *mut rte_mbuf) {
    let refcnt = dpdk_call!(rte_pktmbuf_refcnt_read(pkt));
    tracing::debug!("Refcnt: {}", refcnt);
    if refcnt == 1 || refcnt == 0 {
        tracing::debug!("Free packet");
        dpdk_call!(rte_pktmbuf_free(pkt));
    } else {
        tracing::debug!(refcnt =? refcnt, "Decrementing refcnt");
        dpdk_call!(rte_pktmbuf_refcnt_set(pkt, refcnt - 1));
        tracing::debug!(refcnt =? dpdk_call!(rte_pktmbuf_refcnt_read(pkt)), "New refcnt");
    }
}

#[inline]
pub fn free_mbuf_bare(pkt: *mut rte_mbuf) {
    dpdk_call!(rte_pktmbuf_free(pkt));
}

#[inline]
pub fn dpdk_register_extmem(
    metadata: &mem::MmapMetadata,
    paddrs: *mut usize,
    lkey: *mut u32,
) -> Result<*mut std::os::raw::c_void> {
    /*dpdk_check_not_failed!(rte_extmem_register(
        metadata.ptr as _,
        metadata.length as u64,
        ptr::null_mut(),
        0,
        mem::PAGESIZE as u64
    ));

    // map the external memory per port
    let owner = RTE_ETH_DEV_NO_OWNER as u64;
    let mut p = dpdk_call!(rte_eth_find_next_owned_by(0, owner)) as u16;
    while p < RTE_MAX_ETHPORTS as u16 {
        dpdk_ok_with_errno!(rte_dev_dma_map_wrapper(
            p,
            metadata.ptr as _,
            0,
            metadata.length as u64
        ));
        p = dpdk_call!(rte_eth_find_next_owned_by(p + 1, owner)) as u16;
    }*/
    // use optimization of manually registering external memory (to avoid the btree lookup on send)
    // need to map physical addresses for each virtual region
    dpdk_ok!(mem_lookup_page_phys_addrs(
        metadata.ptr as _,
        metadata.length,
        metadata.get_pagesize(),
        paddrs
    ));

    // if using mellanox, need to retrieve the lkey for this pinned memory
    let mut ibv_mr: *mut ::std::os::raw::c_void = ptr::null_mut();
    #[cfg(feature = "mlx5")]
    {
        // TODO: currently, only calling for port 0
        ibv_mr = dpdk_call!(mlx5_manual_reg_mr_callback(
            0,
            metadata.ptr as _,
            metadata.length,
            lkey
        ));
        if ibv_mr.is_null() {
            bail!("Manual memory registration failed.");
        }
    }

    Ok(ibv_mr)
}

#[inline]
pub fn dpdk_unregister_extmem(metadata: &mem::MmapMetadata) -> Result<()> {
    #[cfg(feature = "mlx5")]
    {
        dpdk_call!(mlx5_manual_dereg_mr_callback(metadata.get_ibv_mr()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mem::MmapMetadata, CornPtr, Cornflake, ScatterGather};
    use color_eyre;
    use eui48::MacAddress;
    use libc;
    use rand::Rng;
    use std::{convert::TryInto, mem::MaybeUninit, net::Ipv4Addr, ptr};
    use tracing::info;
    use tracing_error::ErrorLayer;
    use tracing_subscriber;
    use tracing_subscriber::{layer::SubscriberExt, prelude::*};

    /// TestMbuf has to have data layout as if private data were right after mbuf data structure.
    /// TODO: how do we ensure it's tightly packet?
    pub struct TestMbuf {
        mbuf: rte_mbuf,
        pub lkey: u32,
        pub lkey_present: u16,
        pub refers_to_another: u16,
        pub has_external: bool,
    }

    fn random_mac() -> MacAddress {
        MacAddress::parse_str("b8:83:03:79:af:40").unwrap()
    }

    fn random_ip() -> Ipv4Addr {
        Ipv4Addr::new(123, 0, 0, 1)
    }

    impl TestMbuf {
        pub fn new() -> TestMbuf {
            let mut mbuf: MaybeUninit<rte_mbuf> = MaybeUninit::zeroed();
            unsafe {
                let ptr = libc::malloc(MBUF_BUF_SIZE as usize);
                (*mbuf.as_mut_ptr()).buf_len = MBUF_BUF_SIZE as u16;
                (*mbuf.as_mut_ptr()).buf_addr = ptr;
                (*mbuf.as_mut_ptr()).next = ptr::null_mut();
                (*mbuf.as_mut_ptr()).data_off = 0;
                (*mbuf.as_mut_ptr()).nb_segs = 1;
                (*mbuf.as_mut_ptr()).priv_size = 8;
                let mbuf = mbuf.assume_init();
                TestMbuf {
                    mbuf: mbuf,
                    lkey: 0,
                    lkey_present: 0,
                    refers_to_another: 0,
                    has_external: false,
                }
            }
        }

        pub fn new_external() -> TestMbuf {
            let mut mbuf: MaybeUninit<rte_mbuf> = MaybeUninit::zeroed();
            unsafe {
                let ptr = ptr::null_mut();
                (*mbuf.as_mut_ptr()).buf_len = MBUF_BUF_SIZE as u16;
                (*mbuf.as_mut_ptr()).buf_addr = ptr;
                (*mbuf.as_mut_ptr()).next = ptr::null_mut();
                (*mbuf.as_mut_ptr()).data_off = 0;
                (*mbuf.as_mut_ptr()).nb_segs = 1;
                (*mbuf.as_mut_ptr()).priv_size = 8;
                let mbuf = mbuf.assume_init();
                TestMbuf {
                    mbuf: mbuf,
                    lkey: 0,
                    lkey_present: 0,
                    refers_to_another: 0,
                    has_external: true,
                }
            }
        }

        pub fn get_pointer(&mut self) -> *mut rte_mbuf {
            &mut self.mbuf as _
        }
    }

    impl Drop for TestMbuf {
        fn drop(&mut self) {
            unsafe {
                if !(self.mbuf.buf_addr.is_null()) {
                    if !(self.has_external) {
                        tracing::debug!(
                            external = self.has_external,
                            "Trying to drop mbuf addr: {:?}",
                            self.mbuf.buf_addr
                        );
                        libc::free(self.mbuf.buf_addr);
                    }
                }
            };
        }
    }

    fn get_random_bytes(size: usize) -> Vec<u8> {
        let random_bytes: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
        println!("{:?}", random_bytes);
        random_bytes
    }

    #[test]
    fn valid_headers() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[0] = test_mbuf.get_pointer();
        let mut pkt = Pkt::init(1);
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        let mbuf = mbufs[0];
        unsafe {
            if ((*mbuf).data_len != utils::TOTAL_HEADER_SIZE as u16)
                || ((*mbuf).pkt_len != utils::TOTAL_HEADER_SIZE as u32)
            {
                info!("Header len is supposed to be: {}", utils::TOTAL_HEADER_SIZE);
                info!("Reported data len: {}", (*mbuf).data_len);
                info!("Reported pkt len: {}", (*mbuf).pkt_len);
                panic!("Incorrect header size in packet data structure.");
            }
        }

        // now test that the packet we set is valid
        let eth_hdr_slice = mbuf_slice!(mbuf, 0, utils::ETHERNET2_HEADER2_SIZE);
        let (src_eth, _) = utils::check_eth_hdr(eth_hdr_slice, &dst_info.ether_addr).unwrap();
        assert!(src_eth == src_info.ether_addr);

        let ipv4_hdr_slice = mbuf_slice!(
            mbuf,
            utils::ETHERNET2_HEADER2_SIZE,
            utils::IPV4_HEADER2_SIZE
        );

        let (src_ip, _) = utils::check_ipv4_hdr(ipv4_hdr_slice, &dst_info.ipv4_addr).unwrap();
        assert!(src_ip == src_info.ipv4_addr);

        let udp_hdr_slice = mbuf_slice!(
            mbuf,
            utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE,
            utils::UDP_HEADER2_SIZE
        );

        let (src_port, _) = utils::check_udp_hdr(udp_hdr_slice, dst_info.udp_port).unwrap();
        assert!(src_port == src_info.udp_port);

        let id_hdr_slice = mbuf_slice!(
            mbuf,
            utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE + utils::UDP_HEADER2_SIZE,
            4
        );

        let msg_id = utils::parse_msg_id(id_hdr_slice);
        assert!(msg_id == 1);

        let (msg_id, addr_info) = check_valid_packet(mbuf, &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);
    }

    #[test]
    fn invalid_headers() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[0] = test_mbuf.get_pointer();
        let mut pkt = Pkt::init(1);
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        // now test that the packet does NOT have a valid destination ether addr
        let eth_hdr_slice = mbuf_slice!(mbufs[0], 0, utils::ETHERNET2_HEADER2_SIZE);
        match utils::check_eth_hdr(eth_hdr_slice, &random_mac()) {
            Ok(_) => {
                panic!("Dst mac address should have been invalid.");
            }
            Err(_) => {}
        }

        let ipv4_hdr_slice = mbuf_slice!(
            mbufs[0],
            utils::ETHERNET2_HEADER2_SIZE,
            utils::IPV4_HEADER2_SIZE
        );

        match utils::check_ipv4_hdr(ipv4_hdr_slice, &random_ip()) {
            Ok(_) => {
                panic!("Destination ipv4 address should have been invalid.");
            }
            Err(_) => {}
        }

        let udp_hdr_slice = mbuf_slice!(
            mbufs[0],
            utils::ETHERNET2_HEADER2_SIZE + utils::IPV4_HEADER2_SIZE,
            utils::UDP_HEADER2_SIZE
        );

        match utils::check_udp_hdr(udp_hdr_slice, dst_info.udp_port + 1) {
            Ok(_) => {
                panic!("Destination udp port should have been wrong.");
            }
            Err(_) => {}
        }

        let fake_addr_info =
            utils::AddressInfo::new(dst_info.udp_port + 1, random_ip(), random_mac());
        match check_valid_packet(mbufs[0], &fake_addr_info) {
            Some(_) => {
                panic!("Packet isn't valid, check_valid_packet should fail.");
            }
            None => {}
        }
    }

    #[test]
    fn check_payload() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut pkt = Pkt::init(1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[0] = test_mbuf.get_pointer();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);
        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);
        let payload1 = rand::thread_rng().gen::<[u8; 32]>();
        let payload2 = payload1.clone();
        cornflake.add_entry(CornPtr::Normal(payload1.as_ref()));
        cornflake.add_entry(CornPtr::Normal(payload2.as_ref()));
        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 64);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 64);
        }

        let first_payload = mbuf_slice!(mbufs[0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1));

        let second_payload = mbuf_slice!(mbufs[0], utils::TOTAL_HEADER_SIZE + 32, 32);
        let second_payload_sized: &[u8; 32] = &second_payload[0..32].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload2));
    }

    #[test]
    fn encode_sga_single_owned() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut pkt = Pkt::init(1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[0] = test_mbuf.get_pointer();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let payload = rand::thread_rng().gen::<[u8; 32]>();
        cornflake.add_entry(CornPtr::Normal(payload.as_ref()));

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 32);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 32);
            assert!(((*(mbufs[0])).next).is_null());
            assert!((*(mbufs[0])).nb_segs as usize == 1);
        }

        let first_payload = mbuf_slice!(mbufs[0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload));
    }

    #[test]
    fn encode_sga_multiple_owned() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let payload1 = rand::thread_rng().gen::<[u8; 32]>();
        let payload2 = rand::thread_rng().gen::<[u8; 32]>();
        cornflake.add_entry(CornPtr::Normal(payload1.as_ref()));
        cornflake.add_entry(CornPtr::Normal(payload2.as_ref()));

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[0] = test_mbuf.get_pointer();
        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        assert!(pkt.num_entries() == 1);

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 32 + 32);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 32 + 32);
            assert!((*(mbufs[0])).nb_segs as usize == 1);
            assert!(((*(mbufs[0])).next).is_null());
        }

        let first_payload = mbuf_slice!(mbufs[0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1));

        let second_payload = mbuf_slice!(mbufs[0], utils::TOTAL_HEADER_SIZE + 32, 32);
        let second_payload_sized: &[u8; 32] = &second_payload[0..32].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload2));
    }

    #[test]
    fn encode_single_external_with_mmap() {
        // encode an sga that refers to a single external memory
        // construct fake mmap metadata
        test_init!();
        let mut header_mbuf = TestMbuf::new();
        let mut test_mbuf = TestMbuf::new_external();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);
        tracing::debug!(
            ext_has_external = test_mbuf.has_external,
            header_has_external = header_mbuf.has_external,
            "has external",
        );

        let bytes_vec = get_random_bytes(256);
        let payload1 = bytes_vec.as_slice();
        cornflake.add_entry(CornPtr::Registered(&payload1[200..256].as_ref()));
        let mmap_vec = vec![MmapMetadata::test_mmap(
            (&payload1).as_ptr(),
            payload1.len(),
            vec![(&payload1).as_ptr() as usize],
            0,
        )];
        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[1] = test_mbuf.get_pointer();
        mbufs[0] = header_mbuf.get_pointer();
        info!("Initialized packet with {} entries", pkt.num_entries());
        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &mmap_vec)
            .unwrap();
        info!("Constructed packet successfully");

        assert!(pkt.num_entries() == 2);

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 56);
            assert!((*(mbufs[1])).data_len as usize == 56);
            assert!((*(mbufs[0])).nb_segs as usize == 2);
            assert!((*(mbufs[1])).data_off as usize == 0);
            assert!((*(mbufs[0])).next == mbufs[1]);
            assert!(((*(mbufs[1])).next).is_null());
        }

        let (msg_id, addr_info) = check_valid_packet(mbufs[0], &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);

        let second_payload = mbuf_slice!(mbufs[1], 0, 56);
        debug!("Second payload addr: {:?}", &second_payload);
        let second_payload_sized: &[u8; 56] = &second_payload[0..56].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload1[200..256]));
        tracing::debug!(
            ext_has_external = test_mbuf.has_external,
            header_has_external = header_mbuf.has_external,
            "has external",
        );
    }

    #[test]
    fn encode_sga_single_external_with_native_mbuf() {
        // encode an sga that refers to a single external memory
        // use the default memzone argument (pointing to another "fake" mbuf)
        test_init!();
        let mut header_mbuf = TestMbuf::new();
        let mut test_mbuf = TestMbuf::new_external();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let bytes_vec = get_random_bytes(256);
        let payload1 = bytes_vec.as_slice();
        debug!("Original payload addr: {:?}", &payload1);
        cornflake.add_entry(CornPtr::Registered(&payload1[200..256]));
        let default_memsegments = (
            (&payload1).as_ptr() as usize,
            (&payload1).as_ptr() as usize + 256,
        );

        debug!("Memsegments we are passing in {:?}", default_memsegments);
        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[1] = test_mbuf.get_pointer();
        mbufs[0] = header_mbuf.get_pointer();
        info!("Initialized packet with {} entries", pkt.num_entries());
        pkt.construct_from_test_sga(
            &cornflake,
            &mut mbufs,
            &hdr_info,
            default_memsegments,
            &Vec::default(),
        )
        .unwrap();
        info!("Constructed packet successfully");

        assert!(pkt.num_entries() == 2);

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 56);
            assert!((*(mbufs[1])).data_len as usize == 56);
            assert!((*(mbufs[0])).nb_segs as usize == 2);
            assert!((*(mbufs[0])).next == mbufs[1]);
            assert!(((*(mbufs[1])).next).is_null());
        }

        let (msg_id, addr_info) = check_valid_packet(mbufs[0], &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);

        let second_payload = mbuf_slice!(mbufs[1], 0, 56);
        debug!("Second payload addr: {:?}", &second_payload);
        let second_payload_sized: &[u8; 56] = &second_payload[0..56].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload1[200..256]));
        tracing::debug!("Done running test");
    }

    #[test]
    fn encode_sga_multiple_external() {
        test_init!();
        let mut header_mbuf = TestMbuf::new();
        let mut test_mbuf1 = TestMbuf::new_external();
        let mut test_mbuf2 = TestMbuf::new_external();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let bytes_vec = get_random_bytes(400);
        let payload1 = bytes_vec.as_slice();
        cornflake.add_entry(CornPtr::Registered(&payload1[200..300]));
        cornflake.add_entry(CornPtr::Registered(&payload1[300..400]));
        let default_memsegments = (
            (&payload1).as_ptr() as usize,
            (&payload1).as_ptr() as usize + 400,
        );

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[1] = test_mbuf1.get_pointer();
        mbufs[0] = header_mbuf.get_pointer();
        mbufs[2] = test_mbuf2.get_pointer();
        pkt.construct_from_test_sga(
            &cornflake,
            &mut mbufs,
            &hdr_info,
            default_memsegments,
            &Vec::default(),
        )
        .unwrap();

        assert!(pkt.num_entries() == 3);

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 100 + 100);
            assert!((*(mbufs[0])).nb_segs as usize == 3);
            assert!((*(mbufs[1])).data_len as usize == 100);
            assert!((*(mbufs[1])).data_off as usize == 8);
            assert!((*(mbufs[2])).data_len as usize == 100);
            assert!((*(mbufs[2])).data_off as usize == 108);
            assert!((*(mbufs[1])).pkt_len as usize == 0);
            assert!((*(mbufs[2])).pkt_len as usize == 0);
            assert!((*(mbufs[0])).next == mbufs[1]);
            assert!((*(mbufs[1])).next == mbufs[2]);
            assert!(((*(mbufs[2])).next).is_null());
        }

        let (msg_id, addr_info) = check_valid_packet(mbufs[0], &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);

        let first_payload = mbuf_slice!(mbufs[1], 0, 100);
        let first_payload_sized: &[u8; 100] = &first_payload[0..100].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1[200..300]));

        let second_payload = mbuf_slice!(mbufs[2], 0, 100);
        let second_payload_sized: &[u8; 100] = &second_payload[0..100].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload1[300..400]));
    }

    #[test]
    fn encode_sga_mixed_regions() {
        test_init!();
        let mut header_mbuf = TestMbuf::new();
        let mut test_mbuf1 = TestMbuf::new_external();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(3);

        let payload1 = rand::thread_rng().gen::<[u8; 32]>();
        let bytes_vec = get_random_bytes(232);
        let payload2 = bytes_vec.as_slice();
        cornflake.add_entry(CornPtr::Normal(payload1.as_ref()));
        cornflake.add_entry(CornPtr::Registered(&payload2[200..232]));
        let default_memsegments = (
            (&payload2).as_ptr() as usize,
            (&payload2).as_ptr() as usize + 256,
        );

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[1] = test_mbuf1.get_pointer();
        mbufs[0] = header_mbuf.get_pointer();
        pkt.construct_from_test_sga(
            &cornflake,
            &mut mbufs,
            &hdr_info,
            default_memsegments,
            &Vec::default(),
        )
        .unwrap();

        assert!(pkt.num_entries() == 2);

        unsafe {
            assert!((*(mbufs[0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 32);
            assert!((*(mbufs[0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 32 + 32);
            assert!((*(mbufs[0])).nb_segs as usize == 2);
            assert!((*(mbufs[1])).data_len as usize == 32);
            assert!((*(mbufs[1])).data_off as usize == 8);
            assert!((*(mbufs[1])).pkt_len as usize == 0);
            assert!((*(mbufs[0])).next == mbufs[1]);
            assert!(((*(mbufs[1])).next).is_null());
        }

        let (msg_id, addr_info) = check_valid_packet(mbufs[0], &dst_info).unwrap();
        assert!(msg_id == 3);
        assert!(addr_info == src_info);

        let first_payload = mbuf_slice!(mbufs[0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1));

        let second_payload = mbuf_slice!(mbufs[1], 0, 32);
        let second_payload_sized: &[u8; 32] = &second_payload[0..32].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload2[200..232]));
    }

    #[test]
    fn encode_sga_incorrect() {
        test_init!();
        let mut header_mbuf = TestMbuf::new();
        let mut test_mbuf1 = TestMbuf::new_external();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(3);

        let payload1 = rand::thread_rng().gen::<[u8; 32]>();
        let payload2 = rand::thread_rng().gen::<[u8; 32]>();
        cornflake.add_entry(CornPtr::Registered(payload2.as_ref()));
        cornflake.add_entry(CornPtr::Normal(payload1.as_ref()));

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [*mut rte_mbuf; 32] = [ptr::null_mut(); 32];
        mbufs[1] = test_mbuf1.get_pointer();
        mbufs[0] = header_mbuf.get_pointer();
        match pkt.construct_from_test_sga(
            &cornflake,
            &mut mbufs,
            &hdr_info,
            (0, 0),
            &Vec::default(),
        ) {
            Ok(_) => {
                panic!("Should have failed to construct SGA because owned region comes after borrowed region.");
            }
            Err(_) => {}
        }
    }
}
