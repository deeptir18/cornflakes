#![allow(unused_assignments)]
use super::{
    super::{dpdk_call, dpdk_check_not_failed, dpdk_ok, mbuf_slice},
    allocator::MempoolAllocator,
    dpdk_bindings::*,
    dpdk_check, dpdk_error, dpdk_utils,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{mem, utils, CornType, MsgID, PtrAttributes, ScatterGather};
use hashbrown::HashMap;
use std::{
    ffi::{CStr, CString},
    mem::{size_of, MaybeUninit},
    ptr, slice,
    time::Duration,
};

#[cfg(feature = "profiler")]
use perftools;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct MempoolPtr(pub *mut rte_mempool);
unsafe impl Send for MempoolPtr {}

fn print_error() -> String {
    let errno = dpdk_call!(rte_errno());
    let c_buf = dpdk_call!(rte_strerror(errno));
    let c_str: &CStr = unsafe { CStr::from_ptr(c_buf) };
    let str_slice: &str = c_str.to_str().unwrap();
    format!("Error {}: {:?}", errno, str_slice)
}

use tracing::{debug, info};

/// Constants related to DPDK
pub const NUM_MBUFS: u16 = 8191;
pub const MBUF_CACHE_SIZE: u16 = 250;
const RX_RING_SIZE: u16 = 2048;
const TX_RING_SIZE: u16 = 2048;
pub const MAX_SCATTERS: usize = 33;
pub const RECEIVE_BURST_SIZE: u16 = 32;
pub const MEMPOOL_MAX_SIZE: usize = 65536;

pub const RX_PACKET_LEN: u32 = 9216;
pub const MBUF_BUF_SIZE: u32 = RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM;
pub const MBUF_PRIV_SIZE: usize = 8;
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

/// Constructs and sends a packet with an mbuf from the given mempool, copying the payload.
pub fn get_mbuf_with_memcpy(
    header_mempool: *mut rte_mempool,
    header_info: &utils::HeaderInfo,
    buf: &[u8],
    id: MsgID,
) -> Result<*mut rte_mbuf> {
    tracing::debug!(header_mempool_avail =? dpdk_call!(rte_mempool_avail_count(header_mempool)), "Number available in header_mempool");
    tracing::debug!(len = buf.len(), "Copying single buffer into mbuf");
    let header_mbuf =
        alloc_mbuf(header_mempool).wrap_err("Unable to allocate mbuf from mempool.")?;
    let data_offset = fill_in_header(header_mbuf, header_info, buf.len(), id)
        .wrap_err("unable to fill header info.")?;
    unsafe {
        tracing::debug!(
            len = data_offset + buf.len(),
            "Full length of packet being sent"
        );
        (*header_mbuf).data_len = (data_offset + buf.len()) as u16;
        (*header_mbuf).pkt_len = (data_offset + buf.len()) as u32;
        (*header_mbuf).next = ptr::null_mut();
        (*header_mbuf).nb_segs = 1;
    }
    // copy the payload into the mbuf
    let ctx_hdr_slice = mbuf_slice!(header_mbuf, data_offset, buf.len());
    dpdk_call!(rte_memcpy_wrapper(
        ctx_hdr_slice.as_mut_ptr() as _,
        buf.as_ref().as_ptr() as _,
        buf.len(),
    ));
    Ok(header_mbuf)
}

/// Wrapper around a linked lists of mbufs,
/// for constructing mbufs to send with rte_eth_tx_burst.
pub struct Pkt {
    /// Num filed so far.
    _num_filled: usize,
}

impl Pkt {
    /// Initialize a Pkt data structure, with a given number of segments.
    /// Allocates space in the `mbufs` and `data_offsets` fields according to the size.
    ///
    /// Arguments:
    /// * size - usize which represents the number of scatter-gather segments that will be in this
    /// packet.
    pub fn init() -> Pkt {
        Pkt { _num_filled: 0 }
    }

    pub fn construct_from_sga_without_scatter_gather(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        sga: &impl ScatterGather,
        header_mempool: *mut rte_mempool,
        header_info: &utils::HeaderInfo,
    ) -> Result<()> {
        // 1: allocate and add header mbuf
        let header_mbuf =
            alloc_mbuf(header_mempool).wrap_err("Unable to allocate mbuf from mempool.")?;
        self.add_header_mbuf(header_mbuf, (header_info, sga.data_len(), sga.get_id()), 1)
            .wrap_err("Unable to initialize and add header mbuf.")?;
        mbufs[0][pkt_id] = header_mbuf;

        // 3: copy the payloads from the sga into the mbufs
        self.copy_all_payloads(mbufs, pkt_id, sga)
            .wrap_err("Error in copying payloads from sga to mbufs.")?;
        tracing::debug!(
            "Final pkt len: {} and data len: {}",
            unsafe { (*header_mbuf).pkt_len },
            unsafe { (*header_mbuf).data_len }
        );

        Ok(())
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
    /// * memzones: Vector of memzones for native mbuf allocations.
    /// * external_regions: Vector of information about externally registered memory regions.
    pub fn construct_from_sga(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        sga: &impl ScatterGather,
        header_mempool: *mut rte_mempool,
        extbuf_mempool: *mut rte_mempool,
        header_info: &utils::HeaderInfo,
        allocator: &MempoolAllocator,
        external_regions: &Vec<mem::MmapMetadata>,
        splits_per_chunk: usize,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        perftools::timer!("Construct from sga with scatter-gather");
        // TODO: change this back
        // 1: allocate and add header mbuf
        mbufs[0][pkt_id] =
            alloc_mbuf(header_mempool).wrap_err("Unable to allocate mbuf from mempool.")?;
        self.add_header_mbuf(
            mbufs[0][pkt_id],
            (header_info, sga.data_len(), sga.get_id()),
            sga.num_borrowed_segments() * splits_per_chunk + 1,
        )
        .wrap_err("Unable to initialize and add header mbuf.")?;

        // 2: allocate and add mbufs for external mbufs
        for i in 1..sga.num_borrowed_segments() * splits_per_chunk + 1 {
            let mbuf = alloc_mbuf(extbuf_mempool)
                .wrap_err("Unable to allocate externally allocated mbuf.")?;
            mbufs[i][pkt_id] = mbuf;
            let last_mbuf = mbufs[i - 1][pkt_id];
            unsafe {
                (*last_mbuf).next = mbuf;
                (*mbuf).next = ptr::null_mut();
                (*mbuf).data_len = 0;
            }
        }

        // 3: copy the payloads from the sga into the mbufs
        self.set_sga_payloads(
            mbufs,
            pkt_id,
            sga,
            allocator,
            external_regions,
            splits_per_chunk,
        )
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
    /// * allocator: Mempool allocator with information about the mempools.
    /// * external_regions: Vector of information about externally registered memory regions.
    #[cfg(test)]
    pub fn construct_from_test_sga(
        &mut self,
        sga: &impl ScatterGather,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        header_info: &utils::HeaderInfo,
        allocator: &MempoolAllocator,
        external_regions: &Vec<mem::MmapMetadata>,
    ) -> Result<()> {
        self.add_header_mbuf(
            mbufs[0][0],
            (header_info, sga.data_len(), sga.get_id()),
            sga.num_borrowed_segments() + 1,
        )
        .wrap_err("Failed to add header mbuf.")?;
        self._num_filled += 1;

        for i in 1..sga.num_borrowed_segments() + 1 {
            self._num_filled += 1;
            let mbuf = mbufs[i][0];
            let last_mbuf = mbufs[i - 1][0];
            unsafe {
                (*last_mbuf).next = mbuf;
                (*mbuf).next = ptr::null_mut();
                (*mbuf).data_len = 0;
            }
        }

        // 3: copy the payloads from the sga into the mbufs
        self.set_sga_payloads(mbufs, 0, sga, allocator, external_regions, 1)
            .wrap_err("Error in copying payloads from sga to mbufs.")?;

        Ok(())
    }

    fn add_header_mbuf(
        &mut self,
        header_mbuf: *mut rte_mbuf,
        header_info: (&utils::HeaderInfo, usize, MsgID),
        num_total_mbufs: usize,
    ) -> Result<()> {
        let (info, payload_len, id) = header_info;
        let data_offset = fill_in_header(header_mbuf, info, payload_len, id)
            .wrap_err("unable to fill header info.")?;
        unsafe {
            (*header_mbuf).data_len = data_offset as u16;
            (*header_mbuf).pkt_len = (data_offset + payload_len) as u32;
            (*header_mbuf).next = ptr::null_mut();
            (*header_mbuf).nb_segs = num_total_mbufs as u16;
        }
        Ok(())
    }

    fn copy_all_payloads(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        sga: &impl ScatterGather,
    ) -> Result<()> {
        for i in 0..sga.num_segments() {
            let cornptr = sga.index(i);
            tracing::debug!(seg = i, len = cornptr.as_ref().len(), addr=? cornptr.as_ref().as_ptr(), "Copying cornptr into packet");
            // copy this payload into the head buffer
            self.copy_payload(mbufs, pkt_id, 0, cornptr.as_ref())
                .wrap_err("Failed to copy sga owned entry {} into pkt list.")?;
        }
        Ok(())
    }

    /// Copies the given sga's payloads into the Pkt's mbufs.
    /// The mbufs must already be initialized.
    ///
    /// Arguments:
    /// * sga - Object that implements the scatter-gather trait to copy payloads from.
    /// * allocator - Mempool allocator that has information about the packet mempools.
    /// * external_regions: Vector of information about externally registered memory regions.
    fn set_sga_payloads(
        &mut self,
        mbufs: &mut [[*mut rte_mbuf; RECEIVE_BURST_SIZE as usize]; MAX_SCATTERS],
        pkt_id: usize,
        sga: &impl ScatterGather,
        allocator: &MempoolAllocator,
        external_regions: &Vec<mem::MmapMetadata>,
        splits_per_chunk: usize,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        perftools::timer!("Set sga payloads func");
        let mut current_attached_idx = 0;
        for i in 0..sga.num_segments() {
            let cornptr = sga.index(i);
            // any attached mbufs will start at index 1 (1 after header)
            match cornptr.buf_type() {
                CornType::Registered => {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Processing registered");
                    current_attached_idx += 1;
                    self.set_external_payload(
                        mbufs,
                        pkt_id,
                        current_attached_idx,
                        cornptr.as_ref(),
                        allocator,
                        external_regions,
                        sga.get_id(),
                        splits_per_chunk,
                    )
                    .wrap_err("Failed to set external payload into pkt list.")?;
                    if splits_per_chunk > 1 {
                        current_attached_idx += splits_per_chunk - 1;
                    }
                }
                CornType::Normal => {
                    #[cfg(feature = "profiler")]
                    perftools::timer!("Processing copies");
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
        allocator: &MempoolAllocator,
        external_regions: &Vec<mem::MmapMetadata>,
        sga_id: MsgID,
        splits_per_chunk: usize,
    ) -> Result<()> {
        #[cfg(feature = "profiler")]
        perftools::timer!("Set external payload func");

        debug!("The mbuf idx we're changing: {}", idx);
        // check whether the payload is in one of the memzones, or an external region
        tracing::debug!(
            "[set external payload] Address of payload: {:?}",
            buf.as_ptr()
        );

        let mut in_native_mempool = false;
        let mut in_external_pool = false;
        if let Some((original_mbuf_ptr, data_off)) = allocator.recover_mbuf_ptr(buf)? {
            let (buf_iova, buf_addr) =
                unsafe { ((*original_mbuf_ptr).buf_iova, (*original_mbuf_ptr).buf_addr) };
            let chunk_size = buf.len() / splits_per_chunk;
            for chunk in 0..splits_per_chunk {
                tracing::debug!(
                    "Pointer to current extbuf: {:?}, Pointer to original mbuf ptr: {:?}",
                    mbufs[idx + chunk][pkt_id],
                    original_mbuf_ptr
                );
                unsafe {
                    if (*original_mbuf_ptr).buf_addr == std::ptr::null_mut() {
                        tracing::warn!(
                            "Calculated incorrect ptr: {:?}, for buf: {:?} req id {}",
                            original_mbuf_ptr,
                            buf.as_ptr(),
                            sga_id,
                        );
                    }
                    (*mbufs[idx + chunk][pkt_id]).buf_iova = buf_iova;
                    (*mbufs[idx + chunk][pkt_id]).buf_addr = buf_addr;
                    // TODO: do we even need to modify buf_len here?
                    //(*mbufs[idx + chunk][pkt_id]).buf_len = buf_len as _;
                    (*mbufs[idx + chunk][pkt_id]).data_len = chunk_size as u16;
                    (*mbufs[idx + chunk][pkt_id]).data_off = data_off + (chunk_size * chunk) as u16;
                }
                // set lkey of packet to be -1
                dpdk_call!(set_lkey_not_present(mbufs[idx + chunk][pkt_id]));
                dpdk_call!(set_refers_to_another(mbufs[idx + chunk][pkt_id], 1));
                dpdk_call!(rte_pktmbuf_refcnt_update_or_free(original_mbuf_ptr, 1));
            }
            in_native_mempool = true;
        }
        if !in_native_mempool {
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
                        (*mbufs[idx][pkt_id]).data_len = buf.len() as u16;
                    }
                    in_external_pool = true;
                    break;
                }
            }
        }

        if !in_native_mempool && !in_external_pool {
            bail!("For buffer: {:?}, pkt_id: {}, mbuf list id: {}: could not find native mempool or external memory region.", buf.as_ptr(), pkt_id, idx);
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn num_entries(&self) -> usize {
        self._num_filled
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

fn initialize_dpdk_port(
    port_id: u16,
    num_queues: u16,
    rx_mbuf_pools: &Vec<*mut rte_mempool>,
) -> Result<()> {
    ensure!(
        num_queues as usize == rx_mbuf_pools.len(),
        format!(
            "Mbuf pool list length {} not the same as num_queues {}",
            rx_mbuf_pools.len(),
            num_queues
        )
    );
    assert!(dpdk_call!(rte_eth_dev_is_valid_port(port_id)) == 1);
    let rx_rings: u16 = num_queues;
    let tx_rings: u16 = num_queues;
    let nb_rxd = RX_RING_SIZE;
    let nb_txd = TX_RING_SIZE;

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

    dpdk_call!(eth_dev_configure(port_id, rx_rings, tx_rings));

    let socket_id =
        dpdk_check_not_failed!(rte_eth_dev_socket_id(port_id), "Port id is out of range") as u32;

    // allocate and set up 1 RX queue per Ethernet port
    for i in 0..rx_rings {
        tracing::debug!("Initializing ring {}", i);
        dpdk_ok!(rte_eth_rx_queue_setup(
            port_id,
            i,
            nb_rxd,
            socket_id,
            rx_conf.as_mut_ptr(),
            rx_mbuf_pools[i as usize]
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
pub fn init_extbuf_mempool(name: &str, nb_ports: u16) -> Result<*mut rte_mempool> {
    let name = CString::new(name)?;

    let mut mbp_priv_uninit: MaybeUninit<rte_pktmbuf_pool_private> = MaybeUninit::zeroed();
    unsafe {
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_data_room_size = 0;
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_priv_size = MBUF_PRIV_SIZE as _;
    }

    // actual data size is empty (just mbuf and priv data)
    let elt_size: u32 = size_of::<rte_mbuf>() as u32 + MBUF_PRIV_SIZE as u32;

    tracing::debug!(elt_size, "Trying to init extbuf mempool with elt_size");
    let mbuf_pool = dpdk_call!(rte_mempool_create_empty(
        name.as_ptr(),
        (NUM_MBUFS * nb_ports) as u32,
        elt_size,
        MBUF_CACHE_SIZE.into(),
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

/// Creates a mempool with the given value size, and number of values.
pub fn create_mempool(
    name: &str,
    nb_ports: u16,
    data_size: usize,
    num_values: usize,
) -> Result<*mut rte_mempool> {
    let name_str = CString::new(name)?;

    /*let mut mbp_priv_uninit: MaybeUninit<rte_pktmbuf_pool_private> = MaybeUninit::zeroed();
    unsafe {
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_data_room_size = data_size as u16;
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_priv_size = MBUF_PRIV_SIZE as _;
    }

    // actual data size is empty (just mbuf and priv data)
    let elt_size: u32 = size_of::<rte_mbuf>() as u32 + MBUF_PRIV_SIZE as u32 + data_size as u32;

    tracing::debug!(elt_size, "Trying to init extbuf mempool with elt_size");
    let mbuf_pool = dpdk_call!(rte_mempool_create_empty(
        name.as_ptr(),
        (num_values * nb_ports as usize) as u32,
        elt_size,
        MBUF_CACHE_SIZE.into(),
        MBUF_PRIV_SIZE as u32,
        rte_socket_id() as i32,
        0,
    ));

    ensure!(
        !mbuf_pool.is_null(),
        "Mempool {:?} created with rte_mempool_create_empty is null.",
        name
    );

    // initialize the private data space
    dpdk_call!(rte_pktmbuf_pool_init(
        mbuf_pool,
        mbp_priv_uninit.as_mut_ptr() as _
    ));

    // populate
    if dpdk_call!(rte_mempool_populate_default(mbuf_pool))
        != (num_values * nb_ports as usize) as i32
    {
        dpdk_call!(rte_mempool_free(mbuf_pool));
        bail!(
            "Not able to initialize mempool {:?}: Failed on rte_mempool_populate_default; ERR: {:?}",
            name,
            print_error(),
        );
    }

    // initialize each mbuf
    if dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(rte_pktmbuf_init),
        ptr::null_mut()
    )) != (num_values as u16 * nb_ports) as u32
    {
        dpdk_call!(rte_mempool_free(mbuf_pool));
        bail!(
            "Not able to initialize mempool {:?}: failed at rte_pktmbuf_init; ERR: {:?}",
            name,
            print_error()
        );
    }*/

    let mbuf_pool = dpdk_call!(rte_pktmbuf_pool_create(
        name_str.as_ptr(),
        (num_values as u16 * nb_ports) as u32,
        MBUF_CACHE_SIZE as u32,
        MBUF_PRIV_SIZE as u16,
        data_size as u16, // TODO: add headroom?
        rte_socket_id() as i32,
    ));
    if mbuf_pool.is_null() {
        tracing::warn!(error =? print_error(), "mbuf pool is null.");
    }
    ensure!(!mbuf_pool.is_null(), "mbuf pool null");

    // initialize private data
    if dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(custom_init_priv()),
        ptr::null_mut()
    )) != (num_values as u16 * nb_ports) as u32
    {
        dpdk_call!(rte_mempool_free(mbuf_pool));
        bail!("Not able to initialize private data in extbuf pool: failed on custom_init_priv.");
    }

    Ok(mbuf_pool)
}

pub fn create_native_mempool(name: &str, nb_ports: u16) -> Result<*mut rte_mempool> {
    let name_str = CString::new(name)?;
    let mbuf_pool = dpdk_call!(rte_pktmbuf_pool_create(
        name_str.as_ptr(),
        (NUM_MBUFS * nb_ports) as u32,
        MBUF_CACHE_SIZE as u32,
        MBUF_PRIV_SIZE as u16,
        MBUF_BUF_SIZE as u16,
        rte_socket_id() as i32,
    ));
    if mbuf_pool.is_null() {
        tracing::warn!("mbuf pool is null.");
    }
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

    Ok(mbuf_pool)
}

/// Initializes DPDK ports, and memory pools.
/// Returns two mempool types:
/// (1) One that allocates mbufs with `MBUF_BUF_SIZE` of buffer space.
/// (2) One that allocates empty mbuf structs, meant for attaching external data buffers.
fn dpdk_init_helper(num_cores: usize) -> Result<(Vec<*mut rte_mempool>, u16)> {
    let nb_ports = dpdk_call!(rte_eth_dev_count_avail());
    if nb_ports <= 0 {
        bail!("DPDK INIT: No ports available.");
    }
    info!(
        "DPDK reports that {} ports (interfaces) are available",
        nb_ports
    );

    let mut default_pools: Vec<*mut rte_mempool> = Vec::new();
    for i in 0..num_cores {
        let name = format!("default_mbuf_pool_{}", i);
        let mbuf_pool = create_native_mempool(&name, nb_ports).wrap_err(format!(
            "Not able to create mbuf pool {} in dpdk_init.",
            nb_ports
        ))?;
        default_pools.push(mbuf_pool);
    }

    let owner = RTE_ETH_DEV_NO_OWNER as u64;
    let mut p = dpdk_call!(rte_eth_find_next_owned_by(0, owner)) as u16;
    while p < RTE_MAX_ETHPORTS as u16 {
        initialize_dpdk_port(p, num_cores as u16, &default_pools)?;
        p = dpdk_call!(rte_eth_find_next_owned_by(p + 1, owner)) as u16;
    }

    Ok((default_pools, nb_ports))
}

/// Initializes DPDK EAL and ports, and memory pools given a yaml-config file.
/// Returns two mempools:
/// (1) One that allocates mbufs with `MBUF_BUF_SIZE` of buffer space.
/// (2) One that allocates empty mbuf structs, meant for attaching external data buffers.
///
/// Arguments:
/// * config_path: - A string slice that holds the path to a config file with DPDK initialization.
/// information.
pub fn dpdk_init(config_path: &str, num_cores: usize) -> Result<(Vec<*mut rte_mempool>, u16)> {
    // EAL initialization
    let eal_init = dpdk_utils::parse_eal_init(config_path)?;
    dpdk_eal_init(eal_init).wrap_err("EAL initialization failed.")?;

    // init ports, mempools on the rx side
    dpdk_init_helper(num_cores)
}

/// Returns the result of a mutable ptr to an rte_mbuf allocated from a particular mempool.
///
/// Arguments:
/// * mempool - *mut rte_mempool where packet should be allocated from.
#[inline]
pub fn alloc_mbuf(mempool: *mut rte_mempool) -> Result<*mut rte_mbuf> {
    let mbuf = dpdk_call!(rte_pktmbuf_alloc(mempool));
    if mbuf.is_null() {
        tracing::warn!(
            avail_count = dpdk_call!(rte_mempool_avail_count(mempool)),
            "Amount of mbufs available in mempool"
        );
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
        tracing::debug!("Burst back packet");
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
) -> Result<HashMap<usize, (MsgID, utils::AddressInfo, usize)>> {
    let mut valid_packets: HashMap<usize, (MsgID, utils::AddressInfo, usize)> = HashMap::new();
    let num_received = dpdk_call!(rte_eth_rx_burst(port_id, queue_id, rx_pkts, nb_pkts));
    for i in 0..num_received {
        let pkt = unsafe { *(rx_pkts.offset(i as isize)) };
        match check_valid_packet(pkt, my_addr_info) {
            Some((id, hdr, size)) => {
                valid_packets.insert(i as usize, (id, hdr, size));
            }
            None => {
                tracing::debug!("Queue {} received invalid packet, idx {}", queue_id, i,);
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
) -> Option<(MsgID, utils::AddressInfo, usize)> {
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

    let (src_port, udp_data_len) = match utils::check_udp_hdr(udp_hdr_slice, my_addr_info.udp_port)
    {
        Ok((port, _, size)) => (port, size),
        Err(_) => {
            return None;
        }
    };

    let msg_id = dpdk_call!(read_pkt_id(pkt));
    Some((
        msg_id,
        (utils::AddressInfo::new(src_port, src_ip, src_eth)),
        udp_data_len - 4,
    ))
}

pub fn refcnt(pkt: *mut rte_mbuf) -> u16 {
    dpdk_call!(rte_pktmbuf_refcnt_read(pkt))
}

/// Frees the mbuf, returns it to it's original mempool.
/// Arguments:
/// * pkt - *mut rte_mbuf to free.
#[inline]
pub fn free_mbuf(pkt: *mut rte_mbuf) {
    tracing::debug!(packet =? pkt, cur_refcnt = dpdk_call!(rte_pktmbuf_refcnt_read(pkt)), "Called free_mbuf on packet");
    dpdk_call!(rte_pktmbuf_refcnt_update_or_free(pkt, -1));
}

#[inline]
pub fn dpdk_register_extmem(
    metadata: &mem::MmapMetadata,
    lkey: *mut u32,
) -> Result<*mut std::os::raw::c_void> {
    tracing::debug!(
        "Trying to register: {:?} with lkey: {:?}",
        metadata.ptr,
        lkey
    );
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
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[0][0] = test_mbuf.get_pointer();
        let mut pkt = Pkt::init(1);
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        let mbuf = mbufs[0][0];
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

        let (msg_id, addr_info, _size) = check_valid_packet(mbuf, &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);
    }

    #[test]
    fn invalid_headers() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[0][0] = test_mbuf.get_pointer();
        let mut pkt = Pkt::init(1);
        let mut cornflake = Cornflake::default();
        cornflake.set_id(1);

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        // now test that the packet does NOT have a valid destination ether addr
        let eth_hdr_slice = mbuf_slice!(mbufs[0][0], 0, utils::ETHERNET2_HEADER2_SIZE);
        match utils::check_eth_hdr(eth_hdr_slice, &random_mac()) {
            Ok(_) => {
                panic!("Dst mac address should have been invalid.");
            }
            Err(_) => {}
        }

        let ipv4_hdr_slice = mbuf_slice!(
            mbufs[0][0],
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
            mbufs[0][0],
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
        match check_valid_packet(mbufs[0][0], &fake_addr_info) {
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
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[0][0] = test_mbuf.get_pointer();
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
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 64);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 64);
        }

        let first_payload = mbuf_slice!(mbufs[0][0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1));

        let second_payload = mbuf_slice!(mbufs[0][0], utils::TOTAL_HEADER_SIZE + 32, 32);
        let second_payload_sized: &[u8; 32] = &second_payload[0..32].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload2));
    }

    #[test]
    fn encode_sga_single_owned() {
        test_init!();
        let mut test_mbuf = TestMbuf::new();
        let mut pkt = Pkt::init(1);
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[0][0] = test_mbuf.get_pointer();
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
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 32);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 32);
            assert!(((*(mbufs[0][0])).next).is_null());
            assert!((*(mbufs[0][0])).nb_segs as usize == 1);
        }

        let first_payload = mbuf_slice!(mbufs[0][0], utils::TOTAL_HEADER_SIZE, 32);
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
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[0][0] = test_mbuf.get_pointer();
        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &Vec::default())
            .unwrap();

        assert!(pkt.num_entries() == 1);

        unsafe {
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 32 + 32);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 32 + 32);
            assert!((*(mbufs[0][0])).nb_segs as usize == 1);
            assert!(((*(mbufs[0][0])).next).is_null());
        }

        let first_payload = mbuf_slice!(mbufs[0][0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1));

        let second_payload = mbuf_slice!(mbufs[0][0], utils::TOTAL_HEADER_SIZE + 32, 32);
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

        let bytes_vec = get_random_bytes(280);
        let payload1 = bytes_vec.as_slice();
        cornflake.add_entry(CornPtr::Registered(&payload1[224..280].as_ref()));
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
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[0][0] = header_mbuf.get_pointer();
        mbufs[1][0] = test_mbuf.get_pointer();
        info!("Initialized packet with {} entries", pkt.num_entries());
        pkt.construct_from_test_sga(&cornflake, &mut mbufs, &hdr_info, (0, 0), &mmap_vec)
            .unwrap();
        info!("Constructed packet successfully");

        assert!(pkt.num_entries() == 2);

        unsafe {
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 56);
            assert!((*(mbufs[1][0])).data_len as usize == 56);
            assert!((*(mbufs[0][0])).nb_segs as usize == 2);
            assert!((*(mbufs[1][0])).data_off as usize == 0);
            assert!((*(mbufs[0][0])).next == mbufs[1][0]);
            assert!(((*(mbufs[1][0])).next).is_null());
        }

        let (msg_id, addr_info, _size) = check_valid_packet(mbufs[0][0], &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);

        let second_payload = mbuf_slice!(mbufs[1][0], 0, 56);
        debug!("Second payload addr: {:?}", &second_payload);
        let second_payload_sized: &[u8; 56] = &second_payload[0..56].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload1[224..280]));
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

        let bytes_vec = get_random_bytes(280);
        let payload1 = bytes_vec.as_slice();
        debug!("Original payload addr: {:?}", &payload1);
        cornflake.add_entry(CornPtr::Registered(&payload1[224..280]));
        let default_memsegments = (
            (&payload1).as_ptr() as usize,
            (&payload1).as_ptr() as usize + 280,
        );

        debug!("Memsegments we are passing in {:?}", default_memsegments);
        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[1][0] = test_mbuf.get_pointer();
        mbufs[0][0] = header_mbuf.get_pointer();
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
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 56);
            assert!((*(mbufs[1][0])).data_len as usize == 56);
            assert!((*(mbufs[0][0])).nb_segs as usize == 2);
            assert!((*(mbufs[0][0])).next == mbufs[1][0]);
            assert!(((*(mbufs[1][0])).next).is_null());
        }

        let (msg_id, addr_info, _size) = check_valid_packet(mbufs[0][0], &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);

        let second_payload = mbuf_slice!(mbufs[1][0], 0, 56);
        debug!("Second payload addr: {:?}", &second_payload);
        let second_payload_sized: &[u8; 56] = &second_payload[0..56].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload1[224..280]));
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

        let bytes_vec = get_random_bytes(500);
        let payload1 = bytes_vec.as_slice();
        cornflake.add_entry(CornPtr::Registered(&payload1[300..400]));
        cornflake.add_entry(CornPtr::Registered(&payload1[400..500]));
        let default_memsegments = (
            (&payload1).as_ptr() as usize,
            (&payload1).as_ptr() as usize + 500,
        );

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[1][0] = test_mbuf1.get_pointer();
        mbufs[0][0] = header_mbuf.get_pointer();
        mbufs[2][0] = test_mbuf2.get_pointer();
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
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 100 + 100);
            assert!((*(mbufs[0][0])).nb_segs as usize == 3);
            assert!((*(mbufs[1][0])).data_len as usize == 100);
            tracing::debug!("Actual data off: {:?}", (*(mbufs[1][0])).data_off);
            assert!(
                (*(mbufs[1][0])).data_off as usize
                    == 300
                        - (MEMHDR_OFFSET
                            + MBUF_POOL_HDR_SIZE
                            + RTE_PKTMBUF_HEADROOM as usize
                            + MBUF_PRIV_SIZE)
            );
            assert!((*(mbufs[2][0])).data_len as usize == 100);
            assert!(
                (*(mbufs[2][0])).data_off as usize
                    == 400
                        - (MEMHDR_OFFSET
                            + MBUF_POOL_HDR_SIZE
                            + RTE_PKTMBUF_HEADROOM as usize
                            + MBUF_PRIV_SIZE)
            );
            assert!((*(mbufs[1][0])).pkt_len as usize == 0);
            assert!((*(mbufs[2][0])).pkt_len as usize == 0);
            assert!((*(mbufs[0][0])).next == mbufs[1][0]);
            assert!((*(mbufs[1][0])).next == mbufs[2][0]);
            assert!(((*(mbufs[2][0])).next).is_null());
        }

        let (msg_id, addr_info, _size) = check_valid_packet(mbufs[0][0], &dst_info).unwrap();
        assert!(msg_id == 1);
        assert!(addr_info == src_info);

        let first_payload = mbuf_slice!(mbufs[1][0], 0, 100);
        let first_payload_sized: &[u8; 100] = &first_payload[0..100].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1[300..400]));

        let second_payload = mbuf_slice!(mbufs[2][0], 0, 100);
        let second_payload_sized: &[u8; 100] = &second_payload[0..100].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload1[400..500]));
    }

    #[test]
    fn encode_sga_mixed_regions() {
        test_init!();
        let mut header_mbuf = TestMbuf::new();
        let mut test_mbuf1 = TestMbuf::new_external();
        let mut cornflake = Cornflake::default();
        cornflake.set_id(3);

        let payload1 = rand::thread_rng().gen::<[u8; 32]>();
        let bytes_vec = get_random_bytes(256);
        let payload2 = bytes_vec.as_slice();
        cornflake.add_entry(CornPtr::Normal(payload1.as_ref()));
        cornflake.add_entry(CornPtr::Registered(&payload2[224..256]));
        let default_memsegments = (
            (&payload2).as_ptr() as usize,
            (&payload2).as_ptr() as usize + 256,
        );

        let src_info = utils::AddressInfo::new(12345, Ipv4Addr::LOCALHOST, MacAddress::broadcast());
        let dst_info = utils::AddressInfo::new(12345, Ipv4Addr::BROADCAST, MacAddress::default());
        let hdr_info = utils::HeaderInfo::new(src_info, dst_info);

        let mut pkt = Pkt::init(cornflake.num_borrowed_segments() + 1);
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[1][0] = test_mbuf1.get_pointer();
        mbufs[0][0] = header_mbuf.get_pointer();
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
            assert!((*(mbufs[0][0])).data_len as usize == utils::TOTAL_HEADER_SIZE + 32);
            assert!((*(mbufs[0][0])).pkt_len as usize == utils::TOTAL_HEADER_SIZE + 32 + 32);
            assert!((*(mbufs[0][0])).nb_segs as usize == 2);
            assert!((*(mbufs[1][0])).data_len as usize == 32);
            assert!(
                (*(mbufs[1][0])).data_off as usize
                    == 224
                        - (MEMHDR_OFFSET
                            + MBUF_POOL_HDR_SIZE
                            + RTE_PKTMBUF_HEADROOM as usize
                            + MBUF_PRIV_SIZE)
            );
            assert!((*(mbufs[1][0])).pkt_len as usize == 0);
            assert!((*(mbufs[0][0])).next == mbufs[1][0]);
            assert!(((*(mbufs[1][0])).next).is_null());
        }

        let (msg_id, addr_info, _size) = check_valid_packet(mbufs[0][0], &dst_info).unwrap();
        assert!(msg_id == 3);
        assert!(addr_info == src_info);

        let first_payload = mbuf_slice!(mbufs[0][0], utils::TOTAL_HEADER_SIZE, 32);
        let first_payload_sized: &[u8; 32] = &first_payload[0..32].try_into().unwrap();
        assert!(first_payload_sized.eq(&payload1));

        let second_payload = mbuf_slice!(mbufs[1][0], 0, 32);
        let second_payload_sized: &[u8; 32] = &second_payload[0..32].try_into().unwrap();
        assert!(second_payload_sized.eq(&payload2[224..256]));
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
        let mut mbufs: [[*mut rte_mbuf; 32]; 33] = [[ptr::null_mut(); 32]; 33];
        mbufs[1][0] = test_mbuf1.get_pointer();
        mbufs[0][0] = header_mbuf.get_pointer();
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
