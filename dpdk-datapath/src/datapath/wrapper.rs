use super::{super::dpdk_bindings::*, dpdk_check};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use std::{
    ffi::{CStr, CString},
    mem::{size_of, MaybeUninit},
    ptr,
    time::Duration,
};

/// DPDK memory pool parameters
const DEFAULT_MBUFS_PER_MEMPOOL: u16 = 8191;
const MBUF_CACHE_SIZE: u16 = 250;
const DEFAULT_MBUF_BUF_SIZE: u32 = RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM;
const MBUF_PRIV_SIZE: usize = 8;

pub fn wait_for_link_status_up(port_id: u16) -> Result<()> {
    let sleep_duration_ms = Duration::from_millis(100);
    let retry_count: u32 = 90;

    let mut link: MaybeUninit<rte_eth_link> = MaybeUninit::zeroed();
    for _i in 0..retry_count {
        dpdk_check_not_errored!(rte_eth_link_get_nowait(port_id, link.as_mut_ptr()));
        let link = unsafe { link.assume_init() };
        if RTE_ETH_LINK_UP == link.link_status() as u32 {
            let duplex = if link.link_duplex() as u32 == RTE_ETH_LINK_FULL_DUPLEX {
                "full"
            } else {
                "half"
            };
            tracing::info!(
                "Port {} Link Up - speed {} Mbps - {} duplex",
                port_id,
                link.link_speed,
                duplex
            );
            return Ok(());
        }
        unsafe {
            rte_delay_us_block(sleep_duration_ms.as_micros() as u32);
        }
    }
    bail!("Link never came up");
}

pub fn create_recv_mempool(name: &str) -> Result<*mut rte_mempool> {
    let name_str = CString::new(name)?;
    let mbuf_pool = unsafe {
        rte_pktmbuf_pool_create(
            name_str.as_ptr(),
            DEFAULT_MBUFS_PER_MEMPOOL as u32,
            MBUF_CACHE_SIZE as u32,
            MBUF_PRIV_SIZE as u16,
            DEFAULT_MBUF_BUF_SIZE as u16,
            rte_socket_id() as i32,
        )
    };

    if mbuf_pool.is_null() {
        bail!("Mbuf pool null");
    }

    // initialize private data in mempool: set all lkeys to negative 1
    if unsafe { rte_mempool_obj_iter(mbuf_pool, Some(custom_init_priv()), ptr::null_mut()) }
        != DEFAULT_MBUFS_PER_MEMPOOL as u32
    {
        unsafe { rte_mempool_free(mbuf_pool) };
        bail!("Not able to initialize private data in recv pool.");
    }

    Ok(mbuf_pool)
}

pub fn create_extbuf_pool(name: &str) -> Result<*mut rte_mempool> {
    let name = CString::new(name)?;

    let mut mbp_priv_uninit: MaybeUninit<rte_pktmbuf_pool_private> = MaybeUninit::zeroed();
    unsafe {
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_data_room_size = 0;
        (*mbp_priv_uninit.as_mut_ptr()).mbuf_priv_size = MBUF_PRIV_SIZE as _;
    }

    // actual data size is empty (just mbuf and priv data)
    let elt_size: u32 = size_of::<rte_mbuf>() as u32 + MBUF_PRIV_SIZE as u32;

    tracing::debug!(elt_size, "Trying to init extbuf mempool with elt_size");
    let mbuf_pool = unsafe {
        rte_mempool_create_empty(
            name.as_ptr(),
            DEFAULT_MBUFS_PER_MEMPOOL as u32,
            elt_size,
            MBUF_CACHE_SIZE.into(),
            MBUF_PRIV_SIZE as u32,
            rte_socket_id() as i32,
            0,
        )
    };
    if mbuf_pool.is_null() {
        bail!("Mempool created with rte_mempool_create_empty is null.");
    }

    unsafe {
        // TODO: some of these functions can internally call unsafe; do not need to be called as unsafe
        register_custom_extbuf_ops();
        set_custom_extbuf_ops(mbuf_pool);
        rte_pktmbuf_pool_init(mbuf_pool, mbp_priv_uninit.as_mut_ptr() as _);

        if rte_mempool_populate_default(mbuf_pool) != DEFAULT_MBUFS_PER_MEMPOOL as i32 {
            rte_mempool_free(mbuf_pool);
            bail!("Not able to initialize extbuf mempool: Failed on rte_mempool_populate_default.");
        }

        // initialize each mbuf
        if rte_mempool_obj_iter(mbuf_pool, Some(rte_pktmbuf_init), ptr::null_mut())
            != DEFAULT_MBUFS_PER_MEMPOOL as u32
        {
            rte_mempool_free(mbuf_pool);
            bail!("Not able to initialize each mbuf in extbuf pool");
        }

        // initialize private data
        if rte_mempool_obj_iter(mbuf_pool, Some(custom_init_priv()), ptr::null_mut())
            != DEFAULT_MBUFS_PER_MEMPOOL as u32
        {
            rte_mempool_free(mbuf_pool);
            bail!(
                "Not able to initialize private data in extbuf pool: failed on custom_init_priv."
            );
        }
    }

    Ok(mbuf_pool)
}

pub fn create_mempool(
    name: &str,
    nb_ports: u16,
    data_size: usize,
    num_values: usize,
) -> Result<*mut rte_mempool> {
    let name_str = CString::new(name)?;

    let mbuf_pool = unsafe {
        rte_pktmbuf_pool_create(
            name_str.as_ptr(),
            (num_values as u16 * nb_ports) as u32,
            MBUF_CACHE_SIZE as u32,
            MBUF_PRIV_SIZE as u16,
            data_size as u16,
            rte_socket_id() as i32,
        )
    };
    if mbuf_pool.is_null() {
        tracing::warn!(error =? print_error(), "mbuf pool is null.");
    }
    ensure!(!mbuf_pool.is_null(), "mbuf pool null");

    // initialize private data
    if unsafe { rte_mempool_obj_iter(mbuf_pool, Some(custom_init_priv()), ptr::null_mut()) }
        != (num_values as u16 * nb_ports) as u32
    {
        unsafe {
            rte_mempool_free(mbuf_pool);
        }
        bail!("Not able to initialize private data in extbuf pool: failed on custom_init_priv.");
    }

    Ok(mbuf_pool)
}

fn print_error() -> String {
    let errno = unsafe { rte_errno() };
    let c_buf = unsafe { rte_strerror(errno) };
    let c_str: &CStr = unsafe { CStr::from_ptr(c_buf) };
    let str_slice: &str = c_str.to_str().unwrap();
    format!("Error {}: {:?}", errno, str_slice)
}
