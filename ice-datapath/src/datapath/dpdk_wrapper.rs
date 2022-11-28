use super::{super::dpdk_bindings::*, dpdk_check};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use std::{
    ffi::{CStr, CString},
    fs::read_to_string,
    mem::{size_of, MaybeUninit},
    path::Path,
    ptr,
    time::Duration,
};
use yaml_rust::{Yaml, YamlLoader};

/// DPDK memory pool parameters
const DEFAULT_MBUFS_PER_MEMPOOL: u16 = 8191;
const MBUF_CACHE_SIZE: u16 = 250;
const DEFAULT_MBUF_BUF_SIZE: u32 = RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM;
const MBUF_PRIV_SIZE: usize = 8;

pub fn parse_eal_init(config_path: &str) -> Result<Vec<String>> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let mut args = vec![];
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
                args.push(entry.as_str().unwrap().to_string());
            }
        }

        None => {
            bail!("Yaml config file has no entry dpdk");
        }
    }
    Ok(args)
}

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
