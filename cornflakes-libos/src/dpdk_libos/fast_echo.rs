use super::super::{dpdk_bindings::*, dpdk_call, mbuf_slice, mem, timing::HistogramWrapper};
use super::{dpdk_utils, wrapper};
use bytes::{ByteOrder, LittleEndian};
use color_eyre::eyre::{bail, Result, WrapErr};
use eui48::MacAddress;
use std::{
    ffi::CString,
    io::Write,
    mem::{zeroed, MaybeUninit},
    net::Ipv4Addr,
    ptr, slice,
    str::FromStr,
    time::Instant,
};

const RECEIVE_BURST_SIZE: u32 = 32;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum MemoryMode {
    DPDK,
    EXTERNAL,
}

impl FromStr for MemoryMode {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "DPDK" => MemoryMode::DPDK,
            "EXTERNAL" => MemoryMode::EXTERNAL,
            x => {
                bail!("Unknown memory mode: {}", x);
            }
        })
    }
}

fn get_ether_addr(mac: &MacAddress) -> MaybeUninit<rte_ether_addr> {
    let eth_array = mac.to_array();
    let mut server_eth_uninit: MaybeUninit<rte_ether_addr> = MaybeUninit::zeroed();
    unsafe {
        for i in 0..eth_array.len() {
            (*server_eth_uninit.as_mut_ptr()).addr_bytes[i] = eth_array[i];
        }
    }
    server_eth_uninit
}

pub fn do_client(
    rate: u64,
    total_time: u64,
    size: usize,
    config_path: &str,
    server_ip: &Ipv4Addr,
) -> Result<()> {
    let mut histogram = HistogramWrapper::new("RTTs")?;
    let (ip_to_mac, mac_to_ip, udp_port) = dpdk_utils::parse_yaml_map(config_path).wrap_err(
        "Failed to get ip to mac address mapping, or udp port information from yaml config.",
    )?;
    let (mbuf_pool, _extbuf_mempool, nb_ports) = wrapper::dpdk_init(config_path)?;
    let port = nb_ports - 1;
    let nb_iter = dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(custom_init()),
        ptr::null_mut()
    ));
    assert!(nb_iter == (wrapper::NUM_MBUFS * nb_ports) as u32);

    let clock_offset = Instant::now();
    let start_time = dpdk_call!(rte_get_timer_cycles());
    let mut outstanding = 0;
    let mut sent = 0;

    let cycle_wait: u64 = dpdk_call!(rte_get_timer_hz()) / rate;

    // what is my ethernet address (rte_ether_addr struct)
    let mut my_eth = wrapper::get_my_macaddr(port)?;
    let my_mac = MacAddress::from_bytes(&my_eth.addr_bytes)?;

    // what is their ethernet_addr (should be an rte_ether_addr struct)
    let server_eth_addr_uninit = get_ether_addr(ip_to_mac.get(server_ip).unwrap());
    let mut server_eth = unsafe { server_eth_addr_uninit.assume_init() };

    // what is my IpAddr
    let my_ip_addr = mac_to_ip.get(&my_mac).unwrap();
    let octets = my_ip_addr.octets();
    let my_ip: u32 = dpdk_call!(make_ip(octets[0], octets[1], octets[2], octets[3]));

    // what is their IpAddr
    let octets = server_ip.octets();
    let server_ip: u32 = dpdk_call!(make_ip(octets[0], octets[1], octets[2], octets[3]));

    let mut last_sent_rs = Instant::now();
    while dpdk_call!(rte_get_timer_cycles())
        < (start_time + total_time * dpdk_call!(rte_get_timer_hz()))
    {
        // send a packet
        let mut pkt = wrapper::alloc_mbuf(mbuf_pool)?;

        // fill in packet header
        let header_size = dpdk_call!(fill_in_packet_header(
            pkt,
            &mut my_eth as _,
            &mut server_eth as _,
            my_ip,
            server_ip,
            udp_port,
            size,
        ));

        // fill in packet metadata
        unsafe {
            (*pkt).pkt_len = (size + header_size) as u32;
            (*pkt).data_len = (size + header_size) as u16;
            (*pkt).nb_segs = 1;
        }

        // Write in timestamp as u64 payload
        let send_time = clock_offset.elapsed().as_nanos() as u64;
        let mut mbuf_buffer = mbuf_slice!(pkt, header_size, 8);
        tracing::debug!(len = mbuf_buffer.len(), "mbf buffer length");
        LittleEndian::write_u64(&mut mbuf_buffer, send_time);

        wrapper::tx_burst(port, 0, &mut pkt as _, 1)
            .wrap_err(format!("Failed to send packet, sent = {}", sent))?;
        tracing::debug!("Burst a packet\n");
        sent += 1;
        outstanding += 1;
        tracing::debug!(cycle_wait = cycle_wait, hz =  dpdk_call!(rte_get_timer_hz()), last_sent = ?last_sent_rs.elapsed(), send_time = send_time, "Calling txburst");
        let last_sent = dpdk_call!(rte_get_timer_cycles());
        last_sent_rs = Instant::now();
        let mut rx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] = unsafe { zeroed() };

        /* poll */
        let mut nb_rx;
        while outstanding > 0 {
            nb_rx = dpdk_call!(rte_eth_rx_burst(
                port,
                0,
                rx_bufs.as_mut_ptr(),
                RECEIVE_BURST_SIZE as u16,
            ));
            if nb_rx == 0 {
                if dpdk_call!(rte_get_timer_cycles() > (last_sent + cycle_wait)) {
                    break;
                }
                continue;
            }
            for i in 0..nb_rx {
                let rx_buf = rx_bufs[i as usize];
                let (valid, _payload_length) =
                    dpdk_call!(parse_packet(rx_buf, &mut my_eth as _, my_ip));
                if valid {
                    let now = clock_offset.elapsed().as_nanos() as u64;
                    let mbuf_buffer = mbuf_slice!(rx_buf, header_size, 8);
                    let start = LittleEndian::read_u64(&mbuf_buffer);
                    histogram.record(now - start)?;
                    wrapper::free_mbuf(rx_buf);
                    outstanding -= 1;
                } else {
                    wrapper::free_mbuf(rx_buf);
                }
            }
        }
        while (last_sent + cycle_wait) >= dpdk_call!(rte_get_timer_cycles()) {
            continue;
        }
    }

    histogram.dump_stats();
    Ok(())
}

pub fn do_server(
    config_path: &str,
    _zero_copy: bool,
    memory_mode: MemoryMode,
    num_mbufs: usize,
) -> Result<()> {
    let (_ip_to_mac, mac_to_ip, _udp_port) = dpdk_utils::parse_yaml_map(config_path).wrap_err(
        "Failed to get ip to mac address mapping, or udp port information from yaml config.",
    )?;
    let (mbuf_pool, extbuf_mempool, nb_ports) = wrapper::dpdk_init(config_path)?;
    let port = nb_ports - 1;

    // what is my ethernet address (rte_ether_addr struct)
    let mut my_eth = wrapper::get_my_macaddr(port)?;
    let my_mac = MacAddress::from_bytes(&my_eth.addr_bytes)?;
    // what is my IpAddr
    let my_ip_addr = mac_to_ip.get(&my_mac).unwrap();
    let octets = my_ip_addr.octets();
    let my_ip: u32 = dpdk_call!(make_ip(octets[0], octets[1], octets[2], octets[3]));

    let nb_iter = dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(custom_init()),
        ptr::null_mut()
    ));
    assert!(nb_iter == (wrapper::NUM_MBUFS * nb_ports) as u32);
    let mut header_mbuf_pool: *mut rte_mempool = ptr::null_mut();

    // if using 2 mbufs, make the header_mbuf_pool non_null
    if num_mbufs == 2 && memory_mode == MemoryMode::DPDK {
        let name = CString::new("header_mbuf_pool")?;
        header_mbuf_pool = dpdk_call!(rte_pktmbuf_pool_create(
            name.as_ptr(),
            (wrapper::NUM_MBUFS * 1) as u32,
            wrapper::MBUF_CACHE_SIZE as u32,
            0,
            wrapper::MBUF_BUF_SIZE as u16,
            rte_socket_id() as i32
        ));
        assert!(!header_mbuf_pool.is_null());
        let nb_iter = dpdk_call!(rte_mempool_obj_iter(
            header_mbuf_pool,
            Some(custom_init()),
            ptr::null_mut()
        ));
        assert!(nb_iter == (wrapper::NUM_MBUFS * nb_ports) as u32);
    }

    // if using external memory, initialize all the external memory related things, register
    // external memory and get a pointer to the external memory address
    // write bytes into external memory payload, initialize shinfo
    let mut shared_info_uninit: MaybeUninit<rte_mbuf_ext_shared_info> = MaybeUninit::zeroed();
    unsafe {
        (*shared_info_uninit.as_mut_ptr()).refcnt = 1;
        (*shared_info_uninit.as_mut_ptr()).fcb_opaque = ptr::null_mut();
        (*shared_info_uninit.as_mut_ptr()).free_cb = Some(general_free_cb_);
    }
    let shinfo = shared_info_uninit.as_mut_ptr();
    let (metadata, mut mmap) = mem::mmap_new(100)?;
    wrapper::dpdk_register_extmem(&metadata)?;
    let payload = vec![b'a'; 10000];
    (&mut mmap[..]).write_all(payload.as_ref())?;

    // now, can loop on packet arrival
    let mut rx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] = unsafe { zeroed() };
    let mut tx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] = unsafe { zeroed() };
    let mut secondary_tx_bufs: [*mut rte_mbuf; RECEIVE_BURST_SIZE as usize] = unsafe { zeroed() };

    loop {
        let num_received = dpdk_call!(rte_eth_rx_burst(
            port,
            0,
            rx_bufs.as_mut_ptr(),
            RECEIVE_BURST_SIZE as u16
        ));
        let mut num_valid = 0;
        for i in 0..num_received {
            let n_to_tx = i as usize;
            // first: parse if valid packet, and what the payload size is
            let (is_valid, payload_length) =
                dpdk_call!(parse_packet(rx_bufs[n_to_tx], &mut my_eth as _, my_ip));
            if !is_valid {
                wrapper::free_mbuf(rx_bufs[n_to_tx]);
                continue;
            }
            num_valid += 1;
            let header_size = unsafe { (*rx_bufs[n_to_tx]).pkt_len - payload_length as u32 };
            // first: allocate header mbuf, and if necessary, secondary mbuf
            match memory_mode {
                MemoryMode::DPDK => {
                    if num_mbufs == 2 {
                        // allocate header mbuf
                        tx_bufs[n_to_tx] = wrapper::alloc_mbuf(header_mbuf_pool)?;
                        secondary_tx_bufs[n_to_tx] = wrapper::alloc_mbuf(mbuf_pool)?;
                    } else {
                        // allocate a single mbuf
                        tx_bufs[n_to_tx] = wrapper::alloc_mbuf(mbuf_pool)?;
                    }
                }
                MemoryMode::EXTERNAL => {
                    if num_mbufs == 2 {
                        // allocate header from normal pool
                        tx_bufs[n_to_tx] = wrapper::alloc_mbuf(mbuf_pool)?;
                        secondary_tx_bufs[n_to_tx] = wrapper::alloc_mbuf(extbuf_mempool)?;
                        dpdk_call!(rte_pktmbuf_attach_extbuf(
                            secondary_tx_bufs[n_to_tx],
                            metadata.ptr as _,
                            0,
                            payload_length as u16,
                            shinfo
                        ));
                    } else {
                        tx_bufs[n_to_tx] = wrapper::alloc_mbuf(mbuf_pool)?;
                        dpdk_call!(rte_pktmbuf_attach_extbuf(
                            tx_bufs[n_to_tx],
                            metadata.ptr as _,
                            0,
                            payload_length as u16 + header_size as u16,
                            shinfo,
                        ));
                    }
                }
            }
            let tx_buf = tx_bufs[n_to_tx];
            let rx_buf = rx_bufs[n_to_tx];
            let secondary_tx = secondary_tx_bufs[n_to_tx];
            // switch the headers between the received mbuf and the header mbuf
            dpdk_call!(switch_headers(rx_buf, tx_buf, payload_length));

            // add back in client's timestamp
            let rx_slice = mbuf_slice!(rx_buf, header_size, 8);
            let mut tx_slice = match num_mbufs == 1 {
                true => {
                    mbuf_slice!(tx_buf, header_size, 8)
                }
                false => {
                    assert!(num_mbufs == 2);
                    mbuf_slice!(secondary_tx, 0, 8)
                }
            };
            LittleEndian::write_u64(&mut tx_slice, LittleEndian::read_u64(&rx_slice));

            if num_mbufs == 2 {
                unsafe {
                    (*tx_buf).next = secondary_tx;
                    (*tx_buf).data_len = ((*rx_buf).pkt_len - payload_length as u32) as u16;
                    (*tx_buf).pkt_len = (*rx_buf).pkt_len;
                    (*secondary_tx).data_len = payload_length as u16;
                    (*tx_buf).nb_segs = 2;
                }
            } else {
                unsafe {
                    (*tx_buf).pkt_len = (*rx_buf).pkt_len;
                    (*tx_buf).data_len = (*rx_buf).data_len;
                    (*tx_buf).next = ptr::null_mut();
                    (*tx_buf).nb_segs = 1;
                }
            }
            // free the rx packet
            dpdk_call!(rte_pktmbuf_free(rx_bufs[n_to_tx]));
        }

        // burst all the packets
        if num_valid > 0 {
            wrapper::tx_burst(port, 0, tx_bufs.as_mut_ptr(), num_valid)?;
        }
    }
}
