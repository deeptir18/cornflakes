use super::MsgID;
use byteorder::{ByteOrder, NetworkEndian};
use color_eyre::eyre::{bail, Result};
use eui48::MacAddress;
use std::convert::TryInto;
use std::net::Ipv4Addr;

// Header setting taken from Demikernel's catnip OS:
// https://github.com/demikernel/demikernel/blob/master/src/rust/catnip/src/protocols/
pub const ETHERNET2_HEADER2_SIZE: usize = 14;
pub const IPV4_HEADER2_SIZE: usize = 20;
pub const UDP_HEADER2_SIZE: usize = 8;
pub const DEFAULT_IPV4_TTL: u8 = 64;
pub const IPV4_IHL_NO_OPTIONS: u8 = 5;
pub const IPV4_VERSION: u8 = 4;
pub const IPDEFTTL: u8 = 64;
pub const IPPROTO_UDP: u8 = 17;
pub const ETHERTYPE_IPV4: u16 = 0x800 as u16;
pub const TOTAL_HEADER_SIZE: usize =
    ETHERNET2_HEADER2_SIZE + IPV4_HEADER2_SIZE + UDP_HEADER2_SIZE + 4;

#[derive(Debug)]
pub enum TraceLevel {
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl std::str::FromStr for TraceLevel {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "debug" => TraceLevel::Debug,
            "info" => TraceLevel::Info,
            "warn" => TraceLevel::Warn,
            "error" => TraceLevel::Error,
            "off" => TraceLevel::Off,
            x => bail!("unknown TRACE level {:?}", x),
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HeaderInfo {
    pub udp_src_port: u16,
    pub udp_dst_port: u16,
    pub ipv4_src_addr: Ipv4Addr,
    pub ipv4_dst_addr: Ipv4Addr,
    pub ether_src_addr: MacAddress,
    pub ether_dst_addr: MacAddress,
}

impl HeaderInfo {
    pub fn new(udp_port: u16, src_ip: &Ipv4Addr, src_mac: &MacAddress) -> HeaderInfo {
        HeaderInfo {
            udp_src_port: udp_port,
            udp_dst_port: udp_port,
            ipv4_src_addr: *src_ip,
            ipv4_dst_addr: Ipv4Addr::LOCALHOST,
            ether_src_addr: *src_mac,
            ether_dst_addr: MacAddress::nil(),
        }
    }

    pub fn reverse(&self) -> HeaderInfo {
        HeaderInfo {
            udp_src_port: self.udp_dst_port,
            udp_dst_port: self.udp_src_port,
            ipv4_src_addr: self.ipv4_dst_addr,
            ipv4_dst_addr: self.ipv4_src_addr,
            ether_src_addr: self.ether_dst_addr,
            ether_dst_addr: self.ether_src_addr,
        }
    }

    pub fn get_outgoing(&self, dst_ip: Ipv4Addr, dst_mac: MacAddress) -> HeaderInfo {
        let mut ret = self.clone();
        ret.ipv4_dst_addr = dst_ip;
        ret.ether_dst_addr = dst_mac;
        ret
    }
}

#[inline]
pub fn write_udp_hdr(header_info: &HeaderInfo, buf: &mut [u8], data_len: usize) -> Result<()> {
    let fixed_buf: &mut [u8; UDP_HEADER2_SIZE] = (&mut buf[..UDP_HEADER2_SIZE]).try_into().unwrap();
    NetworkEndian::write_u16(&mut fixed_buf[0..2], header_info.udp_src_port);
    NetworkEndian::write_u16(&mut fixed_buf[2..4], header_info.udp_dst_port);
    NetworkEndian::write_u16(&mut fixed_buf[4..6], (UDP_HEADER2_SIZE + data_len) as u16);
    // no checksum
    NetworkEndian::write_u16(&mut fixed_buf[6..8], 0);
    Ok(())
}

#[inline]
fn ipv4_checksum(buf: &[u8]) -> u16 {
    let buf: &[u8; IPV4_HEADER2_SIZE] = buf.try_into().expect("Invalid header size");
    let mut state = 0xffffu32;
    for i in 0..5 {
        state += NetworkEndian::read_u16(&buf[(2 * i)..(2 * i + 2)]) as u32;
    }
    // Skip the 5th u16 since octets 10-12 are the header checksum, whose value should be zero when
    // computing a checksum.
    for i in 6..10 {
        state += NetworkEndian::read_u16(&buf[(2 * i)..(2 * i + 2)]) as u32;
    }
    while state > 0xffff {
        state -= 0xffff;
    }
    !state as u16
}

#[inline]
pub fn write_ipv4_hdr(header_info: &HeaderInfo, buf: &mut [u8], data_len: usize) -> Result<()> {
    // currently, this only sets some fields
    let buf: &mut [u8; IPV4_HEADER2_SIZE] = buf.try_into()?;
    buf[0] = (IPV4_VERSION << 4) | IPV4_IHL_NO_OPTIONS; // version IHL
    NetworkEndian::write_u16(&mut buf[2..4], (IPV4_HEADER2_SIZE + data_len) as u16); // payload size
    buf[8] = IPDEFTTL; // time to live
    buf[9] = IPPROTO_UDP; // next_proto_id

    buf[12..16].copy_from_slice(&header_info.ipv4_src_addr.octets());
    buf[16..20].copy_from_slice(&header_info.ipv4_dst_addr.octets());

    let checksum = ipv4_checksum(buf);
    NetworkEndian::write_u16(&mut buf[10..12], checksum);
    Ok(())
}

#[inline]
pub fn write_eth_hdr(header_info: &HeaderInfo, buf: &mut [u8]) -> Result<()> {
    let buf: &mut [u8; ETHERNET2_HEADER2_SIZE] = buf.try_into()?;
    buf[0..6].copy_from_slice(header_info.ether_dst_addr.as_bytes());
    buf[6..12].copy_from_slice(header_info.ether_src_addr.as_bytes());
    NetworkEndian::write_u16(&mut buf[12..14], ETHERTYPE_IPV4);
    Ok(())
}

#[inline]
pub fn write_pkt_id(id: MsgID, buf: &mut [u8]) -> Result<()> {
    let buf: &mut [u8; 4] = buf.try_into()?;
    NetworkEndian::write_u32(&mut buf[0..4], id);
    Ok(())
}
