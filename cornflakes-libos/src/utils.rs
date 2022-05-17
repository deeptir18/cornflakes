use super::MsgID;
use byteorder::{ByteOrder, LittleEndian, NetworkEndian};
use color_eyre::eyre::{bail, Result};
use eui48::MacAddress;
use std::convert::{TryFrom, TryInto};
use std::net::Ipv4Addr;
use tracing::debug;

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
pub const HEADER_PADDING_SIZE: usize = 0;
pub const HEADER_ID_SIZE: usize = 4;
pub const TOTAL_UDP_HEADER_SIZE: usize =
    ETHERNET2_HEADER2_SIZE + IPV4_HEADER2_SIZE + UDP_HEADER2_SIZE;
pub const TOTAL_HEADER_SIZE: usize = TOTAL_UDP_HEADER_SIZE + HEADER_ID_SIZE + HEADER_PADDING_SIZE;

#[repr(u16)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum EtherType2 {
    Arp = 0x806,
    Ipv4 = 0x800,
}

impl TryFrom<u16> for EtherType2 {
    type Error = color_eyre::eyre::Error;

    fn try_from(n: u16) -> Result<Self> {
        if n == EtherType2::Arp as u16 {
            Ok(EtherType2::Arp)
        } else if n == EtherType2::Ipv4 as u16 {
            Ok(EtherType2::Ipv4)
        } else {
            bail!("Unsupported ether type: {}", n);
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash)]
pub struct AddressInfo {
    pub udp_port: u16,
    pub ipv4_addr: Ipv4Addr,
    pub ether_addr: MacAddress,
}

impl Default for AddressInfo {
    fn default() -> AddressInfo {
        AddressInfo {
            udp_port: 12345,
            ipv4_addr: Ipv4Addr::LOCALHOST,
            ether_addr: MacAddress::default(),
        }
    }
}

impl AddressInfo {
    pub fn new(port: u16, ipv4: Ipv4Addr, mac: MacAddress) -> AddressInfo {
        AddressInfo {
            udp_port: port,
            ipv4_addr: ipv4,
            ether_addr: mac,
        }
    }

    pub fn get_outgoing(&self, dst_addr: &AddressInfo) -> HeaderInfo {
        HeaderInfo::new(self.clone(), dst_addr.clone())
    }
}

#[derive(Debug, PartialEq, Clone, Default, Copy)]
pub struct HeaderInfo {
    pub src_info: AddressInfo,
    pub dst_info: AddressInfo,
}

impl HeaderInfo {
    pub fn new(src_info: AddressInfo, dst_info: AddressInfo) -> HeaderInfo {
        HeaderInfo {
            src_info: src_info,
            dst_info: dst_info,
        }
    }
}

#[inline]
pub fn write_udp_hdr(header_info: &HeaderInfo, buf: &mut [u8], data_len: usize) -> Result<()> {
    let fixed_buf: &mut [u8; UDP_HEADER2_SIZE] = (&mut buf[..UDP_HEADER2_SIZE]).try_into()?;
    NetworkEndian::write_u16(&mut fixed_buf[0..2], header_info.src_info.udp_port);
    NetworkEndian::write_u16(&mut fixed_buf[2..4], header_info.dst_info.udp_port);
    NetworkEndian::write_u16(&mut fixed_buf[4..6], (UDP_HEADER2_SIZE + data_len) as u16);
    // no checksum
    NetworkEndian::write_u16(&mut fixed_buf[6..8], 0);
    Ok(())
}

#[inline]
fn ipv4_checksum(buf: &[u8]) -> Result<u16> {
    let buf: &[u8; IPV4_HEADER2_SIZE] = buf.try_into()?;
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
    Ok(!state as u16)
}

#[inline]
pub fn write_ipv4_hdr(header_info: &HeaderInfo, buf: &mut [u8], data_len: usize) -> Result<()> {
    // currently, this only sets some fields
    let buf: &mut [u8; IPV4_HEADER2_SIZE] = buf.try_into()?;
    buf[0] = (IPV4_VERSION << 4) | IPV4_IHL_NO_OPTIONS; // version IHL
    NetworkEndian::write_u16(&mut buf[2..4], (IPV4_HEADER2_SIZE + data_len) as u16); // payload size
    buf[8] = IPDEFTTL; // time to live
    buf[9] = IPPROTO_UDP; // next_proto_id

    buf[12..16].copy_from_slice(&header_info.src_info.ipv4_addr.octets());
    buf[16..20].copy_from_slice(&header_info.dst_info.ipv4_addr.octets());

    let checksum = ipv4_checksum(buf)?;
    NetworkEndian::write_u16(&mut buf[10..12], checksum);
    Ok(())
}

#[inline]
pub fn write_eth_hdr(header_info: &HeaderInfo, buf: &mut [u8]) -> Result<()> {
    //let buf: &mut [u8; ETHERNET2_HEADER2_SIZE] = buf.try_into()?;
    buf[0..6].copy_from_slice(header_info.dst_info.ether_addr.as_bytes());
    buf[6..12].copy_from_slice(header_info.src_info.ether_addr.as_bytes());
    NetworkEndian::write_u16(&mut buf[12..14], EtherType2::Ipv4 as u16);
    Ok(())
}

#[inline]
pub fn write_pkt_id(id: MsgID, buf: &mut [u8]) -> Result<()> {
    let buf: &mut [u8; 4] = buf.try_into()?;
    LittleEndian::write_u32(&mut buf[0..4], id);
    Ok(())
}

#[inline]
pub fn check_eth_hdr(hdr_buf: &[u8], my_ether: &MacAddress) -> Result<(MacAddress, MacAddress)> {
    let dst_addr = MacAddress::from_bytes(&hdr_buf[0..6])?;
    let src_addr = MacAddress::from_bytes(&hdr_buf[6..12])?;
    let ether_type = EtherType2::try_from(NetworkEndian::read_u16(&hdr_buf[12..14]))?;
    if (dst_addr != *my_ether) && (dst_addr != MacAddress::broadcast()) {
        debug!(
            "(recv: dropped) Destination ether addr: {:?}, does not match mine {:?}, and is not broadcast.",
            dst_addr, my_ether
        );
        bail!("Destination ether address does not match mine and is not broadcast.");
    }

    if ether_type != EtherType2::Ipv4 {
        bail!("Not correct ether type.");
    }

    Ok((src_addr, dst_addr))
}

#[inline]
pub fn check_ipv4_hdr(hdr_buf: &[u8], my_ip: &Ipv4Addr) -> Result<(Ipv4Addr, Ipv4Addr)> {
    let src_addr = Ipv4Addr::from(NetworkEndian::read_u32(&hdr_buf[12..16]));
    let dst_addr = Ipv4Addr::from(NetworkEndian::read_u32(&hdr_buf[16..20]));
    if hdr_buf[9] != IPPROTO_UDP {
        debug!("(recv: dropped)  ipv4 hdr does not have IPPROTO_UDP.");
        bail!("(recv: dropped)  ipv4 hdr does not have IPPROTO_UDP.");
    }

    if dst_addr != *my_ip {
        debug!(
            "(recv: dropped) Dest ipv4 addr: {:?} does not match mine {:?}",
            dst_addr, my_ip
        );
        bail!(
            "Dest ipv4 addr: {:?} does not match mine {:?}",
            dst_addr,
            my_ip
        );
    }

    Ok((src_addr, dst_addr))
}

#[inline]
pub fn check_udp_hdr(hdr_buf: &[u8], my_udp_port: u16) -> Result<(u16, u16, usize)> {
    let src_port = NetworkEndian::read_u16(&hdr_buf[0..2]);
    let dst_port = NetworkEndian::read_u16(&hdr_buf[2..4]);
    let data_len = NetworkEndian::read_u16(&hdr_buf[4..6]) as usize;

    if dst_port != my_udp_port {
        debug!(
            "recv dropped) Dst udp port {} does not match ours {}.",
            dst_port, my_udp_port
        );
        bail!(
            "recv dropped) Dst udp port {} does not match ours {}.",
            dst_port,
            my_udp_port
        );
    }

    tracing::debug!("data length recorded in packet udp header: {}", data_len);
    Ok((
        src_port,
        dst_port,
        data_len - UDP_HEADER2_SIZE - HEADER_ID_SIZE,
    ))
}

#[inline]
pub fn parse_msg_id(hdr_buf: &[u8]) -> MsgID {
    LittleEndian::read_u32(&hdr_buf[0..4])
}
