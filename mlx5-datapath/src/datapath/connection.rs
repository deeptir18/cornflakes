use super::{
    super::{access, check_ok, mbuf_slice, mlx5_bindings::*},
    allocator::MemoryAllocator,
    check, sizes,
};
use cornflakes_libos::{
    datapath::{Datapath, MetadataOps, ReceivedPkt},
    mem::{PGSIZE_2MB, PGSIZE_4KB},
    serialize::Serializable,
    utils::AddressInfo,
    ConnID, MsgID, RcSga, Sga, USING_REF_COUNTING,
};

use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_utils::{parse_yaml_map, AppMode};
use eui48::MacAddress;
use hashbrown::HashMap;
use std::{
    ffi::CString,
    fs::read_to_string,
    io::Write,
    mem::MaybeUninit,
    net::Ipv4Addr,
    path::Path,
    ptr,
    time::{Duration, Instant},
};
use yaml_rust::{Yaml, YamlLoader};

#[cfg(feature = "profiler")]
use perftools;

const MAX_CONCURRENT_CONNECTIONS: usize = 32;

#[derive(PartialEq, Eq)]
pub struct Mlx5Buffer {
    /// Underlying data pointer
    data: *mut ::std::os::raw::c_void,
    /// Pointer back to the data and metadata pool pair
    mempool: *mut registered_mempool,
    /// Data len,
    data_len: usize,
}

impl Mlx5Buffer {
    pub fn new(
        data: *mut ::std::os::raw::c_void,
        mempool: *mut registered_mempool,
        data_len: usize,
    ) -> Self {
        Mlx5Buffer {
            data: data,
            mempool: mempool,
            data_len: data_len,
        }
    }

    pub fn get_inner(self) -> (*mut ::std::os::raw::c_void, *mut registered_mempool, usize) {
        (self.data, self.mempool, self.data_len)
    }

    pub fn get_mempool(&self) -> *mut registered_mempool {
        self.mempool
    }
}

impl std::fmt::Debug for Mlx5Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, data_len: {}", self.data, self.data_len)
    }
}
impl AsRef<[u8]> for Mlx5Buffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data as *mut u8, self.data_len) }
    }
}

impl AsMut<[u8]> for Mlx5Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data as *mut u8, self.data_len) }
    }
}

impl Write for Mlx5Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // only write the maximum amount
        let data_mempool = unsafe { get_data_mempool(self.mempool) };
        let written = std::cmp::min(unsafe { access!(data_mempool, item_len, usize) }, buf.len());
        let written = self.as_mut().write(&buf[0..written])?;
        self.data_len = written;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// MbufMetadata struct wraps around mbuf data structure.
/// Points to metadata object, which further contains a pointer to:
/// (a) Actual data (which could be allocated separately from mbuf metadata)
/// (b) Another mbuf metadata.
#[derive(PartialEq, Eq)]
pub struct MbufMetadata {
    /// Pointer to allocated mbuf metadata.
    pub mbuf: *mut mbuf,
    /// Application data offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
}

impl MbufMetadata {
    pub fn from_buf(mlx5_buffer: Mlx5Buffer) -> Result<Option<Self>> {
        let (buf, registered_mempool, data_len) = mlx5_buffer.get_inner();
        let metadata_buf = unsafe { alloc_metadata(registered_mempool, buf) };
        if metadata_buf.is_null() {
            // drop the data buffer
            unsafe {
                mempool_free(buf, get_data_mempool(registered_mempool));
            }
            return Ok(None);
        }

        ensure!(!metadata_buf.is_null(), "Allocated metadata buffer is null");
        unsafe {
            init_metadata(
                metadata_buf,
                buf,
                get_data_mempool(registered_mempool),
                get_metadata_mempool(registered_mempool),
                data_len,
                0,
            );
            mbuf_refcnt_update_or_free(metadata_buf, 1);
        }
        Ok(Some(MbufMetadata::new(metadata_buf, 0, Some(data_len))))
    }
    /// Initializes metadata object from existing metadata mbuf in c.
    /// Args:
    /// @mbuf: mbuf structure that contains metadata
    /// @data_offset: Application data offset into this buffer.
    /// @data_len: Optional application data length into the buffer.
    pub fn new(mbuf: *mut mbuf, data_offset: usize, data_len: Option<usize>) -> Self {
        unsafe {
            mbuf_refcnt_update_or_free(mbuf, 1);
        }
        let len = match data_len {
            Some(x) => x,
            None => unsafe { (*mbuf).data_len as usize },
        };
        MbufMetadata {
            mbuf: mbuf,
            offset: data_offset,
            len: len,
        }
    }
}

impl MetadataOps for MbufMetadata {
    fn set_offset(&mut self, off: usize) -> Result<()> {
        ensure!(
            off < unsafe { access!(self.mbuf, data_buf_len, usize) },
            "Offset too large"
        );
        self.offset = off;
        Ok(())
    }

    fn set_data_len(&mut self, data_len: usize) -> Result<()> {
        ensure!(
            data_len <= unsafe { access!(self.mbuf, data_buf_len, usize) },
            "Data_len too large"
        );
        self.len = data_len;
        Ok(())
    }
}

// TODO: might be safest not to have a default function
impl Default for MbufMetadata {
    fn default() -> Self {
        MbufMetadata {
            mbuf: ptr::null_mut(),
            offset: 0,
            len: 0,
        }
    }
}

impl Drop for MbufMetadata {
    fn drop(&mut self) {
        unsafe {
            if USING_REF_COUNTING {
                mbuf_refcnt_update_or_free(self.mbuf, -1);
            } else {
                mbuf_free(self.mbuf);
            }
        }
    }
}

impl Clone for MbufMetadata {
    fn clone(&self) -> MbufMetadata {
        unsafe {
            if USING_REF_COUNTING {
                mbuf_refcnt_update_or_free(self.mbuf, 1);
            }
        }
        MbufMetadata {
            mbuf: self.mbuf,
            offset: self.offset,
            len: self.len,
        }
    }
}

impl std::fmt::Debug for MbufMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Mbuf addr: {:?}, off: {}", self.mbuf, self.offset)
    }
}

impl AsRef<[u8]> for MbufMetadata {
    fn as_ref(&self) -> &[u8] {
        unsafe { mbuf_slice!(self.mbuf, self.offset, self.len) }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Mlx5PerThreadContext {
    /// Queue id
    queue_id: u16,
    /// Source address info (ethernet, ip, port)
    address_info: AddressInfo,
    /// Pointer to datapath specific thread information
    context: *mut mlx5_per_thread_context,
}

impl Mlx5PerThreadContext {
    pub fn get_context_ptr(&self) -> *mut mlx5_per_thread_context {
        self.context
    }

    pub fn get_queue_id(&self) -> u16 {
        self.queue_id
    }

    pub fn get_address_info(&self) -> &AddressInfo {
        &self.address_info
    }
}

unsafe impl Send for Mlx5PerThreadContext {}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Mlx5GlobalContext {
    /// Pointer to datapath specific global state
    context: *mut mlx5_global_context,
}

impl Mlx5GlobalContext {
    fn get_ptr(&self) -> *mut mlx5_global_context {
        self.context
    }
}

#[derive(Debug, Clone)]
pub struct Mlx5DatapathSpecificParams {
    pci_addr: MaybeUninit<pci_addr>,
    eth_addr: MaybeUninit<eth_addr>,
    ip_to_mac: HashMap<Ipv4Addr, MacAddress>,
    our_ip: Ipv4Addr,
    our_eth: MacAddress,
    client_port: u16,
    server_port: u16,
}

impl Mlx5DatapathSpecificParams {
    pub unsafe fn get_pci_addr(&mut self) -> *mut pci_addr {
        self.pci_addr.as_mut_ptr()
    }

    pub unsafe fn get_eth_addr(&mut self) -> *mut eth_addr {
        self.eth_addr.as_mut_ptr()
    }

    pub fn get_ipv4(&self) -> Ipv4Addr {
        self.our_ip.clone()
    }

    pub fn get_mac(&self) -> MacAddress {
        self.our_eth.clone()
    }

    pub fn get_server_port(&self) -> u16 {
        self.server_port
    }

    pub fn get_ip_to_mac(&self) -> HashMap<Ipv4Addr, MacAddress> {
        self.ip_to_mac.clone()
    }
}

unsafe impl Send for Mlx5GlobalContext {}

pub struct Mlx5Connection {
    /// Per thread context.
    thread_context: Mlx5PerThreadContext,
    /// Scatter-gather mode turned on or off.
    use_scatter_gather: bool,
    /// Server or client mode
    mode: AppMode,
    /// Map of IP address to ethernet addresses loaded at config time.
    ip_to_mac: HashMap<Ipv4Addr, MacAddress>,
    /// Current window of outstanding packets (used for keeping track of RTTs).
    outgoing_window: HashMap<MsgID, Instant>,
    /// Active connections:  current connection IDs mapped to addresses.
    active_connections: [(bool, AddressInfo); MAX_CONCURRENT_CONNECTIONS],
    /// Map from AddressInfo to connection id
    address_to_conn_id: HashMap<AddressInfo, ConnID>,
    /// Allocator for outgoing mbufs and packets.
    allocator: MemoryAllocator,
}

fn parse_pci_addr(config_path: &str) -> Result<String> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };
    let yaml = &yamls[0];
    match yaml["mlx5"].as_hash() {
        Some(map) => match map.get(&Yaml::from_str("pci_addr")) {
            Some(val) => {
                return Ok(val.as_str().unwrap().to_string());
            }
            None => {
                bail!("Yaml mlx5 config has no pci_addr entry");
            }
        },
        None => {
            bail!("Yaml has no mlx5 entry");
        }
    }
}

impl Datapath for Mlx5Connection {
    type DatapathBuffer = Mlx5Buffer;

    type DatapathMetadata = MbufMetadata;

    type PerThreadContext = Mlx5PerThreadContext;

    type GlobalContext = Mlx5GlobalContext;

    type DatapathSpecificParams = Mlx5DatapathSpecificParams;

    fn parse_config_file(
        config_file: &str,
        our_ip: Option<Ipv4Addr>,
    ) -> Result<Self::DatapathSpecificParams> {
        // parse the IP to Mac hashmap
        let (ip_to_mac, _mac_to_ip, server_port, client_port) =
            parse_yaml_map(config_file).wrap_err("Failed to parse yaml map")?;

        let pci_addr =
            parse_pci_addr(config_file).wrap_err("Failed to parse pci addr from config")?;

        // for this datapath, knowing our IP address is required (to find our mac address)
        let (eth_addr, ip) = match our_ip {
            None => {
                bail!("For mlx5 datapath, must pass in ip to parse_config_file to retrieve our ethernet address");
            }
            Some(x) => match ip_to_mac.get(&x) {
                Some(e) => (e.clone(), x.clone()),
                None => {
                    bail!("Could not find eth addr for passed in ipv4 addr {:?} in config_file ip_to_mac map: {:?}", x, ip_to_mac);
                }
            },
        };

        // convert pci addr and eth addr to C structs
        let mut ether_addr: MaybeUninit<eth_addr> = MaybeUninit::zeroed();
        // copy given mac addr into the c struct
        unsafe {
            rte_memcpy(
                ether_addr.as_mut_ptr() as _,
                eth_addr.as_bytes().as_ptr() as _,
                6,
            );
        }

        let pci_str = CString::new(pci_addr.as_str()).expect("CString::new failed");
        let mut pci_addr_c: MaybeUninit<pci_addr> = MaybeUninit::zeroed();
        unsafe {
            rte_memcpy(
                pci_addr_c.as_mut_ptr() as _,
                pci_str.as_ptr() as _,
                pci_addr.len(),
            );
        }
        Ok(Mlx5DatapathSpecificParams {
            pci_addr: pci_addr_c,
            eth_addr: ether_addr,
            ip_to_mac: ip_to_mac,
            our_ip: ip,
            our_eth: eth_addr.clone(),
            client_port: client_port,
            server_port: server_port,
        })
    }

    fn compute_affinity(
        datapath_params: &Self::DatapathSpecificParams,
        num_queues: usize,
        _remote_ip: Ipv4Addr,
        app_mode: AppMode,
    ) -> Result<Vec<AddressInfo>> {
        // TODO: how do we compute affinity for more than one queue for mlx5
        if num_queues > 1 {
            bail!("Currently, mlx5 datapath does not support more than one queue");
        }
        if app_mode != AppMode::Server {
            bail!("Currently, mlx5 datapath does not support anything other than server mode");
        }

        Ok(vec![AddressInfo::new(
            datapath_params.get_server_port(),
            datapath_params.get_ipv4(),
            datapath_params.get_mac(),
        )])
    }

    fn global_init(
        num_queues: usize,
        datapath_params: &mut Self::DatapathSpecificParams,
        addresses: Vec<AddressInfo>,
    ) -> Result<(Self::GlobalContext, Vec<Self::PerThreadContext>)> {
        // do time init
        unsafe {
            time_init();
        }

        ensure!(
            num_queues == addresses.len(),
            format!(
                "AddressInfo vector length {} must be equal to num queues {}",
                addresses.len(),
                num_queues
            )
        );
        // allocate the global context
        let global_context = Mlx5GlobalContext {
            context: unsafe {
                let ptr = alloc_global_context(num_queues as _);
                ensure!(!ptr.is_null(), "Allocated global context is null");

                // initialize ibv context
                check_ok!(init_ibv_context(ptr, datapath_params.get_pci_addr()));

                // initialize and register the rx mempools
                let rx_mempool_params: sizes::MempoolAllocationParams =
                    sizes::MempoolAllocationParams::new(
                        sizes::RX_MEMPOOL_MIN_NUM_ITEMS,
                        sizes::RX_MEMPOOL_METADATA_PGSIZE,
                        sizes::RX_MEMPOOL_DATA_PGSIZE,
                        sizes::RX_MEMPOOL_DATA_LEN,
                    )
                    .wrap_err("Incorrect rx allocation params")?;
                check_ok!(init_rx_mempools(
                    ptr,
                    rx_mempool_params.get_item_len() as _,
                    rx_mempool_params.get_num_items() as _,
                    rx_mempool_params.get_data_pgsize() as _,
                    rx_mempool_params.get_metadata_pgsize() as _,
                    ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _
                ));

                // init queues
                for i in 0..num_queues {
                    let per_thread_context = get_per_thread_context(ptr, i as u64);
                    check_ok!(mlx5_init_rxq(per_thread_context));
                    check_ok!(mlx5_init_txq(per_thread_context));
                }

                // init queue steering
                check_ok!(mlx5_qs_init_flows(ptr, datapath_params.get_eth_addr()));
                ptr
            },
        };
        let per_thread_contexts: Vec<Mlx5PerThreadContext> = addresses
            .into_iter()
            .enumerate()
            .map(|(i, addr)| {
                let context_ptr =
                    unsafe { get_per_thread_context(global_context.get_ptr(), i as u64) };
                Mlx5PerThreadContext {
                    queue_id: i as u16,
                    address_info: addr,
                    context: context_ptr,
                }
            })
            .collect();
        Ok((global_context, per_thread_contexts))
    }

    fn global_teardown(context: Self::GlobalContext) -> Result<()> {
        // free the global context
        unsafe {
            free_global_context(context.get_ptr());
        }
        Ok(())
    }

    fn per_thread_init(
        datapath_params: Self::DatapathSpecificParams,
        context: Self::PerThreadContext,
        mode: AppMode,
        use_scatter_gather: bool,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Mlx5Connection {
            thread_context: context,
            use_scatter_gather: use_scatter_gather,
            mode: mode,
            ip_to_mac: datapath_params.get_ip_to_mac(),
            outgoing_window: HashMap::default(),
            active_connections: [(false, AddressInfo::default()); MAX_CONCURRENT_CONNECTIONS],
            address_to_conn_id: HashMap::default(),
            allocator: MemoryAllocator::default(),
        })
    }

    fn connect(&mut self, addr: AddressInfo) -> Result<ConnID> {
        if self.address_to_conn_id.contains_key(&addr) {
            return Ok(*self.address_to_conn_id.get(&addr).unwrap());
        } else {
            if self.address_to_conn_id.len() >= MAX_CONCURRENT_CONNECTIONS {
                bail!("too many concurrent connections; cannot connect to more");
            }
            let mut idx: Option<usize> = None;
            for (i, (used, _)) in self.active_connections.iter().enumerate() {
                if !used {
                    self.address_to_conn_id.insert(addr, i);
                    idx = Some(i);
                    break;
                }
            }
            match idx {
                Some(i) => {
                    self.active_connections[i] = (true, addr);
                    return Ok(i);
                }
                None => {
                    bail!("too many concurrent connections; cannot connect to more");
                }
            }
        }
    }

    fn push_buffers(&mut self, pkts: Vec<(MsgID, ConnID, &[u8])>) -> Result<()> {
        Ok(())
    }

    fn echo(&mut self, pkts: Vec<ReceivedPkt<Self>>) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn serialize_and_send(
        &mut self,
        objects: &Vec<(MsgID, ConnID, impl Serializable<Self>)>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn push_rc_sgas(&mut self, rc_sgas: &Vec<(MsgID, ConnID, RcSga<Self>)>) -> Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }

    fn push_sgas(&mut self, sgas: &Vec<(MsgID, ConnID, Sga)>) -> Result<()> {
        Ok(())
    }

    fn pop_with_durations(&mut self) -> Result<Vec<(ReceivedPkt<Self>, Duration)>>
    where
        Self: Sized,
    {
        Ok(Vec::default())
    }

    fn pop(&mut self) -> Result<Vec<ReceivedPkt<Self>>>
    where
        Self: Sized,
    {
        Ok(Vec::default())
    }

    fn timed_out(&self, time_out: Duration) -> Result<Vec<MsgID>> {
        let mut timed_out: Vec<MsgID> = Vec::default();
        for (id, start) in self.outgoing_window.iter() {
            if start.elapsed().as_nanos() > time_out.as_nanos() {
                tracing::debug!(elapsed = ?start.elapsed().as_nanos(), id = *id, "Timing out");
                timed_out.push(*id);
            }
        }
        Ok(timed_out)
    }

    fn allocate(&mut self, size: usize, alignment: usize) -> Result<Option<Self::DatapathBuffer>> {
        self.allocator.allocate_data_buffer(size, alignment)
    }

    fn get_metadata(&self, buf: Self::DatapathBuffer) -> Result<Option<Self::DatapathMetadata>> {
        MbufMetadata::from_buf(buf)
    }

    fn add_memory_pool(&mut self, size: usize, min_elts: usize) -> Result<()> {
        // use 2MB pages for data, 2MB pages for metadata (?)
        let metadata_pgsize = match min_elts > 8192 {
            true => PGSIZE_4KB,
            false => PGSIZE_2MB,
        };
        let mempool_params =
            sizes::MempoolAllocationParams::new(min_elts, metadata_pgsize, PGSIZE_2MB, size)
                .wrap_err("Incorrect mempool allocation params")?;
        // add tx memory pool
        let tx_mempool_ptr = unsafe {
            alloc_and_register_tx_pool(
                self.thread_context.get_context_ptr(),
                mempool_params.get_item_len() as _,
                mempool_params.get_num_items() as _,
                mempool_params.get_data_pgsize() as _,
                mempool_params.get_metadata_pgsize() as _,
                ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _,
            )
        };
        ensure!(!tx_mempool_ptr.is_null(), "Allocated tx mempool is null");
        self.allocator
            .add_mempool(mempool_params.get_item_len(), tx_mempool_ptr)?;

        Ok(())
    }

    fn cycles_to_ns(&self, t: u64) -> u64 {
        unsafe { cycles_to_ns(t) }
    }

    fn current_cycles(&self) -> u64 {
        unsafe { current_cycles() }
    }
}
