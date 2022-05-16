use cornflakes_libos::{datapath::Datapath, CSge, ConnID, MsgID, Sga};
use cornflakes_utils::{global_debug_init, AppMode, TraceLevel};
use dpdk_datapath::{datapath::connection::DpdkConnection, dpdk_bindings};
use std::{boxed::Box, ffi::CStr, net::Ipv4Addr, str::FromStr};

/// cbindgen:field-names=[addr, len]
/// cbindgen:derive-eq
#[repr(C)]
pub struct ReceivedBuffer(
    pub *mut ::std::os::raw::c_void,
    pub usize,
    pub MsgID,
    pub ConnID,
);

fn convert_c_char(ptr: *const ::std::os::raw::c_char) -> String {
    let cstr: &CStr = unsafe { CStr::from_ptr(ptr) };
    let str_slice: &str = cstr.to_str().unwrap();
    str_slice.to_string()
}

#[no_mangle]
pub extern "C" fn alloc_sga_vec(size: usize) -> *mut ::std::os::raw::c_void {
    let sga_box: Box<Vec<(MsgID, ConnID, Sga)>> = Box::new(vec![(0, 0, Sga::allocate(size))]);
    Box::into_raw(sga_box) as _
}

#[no_mangle]
pub extern "C" fn free_sga_vec(sga_ptr: *mut ::std::os::raw::c_void) {
    unsafe {
        Box::from_raw(sga_ptr as *mut Vec<(MsgID, ConnID, Sga)>);
    }
}

#[no_mangle]
pub extern "C" fn global_init(trace_str: *const ::std::os::raw::c_char) -> std::os::raw::c_int {
    let level = match TraceLevel::from_str(convert_c_char(trace_str).as_str()) {
        Ok(t) => t,
        Err(_e) => {
            tracing::warn!("Unknown trace_level: {:?}", trace_str);
            return libc::EINVAL;
        }
    };
    match global_debug_init(level) {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Error running global debug init: {:?}", e);
            return libc::EINVAL;
        }
    }
    dpdk_bindings::load_mlx5_driver();
    return 0;
}

#[no_mangle]
pub extern "C" fn new_dpdk_datapath(
    config_file: *const ::std::os::raw::c_char,
    ip_octets: &[u8; 4],
) -> *mut ::std::os::raw::c_void {
    let mut datapath_params = match DpdkConnection::parse_config_file(
        convert_c_char(config_file).as_str(),
        &Ipv4Addr::new(ip_octets[0], ip_octets[1], ip_octets[2], ip_octets[3]),
    ) {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!("Failed to init parse config file for DPDK: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let addresses =
        match DpdkConnection::compute_affinity(&datapath_params, 1, None, AppMode::Server) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("Failed to compute addresses for DPDK: {:?}", e);
                return std::ptr::null_mut() as _;
            }
        };
    let per_thread_contexts = match DpdkConnection::global_init(1, &mut datapath_params, addresses)
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Failed to get per thread contexts for DPDK: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let connection = match DpdkConnection::per_thread_init(
        datapath_params,
        per_thread_contexts.into_iter().nth(0).unwrap(),
        AppMode::Server,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Failed to init DPDK connection: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let boxed_connection = Box::new(connection);
    Box::into_raw(boxed_connection) as _
}

#[no_mangle]
pub extern "C" fn drop_dpdk_connection(connection: *mut ::std::os::raw::c_void) {
    unsafe {
        Box::from_raw(connection as *mut DpdkConnection);
    }
}

#[no_mangle]
pub extern "C" fn pop_packets(
    conn: *mut ::std::os::raw::c_void,
    recv_pkts: &mut [ReceivedBuffer; 32],
    size: *mut usize,
) -> ::std::os::raw::c_int {
    let mut connection_box = unsafe { Box::from_raw(conn as *mut DpdkConnection) };
    let pkts = match connection_box.as_mut().pop() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Error receiving packets: {:?}", e);
            return libc::EINVAL;
        }
    };
    unsafe {
        *size = 0;
    }
    if pkts.len() > 0 {
        for (i, pkt) in pkts.iter().enumerate() {
            // TODO: this code assumes packet comes back as contiguous buffer
            let seg = pkt.seg(0).as_ref();
            recv_pkts[i] =
                ReceivedBuffer(seg.as_ptr() as _, seg.len(), pkt.msg_id(), pkt.conn_id());
        }
        unsafe {
            *size = pkts.len();
        }
    }

    Box::into_raw(connection_box);
    return 0;
}

// TODO: figure out something cleaner other than separate addr and sizes array
#[no_mangle]
pub extern "C" fn push_sga(
    conn: *mut ::std::os::raw::c_void,
    msg_id: MsgID,
    conn_id: ConnID,
    addrs: &[*mut ::std::os::raw::c_void; 32],
    sizes: &[usize; 32],
    num_entries: usize,
    allocated_sga_ptr: *mut ::std::os::raw::c_void,
) -> ::std::os::raw::c_int {
    // convert static sga into dynamic sga
    let mut allocated_sga_vec =
        unsafe { Box::from_raw(allocated_sga_ptr as *mut Vec<(MsgID, ConnID, Sga)>) };
    let sga = &mut allocated_sga_vec.as_mut()[0];
    sga.0 = msg_id;
    sga.1 = conn_id;
    sga.2.from_static_array(addrs, sizes, num_entries);

    // convert connection into DPDK connection
    let mut connection_box = unsafe { Box::from_raw(conn as *mut DpdkConnection) };
    let connection_ptr = connection_box.as_mut();

    // push sgas
    match connection_ptr.push_sgas(allocated_sga_vec.as_ref().as_slice()) {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to push sgas: {:?}", e);
            return libc::EINVAL;
        }
    }

    Box::into_raw(allocated_sga_vec);
    Box::into_raw(connection_box);
    return 0;
}

#[no_mangle]
pub extern "C" fn push_buf(
    conn: *mut ::std::os::raw::c_void,
    msg_id: MsgID,
    conn_id: ConnID,
    buf: &CSge,
) -> ::std::os::raw::c_int {
    // convert connection into DPDK connection
    let mut connection_box = unsafe { Box::from_raw(conn as *mut DpdkConnection) };
    let connection_ptr = connection_box.as_mut();
    let buffers = vec![(msg_id, conn_id, buf.to_slice())];

    connection_ptr
        .push_buffers_with_copy(&buffers.as_slice())
        .unwrap();
    Box::into_raw(connection_box);
    return 0;
}
