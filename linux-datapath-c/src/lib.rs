use cornflakes_libos::datapath::{Datapath, InlineMode};
use cornflakes_utils::AppMode;
use linux_datapath::datapath::connection::LinuxConnection;
use std::{ffi::CStr, net::Ipv4Addr, str::FromStr};

fn convert_c_char(ptr: *const ::std::os::raw::c_char) -> String {
    let cstr: &CStr = unsafe { CStr::from_ptr(ptr) };
    let str_slice: &str = cstr.to_str().unwrap();
    str_slice.to_string()
}

// TODO(ygina): move into shared library?
#[repr(C)]
pub struct ReceivedPkt {
    data: *const ::std::os::raw::c_uchar,
    data_len: usize,
    msg_id: u32,
    conn_id: usize,
}

// TODO(ygina): move into shared library?
#[no_mangle]
pub extern "C" fn OrderedSga_allocate(
    size: usize,
) -> *mut ::std::os::raw::c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_new(
    config_file: *const ::std::os::raw::c_char,
    server_ip: *const ::std::os::raw::c_char,
) -> *mut ::std::os::raw::c_void {
    let mut datapath_params = match LinuxConnection::parse_config_file(
        convert_c_char(config_file).as_str(),
        &Ipv4Addr::from_str(convert_c_char(server_ip).as_str()).unwrap(),
    ) {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!("Failed to init parse config file for Linux: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let addresses =
        match LinuxConnection::compute_affinity(&datapath_params, 1, None, AppMode::Server) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("Failed to compute addresses for Linux: {:?}", e);
                return std::ptr::null_mut() as _;
            }
        };
    let per_thread_contexts = match LinuxConnection::global_init(1, &mut datapath_params, addresses)
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Failed to get per thread contexts for Linux: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let connection = match LinuxConnection::per_thread_init(
        datapath_params,
        per_thread_contexts.into_iter().nth(0).unwrap(),
        AppMode::Server,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Failed to init Linux connection: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let boxed_connection = Box::new(connection);
    Box::into_raw(boxed_connection) as _
}

#[no_mangle]
pub extern "C" fn LinuxConnection_set_copying_threshold(
    conn: *mut ::std::os::raw::c_void,
    copying_threshold: usize,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut LinuxConnection) };
    conn_box.set_copying_threshold(copying_threshold);
    Box::into_raw(conn_box);
}

#[no_mangle]
pub extern "C" fn LinuxConnection_set_inline_mode(
    conn: *mut ::std::os::raw::c_void,
    inline_mode: usize,
) {
    // TODO(ygina): use C enum?
    let mut conn_box = unsafe { Box::from_raw(conn as *mut LinuxConnection) };
    let inline_mode = match inline_mode {
        0 => InlineMode::Nothing,
        1 => InlineMode::PacketHeader,
        2 => InlineMode::ObjectHeader,
        _ => {
            tracing::warn!("Invalid inline mode: {}", inline_mode);
            return;
        }
    };
    conn_box.set_inline_mode(inline_mode);
    Box::into_raw(conn_box);
}

#[no_mangle]
pub extern "C" fn LinuxConnection_add_memory_pool(
    conn: *mut ::std::os::raw::c_void,
    buf_size: usize,
    min_elts: usize,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut LinuxConnection) };
    conn_box.add_memory_pool(buf_size, min_elts).unwrap();
    Box::into_raw(conn_box);
}

#[no_mangle]
pub extern "C" fn LinuxConnection_pop(
    conn: *mut ::std::os::raw::c_void,
    n: *mut usize,
) -> *mut *mut ReceivedPkt {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut LinuxConnection) };
    let mut pkts = conn_box.pop().unwrap().into_iter().map(|pkt| {
        // TODO(ygina): assume one segment
        let seg = pkt.seg(0);
        Box::into_raw(Box::new(ReceivedPkt {
            data_len: seg.as_ref().len(),
            data: seg.as_ref().as_ptr(),
            msg_id: pkt.msg_id(),
            conn_id: pkt.conn_id(),
        }))
    }).collect::<Vec<*mut ReceivedPkt>>();
    unsafe { *n = pkts.len(); }
    Box::into_raw(Box::new(pkts.as_mut_ptr())) as _
}

#[no_mangle]
pub extern "C" fn LinuxConnection_push_ordered_sgas(
    conn: *mut ::std::os::raw::c_void,
    n: ::std::os::raw::c_int,
    msg_ids: *mut i32,
    conn_ids: *mut usize,
    ordered_sgas: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}
