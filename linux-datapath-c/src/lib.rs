// TODO(ygina): move into shared library?
#[repr(C)]
pub struct ReceivedPkt {
    data: *mut ::std::os::raw::c_char,
    data_len: usize,
    msg_id: i32,
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
pub extern "C" fn LinuxConnection_parse_config_file(
    config_file: *mut ::std::os::raw::c_char,
    server_ip: *mut ::std::os::raw::c_char,
) -> *mut ::std::os::raw::c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_compute_affinity(
    datapath_params: *mut ::std::os::raw::c_void,
    num_queues: usize,
    remote_ip: *mut ::std::os::raw::c_void,
    is_server: bool,
) -> *mut ::std::os::raw::c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_global_init(
    num_queues: usize,
    datapath_params: *mut ::std::os::raw::c_void,
    addresses: *mut ::std::os::raw::c_void,
) -> *mut ::std::os::raw::c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_per_thread_init(
    datapath_params: *mut ::std::os::raw::c_void,
    context: *mut ::std::os::raw::c_void,
    is_server: bool,
) -> *mut ::std::os::raw::c_void {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_set_copying_threshold(
    conn: *mut ::std::os::raw::c_void,
    copying_threshold: usize,
) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_set_inline_mode(
    conn: *mut ::std::os::raw::c_void,
    inline_mode: usize,
) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_add_memory_pool(
    conn: *mut ::std::os::raw::c_void,
    buf_size: usize,
    min_elts: usize,
) {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn LinuxConnection_pop(
    conn: *mut ::std::os::raw::c_void,
    n: *mut ::std::os::raw::c_int,
) -> *mut *mut ReceivedPkt {
    unimplemented!()
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
