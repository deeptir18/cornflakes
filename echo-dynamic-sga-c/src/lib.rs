use cornflakes_codegen::utils::dynamic_sga_hdr::*;
use cornflakes_libos::OrderedSga;
use linux_datapath::datapath::connection::LinuxConnection;

include!(concat!(env!("OUT_DIR"), "/echo_dynamic_sga.rs"));

///////////////////////////////////////////////////////////////////////////////
// Generated functions in echo_dynamic_sga.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_new(
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let self_ = SingleBufferCF::new();
    let value = Box::into_raw(Box::new(self_));
    unsafe { *return_ptr = value as _ };
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_get_message(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *const ::std::os::raw::c_uchar,
    return_len_ptr: *mut usize,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut SingleBufferCF) };
    // get_ptr() required for CFBytes wrapper
    let value = self_.get_message().get_ptr().as_ptr();
    let value_len = self_.get_message().len();
    unsafe { *return_ptr = value };
    unsafe { *return_len_ptr = value_len };
    Box::into_raw(self_);
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_set_message(
    self_: *mut ::std::os::raw::c_void,
    message: *const ::std::os::raw::c_uchar,
    message_len: usize,
) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut SingleBufferCF) };
    let buffer = unsafe { std::slice::from_raw_parts(message, message_len) };
    let value = CFBytes::new(buffer);
    self_.set_message(value);
    Box::into_raw(self_);
}

///////////////////////////////////////////////////////////////////////////////
// SgaHeaderRepr trait in echo_dynamic_sga.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_num_scatter_gather_entries(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut usize,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut SingleBufferCF) };
    let value = self_.num_scatter_gather_entries();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

///////////////////////////////////////////////////////////////////////////////
// Shared functions in SgaHeaderRepr trait.
// See: cornflakes-codegen/src/utils/dynamic_sga_hdr.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_deserialize(
    self_: *mut ::std::os::raw::c_void,
    buffer: *const ::std::os::raw::c_uchar,
    buffer_len: usize,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut SingleBufferCF) };
    let buffer = unsafe { std::slice::from_raw_parts(buffer, buffer_len) };
    let return_code = match self_.deserialize(buffer) {
        Ok(()) => 0,
        Err(_) => 1,
    };
    Box::into_raw(self_);
    return_code
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_serialize_into_sga(
    self_: *mut ::std::os::raw::c_void,
    ordered_sga: *mut ::std::os::raw::c_void,
    conn: *mut ::std::os::raw::c_void,
) -> u32 {
    let self_ = unsafe { Box::from_raw(self_ as *mut SingleBufferCF) };
    let mut ordered_sga = unsafe { Box::from_raw(ordered_sga as *mut OrderedSga) };
    let conn = unsafe { Box::from_raw(conn as *mut LinuxConnection) };
    let return_code = match self_.serialize_into_sga(&mut
        ordered_sga, conn.as_ref()) {
        Ok(()) => 0,
        Err(_) => 1,
    };
    // NOTE(ygina): free ordered sga?
    Box::into_raw(self_);
    Box::into_raw(ordered_sga);
    Box::into_raw(conn);
    return_code
}
