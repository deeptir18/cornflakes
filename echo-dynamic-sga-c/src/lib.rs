use cornflakes_codegen::utils::dynamic_sga_hdr::CFBytes;
use cornflakes_libos::OrderedSga;
use linux_datapath::datapath::connection::LinuxConnection;

include!(concat!(env!("OUT_DIR"), "/echo_dynamic_sga.rs"));

///////////////////////////////////////////////////////////////////////////////
// Generated functions in echo_dynamic_sga.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_new(
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let single_buffer_cf = SingleBufferCF::new();
    let value = Box::into_raw(Box::new(single_buffer_cf));
    unsafe { *return_ptr = value as _ };
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_get_message(
    single_buffer_cf: *mut ::std::os::raw::c_void,
    return_ptr: *mut *const ::std::os::raw::c_uchar,
    return_len_ptr: *mut usize,
) {
    let single_buffer_cf = unsafe { Box::from_raw(single_buffer_cf as *mut SingleBufferCF) };
    // get_ptr() required for CFBytes wrapper
    let value = single_buffer_cf.get_message().get_ptr().as_ptr();
    let value_len = single_buffer_cf.get_message().len();
    unsafe { *return_ptr = value };
    unsafe { *return_len_ptr = value_len };
    Box::into_raw(single_buffer_cf);
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_set_message(
    single_buffer_cf: *mut ::std::os::raw::c_void,
    message: *const ::std::os::raw::c_uchar,
    message_len: usize,
) {
    let mut single_buffer_cf = unsafe { Box::from_raw(single_buffer_cf as *mut SingleBufferCF) };
    let buffer = unsafe { std::slice::from_raw_parts(message, message_len) };
    let value = CFBytes::new(buffer);
    single_buffer_cf.set_message(value);
    Box::into_raw(single_buffer_cf);
}

///////////////////////////////////////////////////////////////////////////////
// SgaHeaderRepr trait in echo_dynamic_sga.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_num_scatter_gather_entries(
    single_buffer_cf: *mut ::std::os::raw::c_void,
    return_ptr: *mut usize,
) {
    let single_buffer_cf = unsafe { Box::from_raw(single_buffer_cf as *mut SingleBufferCF) };
    let value = single_buffer_cf.num_scatter_gather_entries();
    unsafe { *return_ptr = value };
    Box::into_raw(single_buffer_cf);
}

///////////////////////////////////////////////////////////////////////////////
// Shared functions in SgaHeaderRepr trait.
// See: cornflakes-codegen/src/utils/dynamic_sga_hdr.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_deserialize(
    single_buffer_cf: *mut ::std::os::raw::c_void,
    buffer: *const ::std::os::raw::c_uchar,
    buffer_len: usize,
) -> u32 {
    let mut single_buffer_cf = unsafe { Box::from_raw(single_buffer_cf as *mut SingleBufferCF) };
    let buffer = unsafe { std::slice::from_raw_parts(buffer, buffer_len) };
    let return_code = match single_buffer_cf.deserialize(buffer) {
        Ok(()) => 0,
        Err(_) => 1,
    };
    Box::into_raw(single_buffer_cf);
    return_code
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_serialize_into_sga(
    single_buffer_cf: *mut ::std::os::raw::c_void,
    ordered_sga: *mut ::std::os::raw::c_void,
    conn: *mut ::std::os::raw::c_void,
) -> u32 {
    let single_buffer_cf = unsafe { Box::from_raw(single_buffer_cf as *mut
       SingleBufferCF) };
    let mut ordered_sga = unsafe { Box::from_raw(ordered_sga as *mut OrderedSga) };
    let conn = unsafe { Box::from_raw(conn as *mut LinuxConnection) };
    let return_code = match single_buffer_cf.serialize_into_sga(&mut
        ordered_sga, conn.as_ref()) {
        Ok(()) => 0,
        Err(_) => 1,
    };
    // NOTE(ygina): free ordered sga?
    Box::into_raw(single_buffer_cf);
    Box::into_raw(ordered_sga);
    Box::into_raw(conn);
    return_code
}
