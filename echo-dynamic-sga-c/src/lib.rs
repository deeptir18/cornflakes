use cornflakes_codegen::utils::dynamic_sga_hdr::SgaHeaderRepr;

mod rust {
    include!(concat!(env!("OUT_DIR"), "/echo_dynamic_sga.rs"));
}

#[repr(C)]
pub struct SingleBufferCF {
    bitmap_len: usize,
    bitmap: *mut ::std::os::raw::c_char,
    message: *mut ::std::os::raw::c_char,
}

///////////////////////////////////////////////////////////////////////////////
// Generated functions in echo_dynamic_sga.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_new() -> *mut SingleBufferCF {
    let single_buffer_cf = rust::SingleBufferCF::new();
    Box::into_raw(Box::new(single_buffer_cf)) as _
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_get_message(
    single_buffer_cf: *mut SingleBufferCF,
) -> *mut ::std::os::raw::c_char {
    unimplemented!()
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_set_message(
    single_buffer_cf: *mut SingleBufferCF,
    message: *mut ::std::os::raw::c_char,
) {
    unimplemented!()
}

///////////////////////////////////////////////////////////////////////////////
// SgaHeaderRepr trait in echo_dynamic_sga.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_num_scatter_gather_entries(
    single_buffer_cf: *mut SingleBufferCF,
) -> usize {
    unimplemented!()
}

///////////////////////////////////////////////////////////////////////////////
// Shared functions in SgaHeaderRepr trait.
// See: cornflakes-codegen/src/utils/dynamic_sga_hdr.rs

#[no_mangle]
pub extern "C" fn SingleBufferCF_deserialize(
    single_buffer_cf: *mut SingleBufferCF,
    buffer: *const ::std::os::raw::c_uchar,
    buffer_len: usize,
) -> u32 {
    let mut single_buffer_cf =
        unsafe { Box::from_raw(single_buffer_cf as *mut rust::SingleBufferCF) };
    let buffer = unsafe { std::slice::from_raw_parts(buffer, buffer_len) };
    let return_code = match single_buffer_cf.deserialize(buffer) {
        Ok(()) => 0,
        Err(e) => 1,
    };
    Box::into_raw(single_buffer_cf);
    return_code
}

#[no_mangle]
pub extern "C" fn SingleBufferCF_serialize_into_sga(
    single_buffer_cf: *mut SingleBufferCF,
    ordered_sga: *mut ::std::os::raw::c_void,
    conn: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}
