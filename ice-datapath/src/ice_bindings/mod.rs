#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(improper_ctypes)]

include!(concat!(env!("OUT_DIR"), "/ice_bindings.rs"));

#[link(name = "iceinlined")]
extern "C" {
    fn custom_ice_fill_in_hdrs_(
        hdr_buffer: *mut ::std::os::raw::c_void,
        hdr: *const ::std::os::raw::c_void,
        id: u32,
        data_len: usize,
    );
}

#[inline]
pub unsafe fn fill_in_hdrs(
    hdr_buffer: *mut ::std::os::raw::c_void,
    hdr: *const ::std::os::raw::c_void,
    id: u32,
    data_len: usize,
) {
    custom_ice_fill_in_hdrs_(hdr_buffer, hdr, id, data_len);
}
