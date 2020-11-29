use super::dpdk_bindings;
use color_eyre::eyre::{bail, Result};
use std::ffi::CStr;

pub fn dpdk_ok(ret: ::std::os::raw::c_int) -> Result<()> {
    if ret == -1 {
        unsafe {
            let errno: std::os::raw::c_int = dpdk_bindings::rte_errno();
            let c_buf = dpdk_bindings::rte_strerror(errno);
            let c_str: &CStr = CStr::from_ptr(c_buf);
            let str_slice: &str = c_str.to_str().unwrap();
            bail!("Error {}: {:?}", errno, str_slice);
        }
    } else if -1 > ret {
        unreachable!();
    }
    Ok(())
}
#[macro_export]
macro_rules! dpdk_call (
    ($x: expr) => { unsafe { dpdk_ok($x)? } }
);

pub mod wrapper;
