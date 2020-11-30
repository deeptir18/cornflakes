use super::dpdk_bindings;
use color_eyre::eyre::{bail, Result};
use std::ffi::CStr;

pub unsafe fn dpdk_check(func_name: &str, ret: ::std::os::raw::c_int) -> Result<()> {
    if ret != 0 {
        dpdk_error(func_name, Some(ret))?;
    }
    Ok(())
}

pub unsafe fn dpdk_error(func_name: &str, retval: Option<std::os::raw::c_int>) -> Result<()> {
    let errno = match retval {
        Some(x) => x,
        None => dpdk_bindings::rte_errno(),
    };
    let c_buf = dpdk_bindings::rte_strerror(-1 * errno);
    let c_str: &CStr = CStr::from_ptr(c_buf);
    let str_slice: &str = c_str.to_str().unwrap();
    bail!(
        "Exiting from {}: Error {}: {:?}",
        func_name,
        errno,
        str_slice
    );
}

#[macro_export]
macro_rules! dpdk_check_not_failed(
    ($x: ident ($($arg: expr),*)) =>  {
        unsafe {
            let ret = $x($($arg),*);
            if ret == -1 {
                dpdk_error(stringify!($x), None)?;
            }
        }
    };
    ($x: ident ($($arg: expr),*), $str: expr) => {
        unsafe {
            let ret = $x($($arg),*);
            if (ret == -1) {
                bail!("Exiting from {}: Error {}", stringify!($x), $str);
            }
            ret
        }
    };
);

#[macro_export]
macro_rules! dpdk_ok (
    ($x: ident ($($arg: expr),*)) => { unsafe {
        dpdk_check(stringify!($x), $x($($arg),*))?
    } }
);

#[macro_export]
macro_rules! dpdk_call (
    ($x: expr) => { unsafe { $x } }
);

pub mod wrapper;
