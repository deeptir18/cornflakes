use super::dpdk_bindings;
use color_eyre::eyre::{bail, Result};
use std::ffi::CStr;

pub unsafe fn dpdk_check(
    func_name: &str,
    ret: ::std::os::raw::c_int,
    use_errno: bool,
) -> Result<()> {
    if ret != 0 {
        if use_errno {
            dpdk_error(func_name, None)?;
        } else {
            dpdk_error(func_name, Some(ret))?;
        }
    }
    Ok(())
}

pub unsafe fn dpdk_error(func_name: &str, retval: Option<std::os::raw::c_int>) -> Result<()> {
    let mut errno = match retval {
        Some(x) => x,
        None => dpdk_bindings::rte_errno(),
    };
    if errno < 0 {
        errno *= -1;
    }
    let c_buf = dpdk_bindings::rte_strerror(errno);
    let c_str: &CStr = CStr::from_ptr(c_buf);
    let str_slice: &str = c_str.to_str().unwrap();
    bail!(
        "Exiting from {}: Error {}: {:?}",
        func_name,
        errno,
        str_slice
    );
}

pub unsafe fn check(func_name: &str, errno: ::std::os::raw::c_int) -> Result<()> {
    if errno != 0 {
        let c_buf = dpdk_bindings::rte_strerror(errno);
        let c_str: &CStr = CStr::from_ptr(c_buf);
        let str_slice: &str = c_str.to_str().unwrap();
        bail!("Function {} failed from {} error", func_name, str_slice);
    }
    Ok(())
}

#[macro_export]
macro_rules! access(
    ($struct: expr, $field: ident) => {
         (*$struct).$field
    };
    ($struct: expr, $field: ident, $cast: ty) => {
         (*$struct).$field as $cast
    };
);

#[macro_export]
macro_rules! write_struct_field(
    ($struct: expr, $field: ident, $val: expr) => {
        (*$struct).$field = $val as _
    };
);

#[macro_export]
macro_rules! mbuf_mut_slice(
    ($mbuf: expr, $offset: expr, $len: expr) => {
        std::slice::from_raw_parts_mut(
            ((*$mbuf).buf_addr as *mut u8)
            .offset((*$mbuf).data_off as isize + $offset as isize),
            $len,
        )
    }
);

#[macro_export]
macro_rules! mbuf_slice(
    ($mbuf: expr, $offset: expr, $len: expr) => {
        std::slice::from_raw_parts(
            ((*$mbuf).buf_addr as *mut u8)
            .offset((*$mbuf).data_off as isize + $offset as isize),
            $len,
        )
    }
);

#[macro_export]
macro_rules! dpdk_check_not_failed(
    ($x: ident ($($arg: expr),*)) =>  {
        {
        let ret = $x($($arg),*);
        if ret == -1 {
            dpdk_error(stringify!($x), None)?;
        }
        }
    };
    ($x: ident ($($arg: expr),*), $str: expr) => {
        {
        let ret = $x($($arg),*);
        if (ret == -1) {
            bail!("Exiting from {}: Error {}", stringify!($x), $str);
        }
        ret
        }
    };
);

#[macro_export]
macro_rules! dpdk_check_not_errored (
    ($x: ident ($($arg: expr),*)) => { unsafe {
        dpdk_check(stringify!($x), $x($($arg),*), false).wrap_err("Error running dpdk function.")?
    } };
    ($x: ident ($($arg: expr),*), $y: ident ($($arg2: expr),*)) => {
        {
            match dpdk_check(stringify!($x), $x($($arg),*)) {
                Ok(_) => {}
                Err(e) => {
                    // y is an error function to call
                    $y($($arg2),*);
                    bail!("{:?}", e);
                }
            }
        }
    };
);

#[macro_export]
macro_rules! check_ok(
    ($x: ident ($($arg: expr),*)) => {
        check(stringify!($x), $x($($arg),*)).wrap_err("Error running c function.")?
    }
);
mod allocator;
pub mod connection;
mod dpdk_utils;
mod wrapper;
