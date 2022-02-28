use super::mlx5_bindings;
use color_eyre::eyre::{bail, Result};
use std::ffi::CStr;

pub unsafe fn check(func_name: &str, errno: ::std::os::raw::c_int) -> Result<()> {
    if errno != 0 {
        let c_buf = mlx5_bindings::err_to_str(errno);
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
macro_rules! mbuf_mut_slice(
    ($mbuf: expr, $offset: expr, $len: expr) => {
        std::slice::from_raw_parts_mut(
            ((*$mbuf).buf_addr as *mut u8)
            .offset((*$mbuf).offset as isize + $offset as isize),
            $len,
        )
    }
);

#[macro_export]
macro_rules! mbuf_slice(
    ($mbuf: expr, $offset: expr, $len: expr) => {
        std::slice::from_raw_parts(
            ((*$mbuf).buf_addr as *mut u8)
            .offset((*$mbuf).offset as isize + $offset as isize),
            $len,
        )
    }
);

#[macro_export]
macro_rules! check_ok(
    ($x: ident ($($arg: expr),*)) => {
        check(stringify!($x), $x($($arg),*)).wrap_err("Error running c function.")?
    }
);

mod allocator;
pub mod connection; // implements datapath trait
mod sizes;
mod wrapper; // wrapper to driver code // allocator for mempools
