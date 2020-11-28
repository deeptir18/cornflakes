use cornflakes_libos::dpdk_bindings;
use std::env;
use std::ffi::CString;
fn main() {
    dpdk_bindings::load_mlx5_driver();
    let mut args = vec![];
    let mut ptrs = vec![];
    for arg in env::args().skip(1) {
        let s = CString::new(arg).unwrap();
        ptrs.push(s.as_ptr() as *mut u8);
        args.push(s);
    }
    println!("Args: {:?}", args);
    unsafe {
        dpdk_bindings::rte_eal_init(ptrs.len() as i32, ptrs.as_ptr() as *mut _);
    };
}
