use cornflakes_libos::{
    datapath::{Datapath, ReceivedPkt, InlineMode},
    {ArenaOrderedRcSga, OrderedSga},
};
use cornflakes_utils::{global_debug_init_env, AppMode};
use mlx5_datapath::datapath::connection::Mlx5Connection;
use std::{ffi::CStr, net::Ipv4Addr, str::FromStr};

#[no_mangle]
pub extern "C" fn Mlx5_global_debug_init() {
    global_debug_init_env().unwrap();
}

fn convert_c_char(ptr: *const ::std::os::raw::c_char) -> String {
    let cstr: &CStr = unsafe { CStr::from_ptr(ptr) };
    let str_slice: &str = cstr.to_str().unwrap();
    str_slice.to_string()
}

///////////////////////////////////////////////////////////////////////////////
// cornflakes-libos/src/lib.rs

// // TODO(ygina): move into shared library?
// #[repr(C)]
// #[derive(Debug)]
// pub struct ReceivedPkt {
//     data: *const ::std::os::raw::c_uchar,
//     data_len: usize,
//     msg_id: u32,
//     conn_id: usize,
// }

// TODO(ygina): move into shared library?
#[no_mangle]
pub extern "C" fn OrderedSga_allocate(size: usize, return_ptr: *mut *mut ::std::os::raw::c_void) {
    let ordered_sga = OrderedSga::allocate(size);
    let value = Box::into_raw(Box::new(ordered_sga));
    unsafe { *return_ptr = value as _ };
}

///////////////////////////////////////////////////////////////////////////////
// mlx5-datapath/src/datapath/connection.rs

#[no_mangle]
pub extern "C" fn Mlx5Connection_new(
    config_file: *const ::std::os::raw::c_char,
    server_ip: *const ::std::os::raw::c_char,
) -> *mut ::std::os::raw::c_void {
    let mut datapath_params = match Mlx5Connection::parse_config_file(
        convert_c_char(config_file).as_str(),
        &Ipv4Addr::from_str(convert_c_char(server_ip).as_str()).unwrap(),
    ) {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!("Failed to init parse config file for Mlx5: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let addresses =
        match Mlx5Connection::compute_affinity(&datapath_params, 1, None, AppMode::Server) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("Failed to compute addresses for Mlx5: {:?}", e);
                return std::ptr::null_mut() as _;
            }
        };
    let per_thread_contexts = match Mlx5Connection::global_init(1, &mut datapath_params, addresses)
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Failed to get per thread contexts for Mlx5: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let connection = match Mlx5Connection::per_thread_init(
        datapath_params,
        per_thread_contexts.into_iter().nth(0).unwrap(),
        AppMode::Server,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Failed to init Mlx5 connection: {:?}", e);
            return std::ptr::null_mut() as _;
        }
    };

    let boxed_connection = Box::new(connection);
    Box::into_raw(boxed_connection) as _
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_set_copying_threshold(
    conn: *mut ::std::os::raw::c_void,
    copying_threshold: usize,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    conn_box.set_copying_threshold(copying_threshold);
    Box::into_raw(conn_box);
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_set_inline_mode(
    conn: *mut ::std::os::raw::c_void,
    inline_mode: usize,
) {
    // TODO(ygina): use C enum?
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let inline_mode = match inline_mode {
        0 => InlineMode::Nothing,
        1 => InlineMode::PacketHeader,
        2 => InlineMode::ObjectHeader,
        _ => {
            tracing::warn!("Invalid inline mode: {}", inline_mode);
            return;
        }
    };
    conn_box.set_inline_mode(inline_mode);
    Box::into_raw(conn_box);
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_add_memory_pool(
    conn: *mut ::std::os::raw::c_void,
    buf_size: usize,
    min_elts: usize,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    conn_box.add_memory_pool(buf_size, min_elts).unwrap();
    Box::into_raw(conn_box);
}

// #[no_mangle]
// pub extern "C" fn Mlx5Connection_pop(
//     conn: *mut ::std::os::raw::c_void,
//     n: *mut usize,
// ) -> *mut ReceivedPkt {
//     let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
//     let mut pkts = conn_box
//         .pop()
//         .unwrap()
//         .into_iter()
//         .map(|pkt| {
//             // TODO(ygina): assume one segment
//             let seg = pkt.seg(0);
//             let new_pkt = ReceivedPkt {
//                 data_len: seg.as_ref().len(),
//                 data: seg.as_ref().as_ptr(),
//                 msg_id: pkt.msg_id(),
//                 conn_id: pkt.conn_id(),
//             };
//             // TODO(ygina): prevents deallocation of data buffer but leaks other
//             // fields in the received packet -- implement take() function?
//             std::mem::forget(pkt);
//             new_pkt
//         })
//         .collect::<Vec<ReceivedPkt>>();
//     Box::into_raw(conn_box);
//     unsafe {
//         *n = pkts.len();
//     }
//     let ptr = pkts.as_mut_ptr();
//     Box::into_raw(Box::new(pkts)); // should we return a ptr to the ptr?
//     ptr
// }

#[no_mangle]
pub extern "C" fn Mlx5Connection_pop_raw_packets(
    conn: *mut ::std::os::raw::c_void,
    n: *mut usize,
) -> *mut *mut ::std::os::raw::c_void {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let mut pkts = conn_box
        .pop()
        .unwrap()
        .into_iter()
        .map(|pkt| Box::into_raw(Box::new(pkt)) as *mut ::std::os::raw::c_void)
        .collect::<Vec<*mut ::std::os::raw::c_void>>();
    Box::into_raw(conn_box);
    unsafe {
        *n = pkts.len();
    }
    let ptr = pkts.as_mut_ptr();
    Box::into_raw(Box::new(pkts)); // should we return a ptr to the ptr?
    ptr
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_data(
    pkt: *const ::std::os::raw::c_void
) -> *const ::std::os::raw::c_uchar {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.seg(0).as_ref().as_ptr();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_data_len(
    pkt: *const ::std::os::raw::c_void
) -> usize {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.seg(0).as_ref().len();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_msg_id(
    pkt: *const ::std::os::raw::c_void
) -> u32 {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.msg_id();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_conn_id(
    pkt: *const ::std::os::raw::c_void
) -> usize {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.conn_id();
    Box::into_raw(pkt);
    value
}

pub extern "C" fn Mlx5Connection_RxPacket_free(
    pkt: *const ::std::os::raw::c_void
) {
    let _ = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_push_ordered_sgas(
    conn: *mut ::std::os::raw::c_void,
    n: usize,
    msg_ids: *mut u32,
    conn_ids: *mut usize,
    ordered_sgas: *mut ::std::os::raw::c_void,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let msg_ids: &[u32] = unsafe { std::slice::from_raw_parts(msg_ids, n) };
    let conn_ids: &[usize] = unsafe { std::slice::from_raw_parts(conn_ids, n) };
    let ordered_sgas: &[*mut OrderedSga] =
        unsafe { std::slice::from_raw_parts(ordered_sgas as *const *mut OrderedSga, n) };
    let data = (0..n)
        .map(|i| {
            (msg_ids[i], conn_ids[i], unsafe {
                *Box::from_raw(ordered_sgas[i])
            })
        })
        .collect::<Vec<_>>();
    conn_box.push_ordered_sgas(&data[..]).unwrap();
    Box::into_raw(conn_box);
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_queue_arena_ordered_rcsga(
    conn: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    arena_ordered_rc_sga: *mut ::std::os::raw::c_void,
    end_batch: bool,
) -> u32 {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let arg0 = msg_id;
    let arg1 = conn_id;
    let arg2 =
        unsafe { Box::from_raw(arena_ordered_rc_sga as *mut ArenaOrderedRcSga<Mlx5Connection>) };
    let arg3 = end_batch;
    match conn_box.queue_arena_ordered_rcsga((arg0, arg1, *arg2), arg3) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("{:?}", e);
            return 1;
        }
    };
    Box::into_raw(conn_box);
    0
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_queue_single_buffer_with_copy(
    conn: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    buffer: *const ::std::os::raw::c_uchar,
    buffer_len: usize,
    end_batch: bool,
) -> u32 {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let buffer = unsafe { std::slice::from_raw_parts(buffer, buffer_len) };
    match conn_box.queue_single_buffer_with_copy((msg_id, conn_id, buffer), end_batch) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("{:?}", e);
            return 1;
        }
    }
    Box::into_raw(conn_box);
    return 0;
}

