use cf_kv::{
    retwis::{RetwisServerLoader, RetwisValueSizeGenerator},
    twitter::TwitterServerLoader,
    KVServer, ListKVServer, MsgType, ServerLoadGenerator,
};
use cornflakes_libos::{
    allocator::MempoolID,
    datapath::{Datapath, InlineMode, ReceivedPkt},
    {ArenaOrderedRcSga, OrderedSga},
};
use cornflakes_utils::{global_debug_init_env, AppMode};
use mlx5_datapath::datapath::connection::{Mlx5Buffer, Mlx5Connection};
use std::{ffi::CStr, io::Write, net::Ipv4Addr, str::FromStr};
const MIN_MEMPOOL_BUF_SIZE: usize = 8;

fn pad_mempool_size(size: usize) -> usize {
    if size < MIN_MEMPOOL_BUF_SIZE {
        return MIN_MEMPOOL_BUF_SIZE;
    } else {
        // return nearest power of 2 above this
        return cornflakes_libos::allocator::align_to_pow2(size);
    }
}

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
pub extern "C" fn Mlx5Connection_free_datapath_buffer(
    datapath_buffer: *mut ::std::os::raw::c_void,
) {
    let _datapath_buffer_box = unsafe { Box::from_raw(datapath_buffer as *mut Mlx5Buffer) };
    tracing::debug!(
        "In free datapath buffer, with datapath buffer ptr: {:?}; has cur refcnt {}",
        _datapath_buffer_box.as_ref().as_ref().as_ptr(),
        _datapath_buffer_box.as_ref().read_refcnt()
    );
}
#[no_mangle]
pub extern "C" fn Mlx5Connection_retrieve_raw_ptr(
    datapath_buffer: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let datapath_buffer_box = unsafe { Box::from_raw(datapath_buffer as *mut Mlx5Buffer) };
    unsafe {
        *return_ptr = datapath_buffer_box.as_ref().as_ref().as_ptr() as _;
    }
    Box::into_raw(datapath_buffer_box);
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_allocate_and_copy_into_original_datapath_buffer(
    conn: *mut ::std::os::raw::c_void,
    mempool_ids_vec: *mut ::std::os::raw::c_void,
    data_buffer: *const ::std::os::raw::c_uchar,
    data_buffer_len: usize,
    return_raw_ptr: *mut *mut ::std::os::raw::c_void,
) -> *mut ::std::os::raw::c_void {
    let mut datapath = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let mut mempool_ids_vec_box = unsafe { Box::from_raw(mempool_ids_vec as *mut Vec<MempoolID>) };
    let mut datapath_buffer = match datapath
        .allocate(pad_mempool_size(data_buffer_len))
        .unwrap()
    {
        Some(buf) => buf,
        None => {
            println!(
                "Mempool for size {} doesn't exist; allocating (padded size {})",
                data_buffer_len,
                pad_mempool_size(data_buffer_len)
            );
            println!("Probably crashing at mempool ids box line",);
            let num_mempools = mempool_ids_vec_box.len();
            println!("Didn't make it past mempool ids box line");
            mempool_ids_vec_box.append(
                &mut datapath
                    .add_memory_pool_with_size(pad_mempool_size(data_buffer_len))
                    .unwrap(),
            );
            println!("Adding mempool # {}", num_mempools);
            match datapath
                .allocate(pad_mempool_size(data_buffer_len))
                .unwrap()
            {
                Some(buf) => buf,
                None => {
                    panic!("Could not allocate");
                }
            }
        }
    };
    // copy into data buffer
    let data_slice = unsafe { std::slice::from_raw_parts(data_buffer as _, data_buffer_len as _) };
    let _ = datapath_buffer.write(data_slice).unwrap();

    Box::into_raw(mempool_ids_vec_box);
    Box::into_raw(datapath);
    unsafe {
        *return_raw_ptr = datapath_buffer.as_ref().as_ptr() as _;
    }
    let boxed_buf = Box::new(datapath_buffer);
    Box::into_raw(boxed_buf) as _
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_allocate_datapath_buffer(
    conn: *mut ::std::os::raw::c_void,
    size: usize,
    mempool_ids_vec: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) -> *mut ::std::os::raw::c_void {
    let mut datapath = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let mut mempool_ids_vec_box = unsafe { Box::from_raw(mempool_ids_vec as *mut Vec<MempoolID>) };
    let datapath_buffer = match datapath.allocate(pad_mempool_size(size)).unwrap() {
        Some(buf) => buf,
        None => {
            tracing::debug!(
                "Mempool for size {} doesn't exist; allocating (padded size {})",
                size,
                pad_mempool_size(size)
            );
            let num_mempools = mempool_ids_vec_box.len();
            mempool_ids_vec_box.append(
                &mut datapath
                    .add_memory_pool_with_size(pad_mempool_size(size))
                    .unwrap(),
            );
            tracing::info!("Adding mempool # {}", num_mempools);
            match datapath.allocate(pad_mempool_size(size)).unwrap() {
                Some(buf) => buf,
                None => {
                    panic!("Could not allocate");
                }
            }
        }
    };

    Box::into_raw(mempool_ids_vec_box);
    Box::into_raw(datapath);
    unsafe {
        *return_ptr = datapath_buffer.as_ref().as_ptr() as _;
    }
    let boxed_buf = Box::new(datapath_buffer);
    Box::into_raw(boxed_buf) as _
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
    pkt: *const ::std::os::raw::c_void,
) -> *const ::std::os::raw::c_uchar {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.seg(0).as_ref().as_ptr();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_data_len(pkt: *const ::std::os::raw::c_void) -> usize {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.seg(0).as_ref().len();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_msg_id(pkt: *const ::std::os::raw::c_void) -> u32 {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.msg_id();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_conn_id(pkt: *const ::std::os::raw::c_void) -> usize {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.conn_id();
    Box::into_raw(pkt);
    value
}

#[no_mangle]
pub extern "C" fn Mlx5Connection_RxPacket_free(pkt: *const ::std::os::raw::c_void) {
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

/// returns box of datapath buffer
#[no_mangle]
pub extern "C" fn Mlx5Connection_prepare_single_buffer_with_udp_header(
    conn: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    data_len: usize,
    raw_data_ptr: *mut *mut ::std::os::raw::c_void,
    smart_data_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let buffer = conn_box
        .prepare_single_buffer_with_udp_header((conn_id, msg_id), data_len)
        .unwrap();
    unsafe {
        *raw_data_ptr = buffer.as_ref().as_ptr().offset(cornflakes_libos::utils::TOTAL_HEADER_SIZE as isize) as _;
    }
    let boxed_buffer = Box::new(buffer);
    unsafe {
        *smart_data_ptr = Box::into_raw(boxed_buffer) as _;
    }
    Box::into_raw(conn_box);
}

/// Sends boxed buffer, setting it's length
#[no_mangle]
pub extern "C" fn Mlx5Connection_transmit_single_datapath_buffer_with_header(
    conn: *mut ::std::os::raw::c_void,
    box_buffer: *mut ::std::os::raw::c_void,
    data_len: usize,
    end_batch: usize,
) {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let mut buffer_box = unsafe { Box::from_raw(box_buffer as *mut Mlx5Buffer) };
    buffer_box
        .as_mut()
        .set_len(data_len + cornflakes_libos::utils::TOTAL_HEADER_SIZE);
    conn_box
        .transmit_single_datapath_buffer_with_header(buffer_box, end_batch == 1)
        .unwrap();
    Box::into_raw(conn_box);
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
///////////////////////////////////////////////////////////////////////////////
// cf-kv/src/lib.rs functionality
#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_msg_type(
    pkt: *const ::std::os::raw::c_void,
    size_ptr: *mut u16,
    return_ptr: *mut u16,
) {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let msg_type = MsgType::from_packet(&pkt).unwrap();
    match msg_type {
        MsgType::Get => unsafe {
            *return_ptr = 0;
            *size_ptr = 1;
        },
        MsgType::Put => unsafe {
            *return_ptr = 1;
            *size_ptr = 1;
        },
        MsgType::GetM(s) => unsafe {
            *return_ptr = 2;
            *size_ptr = s;
        },
        MsgType::PutM(_s) => {
            unimplemented!();
        }
        MsgType::GetList(s) => unsafe {
            *return_ptr = 4;
            *size_ptr = s;
        },
        MsgType::PutList(_s) => {
            unimplemented!();
        }
        MsgType::AppendToList(_s) => {
            unimplemented!();
        }
        MsgType::AddUser => {
            unimplemented!();
        }
        MsgType::FollowUnfollow => {
            unimplemented!();
        }
        MsgType::PostTweet => {
            unimplemented!();
        }
        MsgType::GetTimeline(_s) => {
            unimplemented!();
        }
        MsgType::GetFromList => {
            unimplemented!();
        }
    }
    Box::into_raw(pkt);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_size(pkt: *const ::std::os::raw::c_void, return_ptr: *mut u16) {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let seg = pkt.seg(0).as_ref();
    let size = (seg[3] as u16) | ((seg[2] as u16) << 8);
    unsafe { *return_ptr = size };
    Box::into_raw(pkt);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_data(
    pkt: *const ::std::os::raw::c_void,
    return_ptr: *mut *const ::std::os::raw::c_uchar,
) {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.seg(0).as_ref().as_ptr();
    unsafe { *return_ptr = value };
    Box::into_raw(pkt);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_data_len(pkt: *const ::std::os::raw::c_void, return_ptr: *mut usize) {
    let pkt = unsafe { Box::from_raw(pkt as *mut ReceivedPkt<Mlx5Connection>) };
    let value = pkt.seg(0).as_ref().len();
    unsafe { *return_ptr = value };
    Box::into_raw(pkt);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_msg_id(self_: *mut ::std::os::raw::c_void, return_ptr: *mut u32) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReceivedPkt<Mlx5Connection>) };
    let value = self_.msg_id();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_conn_id(self_: *mut ::std::os::raw::c_void, return_ptr: *mut usize) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReceivedPkt<Mlx5Connection>) };
    let value = self_.conn_id();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReceivedPkt_free(self_: *const ::std::os::raw::c_void) {
    let _ = unsafe { Box::from_raw(self_ as *mut ReceivedPkt<Mlx5Connection>) };
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_load_retwis_db(
    conn: *mut ::std::os::raw::c_void,
    key_size: usize,
    value_size: usize,
    num_keys: usize,
    db_ptr: *mut *mut ::std::os::raw::c_void,
    mempools_ptr: *mut *mut ::std::os::raw::c_void,
) -> usize {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let load_generator = RetwisServerLoader::new(
        num_keys,
        key_size,
        RetwisValueSizeGenerator::SingleValue(value_size),
    );
    let (kv, _, _, mempool_ids) = load_generator
        .new_kv_state("", conn_box.as_mut(), false)
        .unwrap();
    let boxed_kv = Box::new(kv);
    let boxed_mempool_ids = Box::new(mempool_ids);
    unsafe {
        *db_ptr = Box::into_raw(boxed_kv) as _;
        *mempools_ptr = Box::into_raw(boxed_mempool_ids) as _;
    }

    Box::into_raw(conn_box);
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_load_twitter_db(
    conn: *mut ::std::os::raw::c_void,
    trace_file: *const ::std::os::raw::c_char,
    end_time: usize,
    min_keys_to_load: usize,
    db_ptr: *mut *mut ::std::os::raw::c_void,
    list_db_ptr: *mut *mut ::std::os::raw::c_void,
    mempools_ptr: *mut *mut ::std::os::raw::c_void,
) -> usize {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };
    let file_str = unsafe { std::ffi::CStr::from_ptr(trace_file).to_str().unwrap() };
    let load_generator = TwitterServerLoader::new(end_time, min_keys_to_load, None);
    let (kv, list_kv, _, mempool_ids) = load_generator
        .new_kv_state(file_str, conn_box.as_mut(), false)
        .unwrap();
    let boxed_kv = Box::new(kv);
    let boxed_list_kv = Box::new(list_kv);
    let boxed_mempool_ids = Box::new(mempool_ids);
    unsafe {
        *db_ptr = Box::into_raw(boxed_kv) as _;
        *list_db_ptr = Box::into_raw(boxed_list_kv) as _;
        *mempools_ptr = Box::into_raw(boxed_mempool_ids) as _;
    }

    Box::into_raw(conn_box);
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_drop_db(db_ptr: *mut ::std::os::raw::c_void) {
    // loading these objects into boxes will drop them
    let _db_box = unsafe { Box::from_raw(db_ptr as *mut KVServer<Mlx5Connection>) };
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_drop_dbs(
    db_ptr: *mut ::std::os::raw::c_void,
    list_db_ptr: *mut ::std::os::raw::c_void,
) {
    // loading these objects into boxes will drop them
    let _db_box = unsafe { Box::from_raw(db_ptr as *mut KVServer<Mlx5Connection>) };
    let _list_db_box = unsafe { Box::from_raw(list_db_ptr as *mut ListKVServer<Mlx5Connection>) };
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_load_ycsb_db(
    conn: *mut ::std::os::raw::c_void,
    trace_file: *const ::std::os::raw::c_char,
    db_ptr: *mut *mut ::std::os::raw::c_void,
    list_db_ptr: *mut *mut ::std::os::raw::c_void,
    mempool_ids_ptr: *mut *mut ::std::os::raw::c_void,
    num_keys: usize,
    num_values: usize,
    value_size: *const ::std::os::raw::c_char,
    use_linked_list: bool,
) -> u32 {
    let mut conn_box = unsafe { Box::from_raw(conn as *mut Mlx5Connection) };

    let file_str = unsafe { std::ffi::CStr::from_ptr(trace_file).to_str().unwrap() };
    let value_size_str = unsafe { std::ffi::CStr::from_ptr(value_size).to_str().unwrap() };
    let value_size_generator =
        cf_kv::ycsb::YCSBValueSizeGenerator::from_str(value_size_str).unwrap();
    let load_generator = cf_kv::ycsb::YCSBServerLoader::new(
        value_size_generator,
        num_values,
        num_keys,
        false,
        use_linked_list,
    );
    let (kv, list_kv, _, mempool_ids) = load_generator
        .new_kv_state(file_str, conn_box.as_mut(), false)
        .unwrap();
    let boxed_kv = Box::new(kv);
    let boxed_list_kv = Box::new(list_kv);
    let boxed_mempool_ids = Box::new(mempool_ids);
    unsafe {
        *db_ptr = Box::into_raw(boxed_kv) as _;
        *list_db_ptr = Box::into_raw(boxed_list_kv) as _;
        *mempool_ids_ptr = Box::into_raw(boxed_mempool_ids) as _;
    }

    Box::into_raw(conn_box);
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_get_db_keys_vec(
    db: *mut ::std::os::raw::c_void,
    db_keys_vec: *mut *mut ::std::os::raw::c_void,
    db_keys_len: *mut usize,
) {
    let db_box = unsafe { Box::from_raw(db as *mut cf_kv::KVServer<Mlx5Connection>) };

    unsafe {
        let keys = db_box.keys();
        *db_keys_len = keys.len();
        let boxed_keys = Box::new(keys);
        *db_keys_vec = Box::into_raw(boxed_keys) as _;
    }

    Box::into_raw(db_box);
}
#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_get_list_db_keys_vec(
    list_db: *mut *mut ::std::os::raw::c_void,
    list_db_keys_vec: *mut *mut ::std::os::raw::c_void,
    list_db_keys_len: *mut usize,
) {
    let list_db_box = unsafe { Box::from_raw(list_db as *mut cf_kv::ListKVServer<Mlx5Connection>) };

    unsafe {
        let keys = list_db_box.keys();
        *list_db_keys_len = keys.len();
        let boxed_keys = Box::new(keys);
        *list_db_keys_vec = Box::into_raw(boxed_keys) as _;
    }

    Box::into_raw(list_db_box);
}
#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_get_db_value_at(
    db: *mut ::std::os::raw::c_void,
    db_keys_vec: *mut ::std::os::raw::c_void,
    key_idx: usize,
    key_ptr: *mut *mut ::std::os::raw::c_void,
    key_len: *mut usize,
    value_ptr: *mut *mut ::std::os::raw::c_void,
    value_len: *mut usize,
    value_box_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let db_box = unsafe { Box::from_raw(db as *mut cf_kv::KVServer<Mlx5Connection>) };
    let db_keys_vec_box = unsafe { Box::from_raw(db_keys_vec as *mut Vec<String>) };

    let key = db_keys_vec_box.get(key_idx).unwrap();
    let value = db_box.get(&key).unwrap();
    unsafe {
        *value_len = value.as_ref().len();
        *value_ptr = value.as_ref().as_ptr() as _;
        *key_len = key.len();
        *key_ptr = key.as_str().as_ptr() as _;
    }
    let cloned_value_box = Box::new(value.clone());
    unsafe {
        *value_box_ptr = Box::into_raw(cloned_value_box) as _;
    }

    Box::into_raw(db_keys_vec_box);
    Box::into_raw(db_box);
}
#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_list_db_get_list_size(
    list_db: *mut ::std::os::raw::c_void,
    list_db_keys_vec: *mut ::std::os::raw::c_void,
    key_idx: usize,
    key_ptr: *mut *mut ::std::os::raw::c_void,
    key_len: *mut usize,
    list_size: *mut usize,
) {
    let list_db_box = unsafe { Box::from_raw(list_db as *mut cf_kv::ListKVServer<Mlx5Connection>) };
    let list_db_keys_vec_box = unsafe { Box::from_raw(list_db_keys_vec as *mut Vec<String>) };
    unsafe {
        let key = list_db_keys_vec_box.get(key_idx).unwrap();
        let value_list = list_db_box.get(&key).unwrap();
        *list_size = value_list.len();
        *key_len = key.len();
        *key_ptr = key.as_str().as_ptr() as _;
    }
    Box::into_raw(list_db_keys_vec_box);
    Box::into_raw(list_db_box);
}
#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_list_db_get_value_at_idx(
    list_db: *mut ::std::os::raw::c_void,
    list_db_keys_vec: *mut ::std::os::raw::c_void,
    key_idx: usize,
    list_idx: usize,
    value_ptr: *mut *mut ::std::os::raw::c_void,
    value_len: *mut usize,
    value_box_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let list_db_box = unsafe { Box::from_raw(list_db as *mut cf_kv::ListKVServer<Mlx5Connection>) };
    let list_db_keys_vec_box = unsafe { Box::from_raw(list_db_keys_vec as *mut Vec<String>) };

    let key = list_db_keys_vec_box.get(key_idx).unwrap();
    let value_list = list_db_box.get(&key).unwrap();
    let value = &value_list[list_idx];
    unsafe {
        *value_len = value.as_ref().len();
        *value_ptr = value.as_ref().as_ptr() as _;
    }
    let cloned_value_box = Box::new(value.clone());
    unsafe {
        *value_box_ptr = Box::into_raw(cloned_value_box) as _;
    }

    Box::into_raw(list_db_keys_vec_box);
    Box::into_raw(list_db_box);
}
