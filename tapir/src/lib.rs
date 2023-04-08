pub mod cf_dynamic;
use bumpalo;
use cornflakes_libos::ArenaOrderedSga;
use cf_dynamic::tapir_serializer::*;
use mlx5_datapath::datapath::connection::Mlx5Connection;
use cornflakes_libos::datapath::{Datapath, ReceivedPkt};
use cornflakes_libos::dynamic_object_arena_hdr::*;

#[inline]
#[no_mangle]
pub extern "C" fn Bump_with_capacity(
    batch_size: usize,
    max_packet_size: usize,
    max_entries: usize,
) -> *mut ::std::os::raw::c_void {
    let capacity = ArenaOrderedSga::arena_size(batch_size, max_packet_size, max_entries);
    let bump_arena = bumpalo::Bump::with_capacity(capacity);
    let arena = Box::into_raw(Box::new(bump_arena));
    arena as _
}

#[inline]
#[no_mangle]
pub extern "C" fn Bump_reset(self_: *mut ::std::os::raw::c_void) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut bumpalo::Bump) };
    self_.reset();
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn print_hello() {
    println!("hello");
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_new_in<'arena>(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = ReplyInconsistentMessage::<'arena, Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_get_view<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    let value = self_.get_view();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_set_view<'registered>(self_: *mut ::std::os::raw::c_void, view: u64) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    self_.set_view(view);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_get_replicaIdx<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u32,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    let value = self_.get_replicaIdx();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_set_replicaIdx<'registered>(self_: *mut ::std::os::raw::c_void, replicaIdx: u32) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    self_.set_replicaIdx(replicaIdx);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_get_finalized<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u32,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    let value = self_.get_finalized();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_set_finalized<'registered>(self_: *mut ::std::os::raw::c_void, finalized: u32) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    self_.set_finalized(finalized);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_get_mut_opid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    let value: *mut OpID<'registered, Mlx5Connection> = self_.get_mut_opid();
    unsafe { *return_ptr = value as _ };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_deserialize<'arena>(
    self_: *mut ::std::os::raw::c_void,
    pkt: *mut ::std::os::raw::c_void,
    offset: usize,
    arena: *mut ::std::os::raw::c_void,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'arena, Mlx5Connection>) };
    let arg0 = pkt as *const ReceivedPkt<Mlx5Connection>;
    let arg1 = offset;
    let arg2 = arena as *const bumpalo::Bump;
    let value = self_.deserialize(unsafe { &*arg0 }, arg1, unsafe { &*arg2 });
    match value {
        Ok(value) => value,
        Err(_) => {
            return 1;
        }
    }
    Box::into_raw(self_);
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn Mlx5Connection_ReplyInconsistentMessage_queue_cornflakes_arena_object<'arena>(
    self_: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    cornflakes_obj: *mut ::std::os::raw::c_void,
    end_batch: bool,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut Mlx5Connection) };
    let arg0 = msg_id;
    let arg1 = conn_id;
    let arg2 = unsafe { *Box::from_raw(cornflakes_obj as *mut ReplyInconsistentMessage<'arena, Mlx5Connection>) };
    let arg3 = end_batch;
    let value = self_.queue_cornflakes_arena_object(arg0, arg1, arg2, arg3);
    match value {
        Ok(value) => value,
        Err(_) => {
            return 1;
        }
    }
    Box::into_raw(self_);
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyInconsistentMessage_free<'arena>(self_: *const ::std::os::raw::c_void) {
    let _ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'arena, Mlx5Connection>) };
}


#[inline]
#[no_mangle]
pub extern "C" fn OpID_get_clientid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut OpID<'registered, Mlx5Connection>) };
    let value = self_.get_clientid();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn OpID_set_clientid<'registered>(self_: *mut ::std::os::raw::c_void, clientid: u64) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut OpID<'registered, Mlx5Connection>) };
    self_.set_clientid(clientid);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn OpID_get_clientreqid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut OpID<'registered, Mlx5Connection>) };
    let value = self_.get_clientreqid();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn OpID_set_clientreqid<'registered>(self_: *mut ::std::os::raw::c_void, clientreqid: u64) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut OpID<'registered, Mlx5Connection>) };
    self_.set_clientreqid(clientreqid);
    Box::into_raw(self_);
}


