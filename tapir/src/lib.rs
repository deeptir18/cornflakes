pub mod cf_dynamic;
use bumpalo;
use cornflakes_libos::ArenaOrderedSga;
use cf_dynamic::tapir_serializer::*;
use mlx5_datapath::datapath::connection::{Mlx5Connection};
use cornflakes_libos::datapath::{Datapath};
use cornflakes_libos::dynamic_object_arena_hdr::*;

// Arena functions

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

// ReplyInconsistentMessage

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
pub extern "C" fn ReplyInconsistentMessage_set_replicaIdx<'registered>(self_: *mut ::std::os::raw::c_void, replica_idx: u32) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'registered, Mlx5Connection>) };
    self_.set_replicaIdx(replica_idx);
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
    data: *const ::std::os::raw::c_void,
    data_len: usize,
    offset: usize,
    arena: *mut ::std::os::raw::c_void,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ReplyInconsistentMessage<'arena, Mlx5Connection>) };
    let data_slice = unsafe { std::slice::from_raw_parts(data as _, data_len as _) };
    let arg1 = offset;
    let arg2 = arena as *const bumpalo::Bump;
    let value = self_.deserialize_from_raw(data_slice, arg1, unsafe { &*arg2 });
    //let value = self_.deserialize(unsafe { &*arg0 }, arg1, unsafe { &*arg2 });
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

// OpID

#[inline]
#[no_mangle]
pub extern "C" fn OpID_get_clientid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let opid = self_ as *mut OpID<'registered, Mlx5Connection>;
    let value = unsafe { (*opid).get_clientid() };
    unsafe { *return_ptr = value };
}

#[inline]
#[no_mangle]
pub extern "C" fn OpID_set_clientid<'registered>(self_: *mut ::std::os::raw::c_void, clientid: u64) {
    let opid = self_ as *mut OpID<'registered, Mlx5Connection>;
    unsafe { (*opid).set_clientid(clientid) };
}

#[inline]
#[no_mangle]
pub extern "C" fn OpID_get_clientreqid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let opid = self_ as *mut OpID<'registered, Mlx5Connection>;
    let value = unsafe { (*opid).get_clientreqid() };
    unsafe { *return_ptr = value };
}

#[inline]
#[no_mangle]
pub extern "C" fn OpID_set_clientreqid<'registered>(self_: *mut ::std::os::raw::c_void, clientreqid: u64) {
    let opid = self_ as *mut OpID<'registered, Mlx5Connection>;
    unsafe { (*opid).set_clientreqid(clientreqid) } ;
}

// ConfirmMessage

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_new_in<'arena>(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = ConfirmMessage::<'arena, Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_get_view<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'registered, Mlx5Connection>) };
    let value = self_.get_view();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_set_view<'registered>(self_: *mut ::std::os::raw::c_void, view: u64) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'registered, Mlx5Connection>) };
    self_.set_view(view);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_get_replicaIdx<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u32,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'registered, Mlx5Connection>) };
    let value = self_.get_replicaIdx();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_set_replicaIdx<'registered>(self_: *mut ::std::os::raw::c_void, replica_idx: u32) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'registered, Mlx5Connection>) };
    self_.set_replicaIdx(replica_idx);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_get_mut_opid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'registered, Mlx5Connection>) };
    let value: *mut OpID<'registered, Mlx5Connection> = self_.get_mut_opid();
    unsafe { *return_ptr = value as _ };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ConfirmMessage_deserialize<'arena>(
    self_: *mut ::std::os::raw::c_void,
    data: *const ::std::os::raw::c_void,
    data_len: usize,
    offset: usize,
    arena: *mut ::std::os::raw::c_void,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'arena, Mlx5Connection>) };
    let data_slice = unsafe { std::slice::from_raw_parts(data as _, data_len as _) };
    let arg1 = offset;
    let arg2 = arena as *const bumpalo::Bump;
    let value = self_.deserialize_from_raw(data_slice, arg1, unsafe { &*arg2 });
    //let value = self_.deserialize(unsafe { &*arg0 }, arg1, unsafe { &*arg2 });
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
pub extern "C" fn Mlx5Connection_ConfirmMessage_queue_cornflakes_arena_object<'arena>(
    self_: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    cornflakes_obj: *mut ::std::os::raw::c_void,
    end_batch: bool,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut Mlx5Connection) };
    let arg0 = msg_id;
    let arg1 = conn_id;
    let arg2 = unsafe { *Box::from_raw(cornflakes_obj as *mut ConfirmMessage<'arena, Mlx5Connection>) };
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
pub extern "C" fn ConfirmMessage_free<'arena>(self_: *const ::std::os::raw::c_void) {
    let _ = unsafe { Box::from_raw(self_ as *mut ConfirmMessage<'arena, Mlx5Connection>) };
}

// ReplyConsensusMessage

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_new_in<'arena>(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = ReplyConsensusMessage::<'arena, Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_get_view<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    let value = self_.get_view();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_set_view<'registered>(self_: *mut ::std::os::raw::c_void, view: u64) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    self_.set_view(view);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_get_replicaIdx<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u32,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    let value = self_.get_replicaIdx();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_set_replicaIdx<'registered>(self_: *mut ::std::os::raw::c_void, replica_idx: u32) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    self_.set_replicaIdx(replica_idx);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_get_finalized<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u32,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    let value = self_.get_finalized();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_set_finalized<'registered>(self_: *mut ::std::os::raw::c_void, finalized: u32) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    self_.set_finalized(finalized);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_get_mut_opid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    let value: *mut OpID<'registered, Mlx5Connection> = self_.get_mut_opid();
    unsafe { *return_ptr = value as _ };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_get_mut_result<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    let value: *mut Reply<'registered, Mlx5Connection> = self_.get_mut_result();
    unsafe { *return_ptr = value as _ };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_set_result<'registered>(
    self_: *mut ::std::os::raw::c_void,
    result: *mut ::std::os::raw::c_void,
) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'registered, Mlx5Connection>) };
    // Should do box from raw if the Reply was created with new
    let tapir_reply = unsafe { Box::from_raw(result as *mut Reply<'registered, Mlx5Connection>) }; 
    self_.set_result((*tapir_reply).clone());
    Box::into_raw(self_);
    Box::into_raw(tapir_reply);
}

#[inline]
#[no_mangle]
pub extern "C" fn ReplyConsensusMessage_deserialize<'arena>(
    self_: *mut ::std::os::raw::c_void,
    data: *const ::std::os::raw::c_void,
    data_len: usize,
    offset: usize,
    arena: *mut ::std::os::raw::c_void,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'arena, Mlx5Connection>) };
    let data_slice = unsafe { std::slice::from_raw_parts(data as _, data_len as _) };
    let arg1 = offset;
    let arg2 = arena as *const bumpalo::Bump;
    let value = self_.deserialize_from_raw(data_slice, arg1, unsafe { &*arg2 });
    //let value = self_.deserialize(unsafe { &*arg0 }, arg1, unsafe { &*arg2 });
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
pub extern "C" fn Mlx5Connection_ReplyConsensusMessage_queue_cornflakes_arena_object<'arena>(
    self_: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    cornflakes_obj: *mut ::std::os::raw::c_void,
    end_batch: bool,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut Mlx5Connection) };
    let arg0 = msg_id;
    let arg1 = conn_id;
    let arg2 = unsafe { *Box::from_raw(cornflakes_obj as *mut ReplyConsensusMessage<'arena, Mlx5Connection>) };
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
pub extern "C" fn ReplyConsensusMessage_free<'arena>(self_: *const ::std::os::raw::c_void) {
    let _ = unsafe { Box::from_raw(self_ as *mut ReplyConsensusMessage<'arena, Mlx5Connection>) };
}

// CFBytes

#[inline]
#[no_mangle]
pub extern "C" fn CFBytes_new_in(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = CFBytes::<Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn CFBytes_new(
    ptr: *const ::std::os::raw::c_uchar,
    ptr_len: usize,
    datapath: *mut ::std::os::raw::c_void,
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) -> u32 {
    let arg0 = unsafe { std::slice::from_raw_parts(ptr, ptr_len) };
    let arg1 = datapath as *mut Mlx5Connection;
    let arg2 = arena as *const bumpalo::Bump;
    let value = CFBytes::<Mlx5Connection>::new(arg0, unsafe { &mut *arg1 }, unsafe { &*arg2 });
    let value = match value {
        Ok(value) => value,
        Err(_) => {
            return 1;
        }
    };
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn CFBytes_unpack(
    self_: *const ::std::os::raw::c_void,
    return_ptr: *mut *const ::std::os::raw::c_uchar,
    return_len_ptr: *mut usize,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut CFBytes<Mlx5Connection>) };
    let ptr = (*self_).as_ref();
    unsafe { *return_ptr = ptr.as_ptr() };
    unsafe { *return_len_ptr = self_.len() };
    Box::into_raw(self_);
}

// UnloggedReplyMessage

#[inline]
#[no_mangle]
pub extern "C" fn UnloggedReplyMessage_new_in<'arena>(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = UnloggedReplyMessage::<'arena, Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn UnloggedReplyMessage_get_clientreqid<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut UnloggedReplyMessage<'registered, Mlx5Connection>) };
    let value = self_.get_clientreqid();
    unsafe { *return_ptr = value };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn UnloggedReplyMessage_set_clientreqid<'registered>(self_: *mut ::std::os::raw::c_void, clientreqid: u64) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut UnloggedReplyMessage<'registered, Mlx5Connection>) };
    self_.set_clientreqid(clientreqid);
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn UnloggedReplyMessage_get_mut_reply<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut UnloggedReplyMessage<'registered, Mlx5Connection>) };
    let value: *mut Reply<'registered, Mlx5Connection> = self_.get_mut_reply();
    unsafe { *return_ptr = value as _ };
    Box::into_raw(self_);
}

#[inline]
#[no_mangle]
pub extern "C" fn UnloggedReplyMessage_set_reply<'registered>(
    self_: *mut ::std::os::raw::c_void,
    reply: *mut ::std::os::raw::c_void,
) {
    let mut self_ =
        unsafe { Box::from_raw(self_ as *mut UnloggedReplyMessage<'registered, Mlx5Connection>) };
    // Should do box from raw if the Reply was created with new
    let tapir_reply = unsafe { Box::from_raw(reply as *mut Reply<'registered, Mlx5Connection>) }; 
    self_.set_reply((*tapir_reply).clone());
    Box::into_raw(self_);
    Box::into_raw(tapir_reply);
}

#[inline]
#[no_mangle]
pub extern "C" fn UnloggedReplyMessage_deserialize<'arena>(
    self_: *mut ::std::os::raw::c_void,
    data: *const ::std::os::raw::c_void,
    data_len: usize,
    offset: usize,
    arena: *mut ::std::os::raw::c_void,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut UnloggedReplyMessage<'arena, Mlx5Connection>) };
    let data_slice = unsafe { std::slice::from_raw_parts(data as _, data_len as _) };
    let arg1 = offset;
    let arg2 = arena as *const bumpalo::Bump;
    let value = self_.deserialize_from_raw(data_slice, arg1, unsafe { &*arg2 });
    //let value = self_.deserialize(unsafe { &*arg0 }, arg1, unsafe { &*arg2 });
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
pub extern "C" fn Mlx5Connection_UnloggedReplyMessage_queue_cornflakes_arena_object<'arena>(
    self_: *mut ::std::os::raw::c_void,
    msg_id: u32,
    conn_id: usize,
    cornflakes_obj: *mut ::std::os::raw::c_void,
    end_batch: bool,
) -> u32 {
    let mut self_ = unsafe { Box::from_raw(self_ as *mut Mlx5Connection) };
    let arg0 = msg_id;
    let arg1 = conn_id;
    let arg2 = unsafe { *Box::from_raw(cornflakes_obj as *mut UnloggedReplyMessage<'arena, Mlx5Connection>) };
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
pub extern "C" fn UnloggedReplyMessage_free<'arena>(self_: *const ::std::os::raw::c_void) {
    let _ = unsafe { Box::from_raw(self_ as *mut UnloggedReplyMessage<'arena, Mlx5Connection>) };
}

// Reply

#[inline]
#[no_mangle]
pub extern "C" fn Reply_new_in<'arena>(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = Reply::<'arena, Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn Reply_get_mut_result<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let reply = self_ as *mut Reply<'registered, Mlx5Connection>;
    let value: *mut TapirReply<'registered, Mlx5Connection> = unsafe { (*reply).get_mut_result() };
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn Reply_get_result<'registered>(
    self_: *const ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let reply = self_ as *const Reply<'registered, Mlx5Connection>;
    let value: *const TapirReply<'registered, Mlx5Connection> = unsafe { (*reply).get_result() };
    unsafe { *return_ptr = value as _ };
}


#[inline]
#[no_mangle]
pub extern "C" fn Reply_free<'arena>(self_: *const ::std::os::raw::c_void) {
    let _ = unsafe { Box::from_raw(self_ as *mut Reply<'arena, Mlx5Connection>) };
}

// CFString

#[inline]
#[no_mangle]
pub extern "C" fn CFString_new_in(
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let arg0 = arena as *const bumpalo::Bump;
    let value = CFString::<Mlx5Connection>::new_in(unsafe { &*arg0 });
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn CFString_new(
    ptr: *const ::std::os::raw::c_uchar,
    ptr_len: usize,
    datapath: *mut ::std::os::raw::c_void,
    arena: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) -> u32 {
    let arg0 = unsafe { std::slice::from_raw_parts(ptr, ptr_len) };
    let arg1 = datapath as *mut Mlx5Connection;
    let arg2 = arena as *const bumpalo::Bump;
    let value = CFString::<Mlx5Connection>::new(arg0, unsafe { &mut *arg1 }, unsafe { &*arg2 });
    let value = match value {
        Ok(value) => value,
        Err(_) => {
            return 1;
        }
    };
    let value = Box::into_raw(Box::new(value));
    unsafe { *return_ptr = value as _ };
    0
}

#[inline]
#[no_mangle]
pub extern "C" fn CFString_unpack(
    self_: *const ::std::os::raw::c_void,
    return_ptr: *mut *const ::std::os::raw::c_uchar,
    return_len_ptr: *mut usize,
) {
    let self_ = unsafe { Box::from_raw(self_ as *mut CFString<Mlx5Connection>) };
    let ptr = (*self_).as_ref();
    unsafe { *return_ptr = ptr.as_ptr() };
    unsafe { *return_len_ptr = self_.len() };
    Box::into_raw(self_);
}

// TapirReply
#[inline]
#[no_mangle]
pub extern "C" fn TapirReply_get_mut_timestamp<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let reply = self_ as *mut TapirReply<'registered, Mlx5Connection>;
    let value: *mut TimestampMessage<'registered, Mlx5Connection> = unsafe { (*reply).get_mut_timestamp() };
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn TapirReply_get_status<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut i32,
) {
    let reply = self_ as *mut TapirReply<'registered, Mlx5Connection>;
    let value = unsafe { (*reply).get_status() };
    unsafe { *return_ptr = value };
}

#[inline]
#[no_mangle]
pub extern "C" fn TapirReply_set_status<'registered>(self_: *mut ::std::os::raw::c_void, status: i32) {
    let reply = self_ as *mut TapirReply<'registered, Mlx5Connection>;
    unsafe { (*reply).set_status(status)};
}

#[inline]
#[no_mangle]
pub extern "C" fn TapirReply_get_value<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut *mut ::std::os::raw::c_void,
) {
    let reply = self_ as *mut TapirReply<'registered, Mlx5Connection>;
    let value: *const CFString<Mlx5Connection> = unsafe { (*reply).get_value() };
    unsafe { *return_ptr = value as _ };
}

#[inline]
#[no_mangle]
pub extern "C" fn TapirReply_set_value<'registered>(
    self_: *mut ::std::os::raw::c_void,
    val: *const ::std::os::raw::c_void,
) {
    let reply = self_ as *mut TapirReply<'registered, Mlx5Connection>;
    let arg0 = unsafe { *Box::from_raw(val as *mut CFString<Mlx5Connection>) };
    unsafe { (*reply).set_value(arg0) };
}

// TimestampMesssage

#[inline]
#[no_mangle]
pub extern "C" fn TimestampMessage_get_id<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let ts_msg = self_ as *mut TimestampMessage<'registered, Mlx5Connection>;
    let value = unsafe { (*ts_msg).get_id() };
    unsafe { *return_ptr = value };
}

#[inline]
#[no_mangle]
pub extern "C" fn TimestampMessage_set_id<'registered>(self_: *mut ::std::os::raw::c_void, id: u64) {
    let ts_msg = self_ as *mut TimestampMessage<'registered, Mlx5Connection>;
    unsafe { (*ts_msg).set_id(id) };
}

#[inline]
#[no_mangle]
pub extern "C" fn TimestampMessage_get_timestamp<'registered>(
    self_: *mut ::std::os::raw::c_void,
    return_ptr: *mut u64,
) {
    let ts_msg = self_ as *mut TimestampMessage<'registered, Mlx5Connection>;
    let value = unsafe { (*ts_msg).get_id() };
    unsafe { *return_ptr = value };
}

#[inline]
#[no_mangle]
pub extern "C" fn TimestampMessage_set_timestamp<'registered>(self_: *mut ::std::os::raw::c_void, timestamp: u64) {
    let ts_msg = self_ as *mut TimestampMessage<'registered, Mlx5Connection>;
    unsafe { (*ts_msg).set_timestamp(timestamp) };
}
