pub mod cf_dynamic;
use bumpalo;
use cornflakes_libos::ArenaOrderedSga;
use cf_dynamic::tapir_serializer::*;

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


