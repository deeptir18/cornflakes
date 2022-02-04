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

mod allocator;
pub mod connection; // implements datapath trait
mod wrapper; // wrapper to driver code // allocator for mempools
