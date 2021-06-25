use cxx;
#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("echo-server/src/cereal/include/cereal_headers.hh");
        type SingleCereal;
        type ListCereal;
        type Tree1Cereal;

        fn set_data(self: &SingleCereal, value: &[u8]);
        fn get_data(self: &SingleCereal) -> &CxxString;
        fn serialize_to_array(self: &SingleCereal, buf: &mut [u8]);
        fn new_single_cereal() -> UniquePtr<SingleCereal>;

        fn append_string(self: &ListCereal, value: &[u8]);
        fn get(self: &ListCereal, idx: usize) -> &CxxString;
        fn set(self: &ListCereal, idx: usize, value: &[u8]);
        fn serialize_to_array(self: &ListCereal, buf: &mut [u8]);
        fn new_list_cereal() -> UniquePtr<ListCereal>;

        fn set_left(self: &Tree1Cereal, left: UniquePtr<SingleCereal>);
        fn set_right(self: &Tree1Cereal, left: UniquePtr<SingleCereal>);

    }
}
