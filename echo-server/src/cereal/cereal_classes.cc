#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/archives/binary.hpp"
#include "echo-server/src/cereal/include/cxx.h"
#include "echo-server/src/cereal/include/cereal_headers.hh"
#include <streambuf>

class SingleCereal::impl {
    friend SingleCereal;
    // TODO: possibly, string isn't the right move here
    std::string data;
};

SingleCereal::SingleCereal() : impl(new class SingleCereal::impl) {}

void SingleCereal::set_data(rust::Slice<const uint8_t> data) const {
    impl->data = std::move(std::string(reinterpret_cast<const char *>(data.data()), data.length()));
}

const std::string& SingleCereal::get_data() const {
    return impl->data;
}

void SingleCereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(reinterpret_cast<char *>(buf.data()), reinterpret_cast<char *>(buf.data()) + buf.length());
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

template <class Archive>
void SingleCereal::serialize(Archive & ar ) const {
    ar( impl->data );
}

std::unique_ptr<SingleCereal> new_single_cereal() {
    return std::make_unique<SingleCereal>();
}

class ListCereal::impl {
    friend ListCereal;
    std::vector<std::string> list_data;
};

ListCereal::ListCereal() : impl(new class ListCereal::impl) {}

void ListCereal::append_string(rust::Slice<const uint8_t> data) const {
    impl->list_data.push_back(std::move(std::string(reinterpret_cast<const char *>(data.data()), data.length())));
}

// TODO: catch out of bounds exception
const std::string& ListCereal::get(size_t idx) const {
    return impl->list_data[idx];
}

void ListCereal::set(size_t idx, rust::Slice<const uint8_t> data) const {
    impl->list_data[idx] = std::move(std::string(reinterpret_cast<const char *>(data.data()), data.length()));
}

void ListCereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(reinterpret_cast<char *>(buf.data()), reinterpret_cast<char *>(buf.data()) + buf.length());
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

template <class Archive>
void ListCereal::serialize(Archive & ar ) const {
    ar( impl->list_data );
}

std::unique_ptr<ListCereal> new_list_cereal() {
    return std::make_unique<ListCereal>();
}

class Tree1Cereal::impl {
    friend Tree1Cereal;
    std::unique_ptr<SingleCereal> left;
    std::unique_ptr<SingleCereal> right;
};

Tree1Cereal::Tree1Cereal() : impl(new class Tree1Cereal::impl) {}

void Tree1Cereal::set_left(std::unique_ptr<SingleCereal> left) const {
    impl->left = std::move(left);
}

void Tree1Cereal::set_right(std::unique_ptr<SingleCereal> right) const {
    impl->right = std::move(right);
}

