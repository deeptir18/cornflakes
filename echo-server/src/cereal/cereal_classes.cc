#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/archives/binary.hpp"
#include "rust/cxx.h"
#include "echo-server/src/cereal/include/cereal_headers.hh"
#include <stdio.h>
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

void SingleCereal::set_data(const std::string& data) const {
    impl->data = std::move(std::string(data));
}

const std::string& SingleCereal::get_data() const {
    return impl->data;
}

size_t SingleCereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void SingleCereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    printf("In serialize to array function, address of array is : %p\n", reinterpret_cast<char *>(buf.data()));
    char *data_ptr = reinterpret_cast<char *>(buf.data());
    *(data_ptr) = 'f';
    membuf stream_buffer_copy = membuf(buf);
    stream_buffer_copy.sputc('m');
    stream_buffer_copy.sputc('e');
    stream_buffer_copy.sputc('e');
    printf("1st: %c, 2nd: %c, third: %c\n", *data_ptr, *(data_ptr +1), *(data_ptr + 2));
    std::string original_foo = std::string(reinterpret_cast<char *>(buf.data()), 3);
    
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    stream.write("baa", 3);
    stream.flush();
    std::string foo = std::string(reinterpret_cast<char *>(buf.data()), 3);
    printf("Wrote to string: original: %s, new: %s\n", original_foo.c_str(), foo.c_str());
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

bool SingleCereal::equals(const std::unique_ptr<SingleCereal> other) const {
    if (impl->data.compare(other->get_data()) == 0) {
        return true;
    }
    return false;
}

bool SingleCereal::equals(const std::shared_ptr<SingleCereal> other) const {
    if (impl->data.compare(other->get_data()) == 0) {
        return true;
    }
    return false;
}

std::unique_ptr<SingleCereal> deserialize_single_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<SingleCereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize SingleCereal.\n");
        e.what();
    }
    return cereal;
}

template <class Archive>
void SingleCereal::serialize(Archive & ar ) const {
    printf("In serialize func\n");
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

void ListCereal::append(rust::Slice<const uint8_t> data) const {
    impl->list_data.push_back(std::move(std::string(reinterpret_cast<const char *>(data.data()), data.length())));
}

// TODO: catch out of bounds exception
const std::string& ListCereal::get(size_t idx) const {
    return impl->list_data[idx];
}

void ListCereal::set(size_t idx, rust::Slice<const uint8_t> data) const {
    impl->list_data[idx] = std::move(std::string(reinterpret_cast<const char *>(data.data()), data.length()));
}

size_t ListCereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void ListCereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
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

std::unique_ptr<ListCereal> deserialize_list_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<ListCereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree1Cereal.\n");
        e.what();
    }
    return cereal;
}

class Tree1Cereal::impl {
    friend Tree1Cereal;
    std::shared_ptr<SingleCereal> left;
    std::shared_ptr<SingleCereal> right;
};

Tree1Cereal::Tree1Cereal() : impl(new class Tree1Cereal::impl) {}

std::shared_ptr<SingleCereal> Tree1Cereal::get_left() const {
    return impl->left;
}

void Tree1Cereal::set_left(std::unique_ptr<SingleCereal> left) const {
    impl->left = std::shared_ptr<SingleCereal>(left.get());
}

std::shared_ptr<SingleCereal> Tree1Cereal::get_right() const {
    return impl->right;
}

void Tree1Cereal::set_right(std::unique_ptr<SingleCereal> right) const {
    impl->right = std::shared_ptr<SingleCereal>(right.get());
}

size_t Tree1Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void Tree1Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

bool Tree1Cereal::equals(std::unique_ptr<Tree1Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

bool Tree1Cereal::equals(std::shared_ptr<Tree1Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

template <class Archive>
void Tree1Cereal::serialize(Archive & ar ) const {
    ar(impl->left, impl->right);
}

std::unique_ptr<Tree1Cereal> new_tree1_cereal() {
    return std::make_unique<Tree1Cereal>();
}

std::unique_ptr<Tree1Cereal> deserialize_tree1_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<Tree1Cereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree1Cereal.\n");
        e.what();
    }
    return cereal;
}

std::unique_ptr<Tree1Cereal> reserialize_tree1(std::unique_ptr<Tree1Cereal> input) {
    std::unique_ptr<Tree1Cereal> output;
    std::unique_ptr<SingleCereal> left;
    std::unique_ptr<SingleCereal> right;

    const std::string& left_data = input->get_left()->get_data();
    const std::string& right_data = input->get_right()->get_data();

    left->set_data(left_data);
    right->set_data(right_data);

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree1Cereal> reserialize_tree1(std::shared_ptr<Tree1Cereal> input) {
    std::unique_ptr<Tree1Cereal> output;
    std::unique_ptr<SingleCereal> left;
    std::unique_ptr<SingleCereal> right;

    const std::string& left_data = input->get_left()->get_data();
    const std::string& right_data = input->get_right()->get_data();

    left->set_data(left_data);
    right->set_data(right_data);

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}
class Tree2Cereal::impl {
    friend Tree2Cereal;
    std::shared_ptr<Tree1Cereal> left;
    std::shared_ptr<Tree1Cereal> right;
};

Tree2Cereal::Tree2Cereal() : impl(new class Tree2Cereal::impl) {}

std::shared_ptr<Tree1Cereal> Tree2Cereal::get_left() const {
    return impl->left;
}

void Tree2Cereal::set_left(std::unique_ptr<Tree1Cereal> left) const {
    impl->left = std::shared_ptr<Tree1Cereal>(left.get());
}

std::shared_ptr<Tree1Cereal> Tree2Cereal::get_right() const {
    return impl->right;
}

void Tree2Cereal::set_right(std::unique_ptr<Tree1Cereal> right) const {
    impl->right = std::shared_ptr<Tree1Cereal>(right.get());
}

size_t Tree2Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void Tree2Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

bool Tree2Cereal::equals(std::unique_ptr<Tree2Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

bool Tree2Cereal::equals(std::shared_ptr<Tree2Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

template <class Archive>
void Tree2Cereal::serialize(Archive & ar ) const {
    ar(impl->left, impl->right);
}

std::unique_ptr<Tree2Cereal> new_tree2_cereal() {
    return std::make_unique<Tree2Cereal>();
}

std::unique_ptr<Tree2Cereal> deserialize_tree2_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<Tree2Cereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree2Cereal.\n");
        e.what();
    }
    return cereal;
}

std::unique_ptr<Tree2Cereal> reserialize_tree2(std::unique_ptr<Tree2Cereal> input) {
    std::unique_ptr<Tree2Cereal> output;
    std::unique_ptr<Tree1Cereal> left = reserialize_tree1(input->get_left());
    std::unique_ptr<Tree1Cereal> right = reserialize_tree1(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree2Cereal> reserialize_tree2(std::shared_ptr<Tree2Cereal> input) {
    std::unique_ptr<Tree2Cereal> output;
    std::unique_ptr<Tree1Cereal> left = reserialize_tree1(input->get_left());
    std::unique_ptr<Tree1Cereal> right = reserialize_tree1(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

class Tree3Cereal::impl {
    friend Tree3Cereal;
    std::shared_ptr<Tree2Cereal> left;
    std::shared_ptr<Tree2Cereal> right;
};

Tree3Cereal::Tree3Cereal() : impl(new class Tree3Cereal::impl) {}

std::shared_ptr<Tree2Cereal> Tree3Cereal::get_left() const {
    return impl->left;
}

void Tree3Cereal::set_left(std::unique_ptr<Tree2Cereal> left) const {
    impl->left = std::shared_ptr<Tree2Cereal>(left.get());
}

std::shared_ptr<Tree2Cereal> Tree3Cereal::get_right() const {
    return impl->right;
}

void Tree3Cereal::set_right(std::unique_ptr<Tree2Cereal> right) const {
    impl->right = std::shared_ptr<Tree2Cereal>(right.get());
}

size_t Tree3Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void Tree3Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

bool Tree3Cereal::equals(std::unique_ptr<Tree3Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

bool Tree3Cereal::equals(std::shared_ptr<Tree3Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

template <class Archive>
void Tree3Cereal::serialize(Archive & ar ) const {
    ar(impl->left, impl->right);
}

std::unique_ptr<Tree3Cereal> new_tree3_cereal() {
    return std::make_unique<Tree3Cereal>();
}

std::unique_ptr<Tree3Cereal> deserialize_tree3_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<Tree3Cereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree3Cereal.\n");
        e.what();
    }
    return cereal;
}

std::unique_ptr<Tree3Cereal> reserialize_tree3(std::unique_ptr<Tree3Cereal> input) {
    std::unique_ptr<Tree3Cereal> output;
    std::unique_ptr<Tree2Cereal> left = reserialize_tree2(input->get_left());
    std::unique_ptr<Tree2Cereal> right = reserialize_tree2(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree3Cereal> reserialize_tree3(std::shared_ptr<Tree3Cereal> input) {
    std::unique_ptr<Tree3Cereal> output;
    std::unique_ptr<Tree2Cereal> left = reserialize_tree2(input->get_left());
    std::unique_ptr<Tree2Cereal> right = reserialize_tree2(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

class Tree4Cereal::impl {
    friend Tree4Cereal;
    std::shared_ptr<Tree3Cereal> left;
    std::shared_ptr<Tree3Cereal> right;
};

Tree4Cereal::Tree4Cereal() : impl(new class Tree4Cereal::impl) {}

std::shared_ptr<Tree3Cereal> Tree4Cereal::get_left() const {
    return impl->left;
}

void Tree4Cereal::set_left(std::unique_ptr<Tree3Cereal> left) const {
    impl->left = std::shared_ptr<Tree3Cereal>(left.get());
}

std::shared_ptr<Tree3Cereal> Tree4Cereal::get_right() const {
    return impl->right;
}

void Tree4Cereal::set_right(std::unique_ptr<Tree3Cereal> right) const {
    impl->right = std::shared_ptr<Tree3Cereal>(right.get());
}

size_t Tree4Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void Tree4Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

bool Tree4Cereal::equals(std::unique_ptr<Tree4Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

bool Tree4Cereal::equals(std::shared_ptr<Tree4Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

template <class Archive>
void Tree4Cereal::serialize(Archive & ar ) const {
    ar(impl->left, impl->right);
}

std::unique_ptr<Tree4Cereal> new_tree4_cereal() {
    return std::make_unique<Tree4Cereal>();
}

std::unique_ptr<Tree4Cereal> deserialize_tree4_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<Tree4Cereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree4Cereal.\n");
        e.what();
    }
    return cereal;
}

std::unique_ptr<Tree4Cereal> reserialize_tree4(std::unique_ptr<Tree4Cereal> input) {
    std::unique_ptr<Tree4Cereal> output;
    std::unique_ptr<Tree3Cereal> left = reserialize_tree3(input->get_left());
    std::unique_ptr<Tree3Cereal> right = reserialize_tree3(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree4Cereal> reserialize_tree4(std::shared_ptr<Tree4Cereal> input) {
    std::unique_ptr<Tree4Cereal> output;
    std::unique_ptr<Tree3Cereal> left = reserialize_tree3(input->get_left());
    std::unique_ptr<Tree3Cereal> right = reserialize_tree3(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

class Tree5Cereal::impl {
    friend Tree5Cereal;
    std::shared_ptr<Tree4Cereal> left;
    std::shared_ptr<Tree4Cereal> right;
};

Tree5Cereal::Tree5Cereal() : impl(new class Tree5Cereal::impl) {}

std::shared_ptr<Tree4Cereal> Tree5Cereal::get_left() const {
    return impl->left;
}

void Tree5Cereal::set_left(std::unique_ptr<Tree4Cereal> left) const {
    impl->left = std::shared_ptr<Tree4Cereal>(left.get());
}

std::shared_ptr<Tree4Cereal> Tree5Cereal::get_right() const {
    return impl->right;
}

void Tree5Cereal::set_right(std::unique_ptr<Tree4Cereal> right) const {
    impl->right = std::shared_ptr<Tree4Cereal>(right.get());
}

size_t Tree5Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    serialize(oarchive);
    return sstream.str().size();
}

void Tree5Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    serialize(oarchive);
}

bool Tree5Cereal::equals(std::unique_ptr<Tree5Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

bool Tree5Cereal::equals(std::shared_ptr<Tree5Cereal> other) const {
    if ((!impl->left->equals(other->get_left())) || (!impl->right->equals(other->get_right()))) {
            return false;
        }
        return true;
}

template <class Archive>
void Tree5Cereal::serialize(Archive & ar ) const {
    ar(impl->left, impl->right);
}

std::unique_ptr<Tree5Cereal> new_tree5_cereal() {
    return std::make_unique<Tree5Cereal>();
}

std::unique_ptr<Tree5Cereal> deserialize_tree5_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    std::unique_ptr<Tree5Cereal> cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree5Cereal.\n");
        e.what();
    }
    return cereal;
}

std::unique_ptr<Tree5Cereal> reserialize_tree5(std::unique_ptr<Tree5Cereal> input) {
    std::unique_ptr<Tree5Cereal> output;
    std::unique_ptr<Tree4Cereal> left = reserialize_tree4(input->get_left());
    std::unique_ptr<Tree4Cereal> right = reserialize_tree4(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree5Cereal> reserialize_tree5(std::shared_ptr<Tree5Cereal> input) {
    std::unique_ptr<Tree5Cereal> output;
    std::unique_ptr<Tree4Cereal> left = reserialize_tree4(input->get_left());
    std::unique_ptr<Tree4Cereal> right = reserialize_tree4(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}
