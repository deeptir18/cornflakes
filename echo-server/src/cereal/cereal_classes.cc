#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/polymorphic.hpp"
#include <cereal/types/memory.hpp>
#include "cereal/archives/binary.hpp"
#include "rust/cxx.h"
#include "echo-server/src/cereal/include/cereal_headers.hh"
#include <stdio.h>
#include <streambuf>

class SingleCereal::impl {
    friend class cereal::access;
    friend class SingleCereal;
    impl() : data( std::string("") ) {}
    // TODO: possibly, string isn't the right move here
    std::string data;

    template<class Archive>
    void load(Archive & ar) {
        ar ( data );
    }

    template<class Archive>
    void save(Archive & ar) const {
        ar ( data );
    }
};

template <class Archive> inline
void SingleCereal::load(Archive & ar )  {
    ar( impl );
}

template <class Archive> inline
void SingleCereal::save(Archive & ar ) const {
    ar( impl );
}

SingleCereal::SingleCereal() : impl(new class SingleCereal::impl()) {}

SingleCereal::~SingleCereal() = default;

SingleCereal::SingleCereal(SingleCereal && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

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
    save(oarchive);
    return sstream.str().size();
}

void SingleCereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
}

bool SingleCereal::_equals(const std::unique_ptr<SingleCereal> other) const {
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

// We must also explicitly instantiate our template
// functions for serialization
template void SingleCereal::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void SingleCereal::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void SingleCereal::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void SingleCereal::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

std::unique_ptr<SingleCereal> deserialize_single_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    SingleCereal cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize SingleCereal.\n");
        e.what();
    }
    std::unique_ptr<SingleCereal> new_cereal = std::make_unique<SingleCereal>(std::move(cereal));
    return new_cereal;
}

std::unique_ptr<SingleCereal> new_single_cereal() {
    return std::make_unique<SingleCereal>();
}

class ListCereal::impl {
    friend ListCereal;
    std::vector<std::string> list_data;
};

ListCereal::ListCereal() : impl(new class ListCereal::impl) {}

ListCereal::ListCereal(ListCereal && other) : impl(nullptr) {
    impl = other.impl;
}

void ListCereal::append(rust::Slice<const uint8_t> data) const {
    impl->list_data.push_back(std::move(std::string(reinterpret_cast<const char *>(data.data()), data.length())));
}

// TODO: catch out of bounds exception
const std::string& ListCereal::get(size_t idx) const {
    return impl->list_data[idx];
}

void ListCereal::_set(size_t idx, rust::Slice<const uint8_t> data) const {
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
    ListCereal cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize ListCereal.\n");
        e.what();
    }
    return std::make_unique<ListCereal>(std::move(cereal));
}

class Tree1Cereal::impl {
    friend class cereal::access;
    friend class Tree1Cereal;
    std::shared_ptr<SingleCereal> left;
    std::shared_ptr<SingleCereal> right;

    impl() : left(nullptr), right(nullptr) {}

    template<class Archive>
    void load(Archive & ar) {
        ar ( left, right );
    }

    template<class Archive>
    void save(Archive & ar) const {
        ar ( left, right );
    }
};

template <class Archive> inline
void Tree1Cereal::load(Archive & ar )  {
    ar( impl );
}

template <class Archive> inline
void Tree1Cereal::save(Archive & ar ) const {
    ar( impl );
}

Tree1Cereal::Tree1Cereal() : impl(new class Tree1Cereal::impl()) {}

Tree1Cereal::~Tree1Cereal() = default;

Tree1Cereal::Tree1Cereal(Tree1Cereal && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

std::shared_ptr<SingleCereal> Tree1Cereal::get_left() const {
    return impl->left;
}

void Tree1Cereal::set_left(std::unique_ptr<SingleCereal> left) const {
    impl->left = std::shared_ptr<SingleCereal>(std::move(left));
}

std::shared_ptr<SingleCereal> Tree1Cereal::get_right() const {
    return impl->right;
}

void Tree1Cereal::set_right(std::unique_ptr<SingleCereal> right) const {
    impl->right = std::shared_ptr<SingleCereal>(std::move(right));
}

size_t Tree1Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    save(oarchive);
    return sstream.str().size();
}

void Tree1Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
}

// We must also explicitly instantiate our template
// functions for serialization
template void Tree1Cereal::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree1Cereal::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void Tree1Cereal::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree1Cereal::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

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

std::unique_ptr<Tree1Cereal> new_tree1_cereal() {
    return std::make_unique<Tree1Cereal>();
}

std::unique_ptr<Tree1Cereal> deserialize_tree1_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    Tree1Cereal cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree1Cereal.\n");
        e.what();
    }
    return std::make_unique<Tree1Cereal>(std::move(cereal));
}

std::unique_ptr<Tree1Cereal> reserialize_tree1(std::unique_ptr<Tree1Cereal> input) {
    std::unique_ptr<Tree1Cereal> output = std::make_unique<Tree1Cereal>();
    std::unique_ptr<SingleCereal> left = std::make_unique<SingleCereal>();
    std::unique_ptr<SingleCereal> right = std::make_unique<SingleCereal>();

    const std::string& left_data = input->get_left()->get_data();
    const std::string& right_data = input->get_right()->get_data();

    left->set_data(left_data);
    right->set_data(right_data);

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree1Cereal> reserialize_tree1(std::shared_ptr<Tree1Cereal> input) {
    std::unique_ptr<Tree1Cereal> output = std::make_unique<Tree1Cereal>();
    std::unique_ptr<SingleCereal> left = std::make_unique<SingleCereal>();
    std::unique_ptr<SingleCereal> right = std::make_unique<SingleCereal>();

    const std::string& left_data = input->get_left()->get_data();
    const std::string& right_data = input->get_right()->get_data();

    left->set_data(left_data);
    right->set_data(right_data);

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}
class Tree2Cereal::impl {
    friend class cereal::access;
    friend Tree2Cereal;
    std::shared_ptr<Tree1Cereal> left;
    std::shared_ptr<Tree1Cereal> right;

    impl() : left(nullptr), right(nullptr) {}

    template <class Archive>
    void load(Archive & ar) {
        ar(left, right);
    }

    template <class Archive>
    void save(Archive &ar) const {
        ar(left, right);
    }
};

template <class Archive>
void Tree2Cereal::load(Archive &ar) {
    ar(impl);
}

template <class Archive>
void Tree2Cereal::save(Archive &ar) const {
    ar(impl);
}

Tree2Cereal::Tree2Cereal() : impl(new class Tree2Cereal::impl()) {}

Tree2Cereal::~Tree2Cereal() = default;

Tree2Cereal::Tree2Cereal(Tree2Cereal && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

std::shared_ptr<Tree1Cereal> Tree2Cereal::get_left() const {
    return impl->left;
}

void Tree2Cereal::set_left(std::unique_ptr<Tree1Cereal> left) const {
    impl->left = std::shared_ptr<Tree1Cereal>(std::move(left));
}

std::shared_ptr<Tree1Cereal> Tree2Cereal::get_right() const {
    return impl->right;
}

void Tree2Cereal::set_right(std::unique_ptr<Tree1Cereal> right) const {
    impl->right = std::shared_ptr<Tree1Cereal>(std::move(right));
}

size_t Tree2Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    save(oarchive);
    return sstream.str().size();
}

void Tree2Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
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

// We must also explicitly instantiate our template
// functions for serialization
template void Tree2Cereal::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree2Cereal::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void Tree2Cereal::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree2Cereal::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

std::unique_ptr<Tree2Cereal> new_tree2_cereal() {
    return std::make_unique<Tree2Cereal>();
}

std::unique_ptr<Tree2Cereal> deserialize_tree2_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    Tree2Cereal cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree2Cereal.\n");
        e.what();
    }
    return std::make_unique<Tree2Cereal>(std::move(cereal));
}

std::unique_ptr<Tree2Cereal> reserialize_tree2(std::unique_ptr<Tree2Cereal> input) {
    std::unique_ptr<Tree2Cereal> output = std::make_unique<Tree2Cereal>();
    std::unique_ptr<Tree1Cereal> left = reserialize_tree1(input->get_left());
    std::unique_ptr<Tree1Cereal> right = reserialize_tree1(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree2Cereal> reserialize_tree2(std::shared_ptr<Tree2Cereal> input) {
    std::unique_ptr<Tree2Cereal> output = std::make_unique<Tree2Cereal>();
    std::unique_ptr<Tree1Cereal> left = reserialize_tree1(input->get_left());
    std::unique_ptr<Tree1Cereal> right = reserialize_tree1(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

class Tree3Cereal::impl {
    friend class cereal::access;
    friend Tree3Cereal;
    std::shared_ptr<Tree2Cereal> left;
    std::shared_ptr<Tree2Cereal> right;

    impl(): left(nullptr), right(nullptr) {}

    template <class Archive>
    void load(Archive & ar) {
        ar(left, right);
    }

    template <class Archive>
    void save(Archive &ar) const {
        ar(left, right);
    }
};

template <class Archive> inline
void Tree3Cereal::load(Archive & ar )  {
    ar( impl );
}

template <class Archive> inline
void Tree3Cereal::save(Archive & ar ) const {
    ar( impl );
}

Tree3Cereal::Tree3Cereal() : impl(new class Tree3Cereal::impl()) {}

Tree3Cereal::~Tree3Cereal() = default;

Tree3Cereal::Tree3Cereal(Tree3Cereal && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

std::shared_ptr<Tree2Cereal> Tree3Cereal::get_left() const {
    return impl->left;
}

void Tree3Cereal::set_left(std::unique_ptr<Tree2Cereal> left) const {
    impl->left = std::shared_ptr<Tree2Cereal>(std::move(left));
}

std::shared_ptr<Tree2Cereal> Tree3Cereal::get_right() const {
    return impl->right;
}

void Tree3Cereal::set_right(std::unique_ptr<Tree2Cereal> right) const {
    impl->right = std::shared_ptr<Tree2Cereal>(std::move(right));
}

size_t Tree3Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    save(oarchive);
    return sstream.str().size();
}

void Tree3Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
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

// We must also explicitly instantiate our template
// functions for serialization
template void Tree3Cereal::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree3Cereal::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void Tree3Cereal::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree3Cereal::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

std::unique_ptr<Tree3Cereal> new_tree3_cereal() {
    return std::make_unique<Tree3Cereal>();
}

std::unique_ptr<Tree3Cereal> deserialize_tree3_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    Tree3Cereal cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree3Cereal.\n");
        e.what();
    }
    return std::make_unique<Tree3Cereal>(std::move(cereal));
}

std::unique_ptr<Tree3Cereal> reserialize_tree3(std::unique_ptr<Tree3Cereal> input) {
    std::unique_ptr<Tree3Cereal> output = std::make_unique<Tree3Cereal>();
    std::unique_ptr<Tree2Cereal> left = reserialize_tree2(input->get_left());
    std::unique_ptr<Tree2Cereal> right = reserialize_tree2(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree3Cereal> reserialize_tree3(std::shared_ptr<Tree3Cereal> input) {
    std::unique_ptr<Tree3Cereal> output = std::make_unique<Tree3Cereal>();
    std::unique_ptr<Tree2Cereal> left = reserialize_tree2(input->get_left());
    std::unique_ptr<Tree2Cereal> right = reserialize_tree2(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

class Tree4Cereal::impl {
    friend Tree4Cereal;
    friend class cereal::access;
    std::shared_ptr<Tree3Cereal> left;
    std::shared_ptr<Tree3Cereal> right;
        
    template <class Archive>
    void load(Archive & ar) {
        ar( left, right );
    }
    
    template <class Archive>
    void save(Archive & ar) const {
        ar( left, right );
    }
};

template <class Archive>
void Tree4Cereal::load(Archive & ar) {
    ar( impl );
}
    
template <class Archive>
void Tree4Cereal::save(Archive & ar) const {
    ar( impl );
}

Tree4Cereal::Tree4Cereal() : impl(new class Tree4Cereal::impl()) {}

Tree4Cereal::~Tree4Cereal() = default;

Tree4Cereal::Tree4Cereal(Tree4Cereal && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

std::shared_ptr<Tree3Cereal> Tree4Cereal::get_left() const {
    return impl->left;
}

void Tree4Cereal::set_left(std::unique_ptr<Tree3Cereal> left) const {
    impl->left = std::shared_ptr<Tree3Cereal>(std::move(left));
}

std::shared_ptr<Tree3Cereal> Tree4Cereal::get_right() const {
    return impl->right;
}

void Tree4Cereal::set_right(std::unique_ptr<Tree3Cereal> right) const {
    impl->right = std::shared_ptr<Tree3Cereal>(std::move(right));
}

size_t Tree4Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    save(oarchive);
    return sstream.str().size();
}

void Tree4Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
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

// We must also explicitly instantiate our template
// functions for serialization
template void Tree4Cereal::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree4Cereal::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void Tree4Cereal::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree4Cereal::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

std::unique_ptr<Tree4Cereal> new_tree4_cereal() {
    return std::make_unique<Tree4Cereal>();
}

std::unique_ptr<Tree4Cereal> deserialize_tree4_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    Tree4Cereal cereal;
    try {
        iarchive(cereal);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree4Cereal.\n");
        e.what();
    }
    return std::make_unique<Tree4Cereal>(std::move(cereal));
}

std::unique_ptr<Tree4Cereal> reserialize_tree4(std::unique_ptr<Tree4Cereal> input) {
    std::unique_ptr<Tree4Cereal> output = std::make_unique<Tree4Cereal>();
    std::unique_ptr<Tree3Cereal> left = reserialize_tree3(input->get_left());
    std::unique_ptr<Tree3Cereal> right = reserialize_tree3(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree4Cereal> reserialize_tree4(std::shared_ptr<Tree4Cereal> input) {
    std::unique_ptr<Tree4Cereal> output = std::make_unique<Tree4Cereal>();
    std::unique_ptr<Tree3Cereal> left = reserialize_tree3(input->get_left());
    std::unique_ptr<Tree3Cereal> right = reserialize_tree3(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

class Tree5Cereal::impl {
    friend Tree5Cereal;
    friend class cereal::access;
    std::shared_ptr<Tree4Cereal> left;
    std::shared_ptr<Tree4Cereal> right;

    impl(): left(nullptr), right(nullptr) {}

    template <class Archive>
    void load(Archive &ar) {
        ar(left, right);
    }

    template <class Archive>
    void save(Archive &ar) const {
        ar(left, right);
    }
};

template <class Archive>
void Tree5Cereal::load(Archive &ar) {
    ar(impl);
}

template <class Archive>
void Tree5Cereal::save(Archive &ar) const {
    ar(impl);
}

Tree5Cereal::Tree5Cereal() : impl(new class Tree5Cereal::impl()) {}

Tree5Cereal::~Tree5Cereal() = default;

Tree5Cereal::Tree5Cereal(Tree5Cereal && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

std::shared_ptr<Tree4Cereal> Tree5Cereal::get_left() const {
    return impl->left;
}

void Tree5Cereal::set_left(std::unique_ptr<Tree4Cereal> left) const {
    impl->left = std::shared_ptr<Tree4Cereal>(std::move(left));
}

std::shared_ptr<Tree4Cereal> Tree5Cereal::get_right() const {
    return impl->right;
}

void Tree5Cereal::set_right(std::unique_ptr<Tree4Cereal> right) const {
    impl->right = std::shared_ptr<Tree4Cereal>(std::move(right));
}

size_t Tree5Cereal::serialized_size() const {
    std::stringstream sstream(std::ios_base::out);
    cereal::BinaryOutputArchive oarchive(sstream);
    save(oarchive);
    return sstream.str().size();
}

void Tree5Cereal::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
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

std::unique_ptr<Tree5Cereal> new_tree5_cereal() {
    return std::make_unique<Tree5Cereal>();
}

std::unique_ptr<Tree5Cereal> deserialize_tree5_cereal_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    Tree5Cereal cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize Tree5Cereal.\n");
        e.what();
    }
    return std::make_unique<Tree5Cereal>(std::move(cereal));
}

std::unique_ptr<Tree5Cereal> reserialize_tree5(std::unique_ptr<Tree5Cereal> input) {
    std::unique_ptr<Tree5Cereal> output = std::make_unique<Tree5Cereal>();
    std::unique_ptr<Tree4Cereal> left = reserialize_tree4(input->get_left());
    std::unique_ptr<Tree4Cereal> right = reserialize_tree4(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

std::unique_ptr<Tree5Cereal> reserialize_tree5(std::shared_ptr<Tree5Cereal> input) {
    std::unique_ptr<Tree5Cereal> output = std::make_unique<Tree5Cereal>();
    std::unique_ptr<Tree4Cereal> left = reserialize_tree4(input->get_left());
    std::unique_ptr<Tree4Cereal> right = reserialize_tree4(input->get_right());

    output->set_left(std::move(left));
    output->set_right(std::move(right));
    return output;
}

// We must also explicitly instantiate our template
// functions for serialization
template void Tree5Cereal::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree5Cereal::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void Tree5Cereal::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void Tree5Cereal::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

