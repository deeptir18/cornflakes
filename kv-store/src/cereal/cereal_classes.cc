#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/polymorphic.hpp"
#include <cereal/types/memory.hpp>
#include "cereal/archives/binary.hpp"
#include "rust/cxx.h"
#include "kv-store/src/cereal/include/cereal_headers.hh"
#include <stdio.h>
#include <streambuf>

class GetRequest::impl {
    friend class cereal::access;
    friend class GetRequest;
    impl() : 
        id(0),
        key(std::string(""))
    {}

    uint32_t id;
    std::string key;

    template <class Archive>
    void load(Archive &ar) {
        ar( id, key );
    }

    template<class Archive>
    void save(Archive & ar) const {
        ar ( id, key );
    }
};

template <class Archive> inline
void GetRequest::load(Archive & ar )  {
    ar( impl );
}

template <class Archive> inline
void GetRequest::save(Archive & ar ) const {
    ar( impl );
}

GetRequest::GetRequest() : impl(new class GetRequest::impl()) {}

GetRequest::~GetRequest() = default;

GetRequest::GetRequest(GetRequest && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

void GetRequest::set_id(uint32_t id) const {
    impl->id = id;
}

uint32_t GetRequest::get_id() const {
    return impl->id;
}

void GetRequest::set_key(rust::Slice<const uint8_t> key) const {
    impl->key = std::move(std::string(reinterpret_cast<const char *>(key.data()), key.length()));
}

const std::string& GetRequest::get_key() const {
    return impl->key;
}

size_t GetRequest::serialized_size() const {
    COUNTER_BUFFER buffer;
    std::basic_ostream<char> sstream(&buffer);
    cereal::BinaryOutputArchive Archive(sstream);
    return buffer.size();
}

void GetRequest::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
}

// We must also explicitly instantiate our template
// functions for serialization
template void GetRequest::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void GetRequest::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void GetRequest::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void GetRequest::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

std::unique_ptr<GetRequest> deserialize_get_request_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    GetRequest cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize GetRequest.\n");
        e.what();
    }
    std::unique_ptr<GetRequest> new_cereal = std::make_unique<GetRequest>(std::move(cereal));
    return new_cereal;
}

std::unique_ptr<GetRequest> new_get_request() {
    return std::make_unique<GetRequest>();
}

class GetResponse::impl {
    friend class cereal::access;
    friend class GetResponse;
    impl() : 
        id(0),
        value(std::string(""))
    {}

    uint32_t id;
    std::string value;

    template <class Archive>
    void load(Archive &ar) {
        ar( id, value );
    }

    template<class Archive>
    void save(Archive & ar) const {
        ar ( id, value );
    }
};

template <class Archive> inline
void GetResponse::load(Archive & ar )  {
    ar( impl );
}

template <class Archive> inline
void GetResponse::save(Archive & ar ) const {
    ar( impl );
}

GetResponse::GetResponse() : impl(new class GetResponse::impl()) {}

GetResponse::~GetResponse() = default;

GetResponse::GetResponse(GetResponse && other) : impl(nullptr) {
    impl = std::move(other.impl);
}

void GetResponse::set_id(uint32_t id) const {
    impl->id = id;
}

uint32_t GetResponse::get_id() const {
    return impl->id;
}

void GetResponse::set_value(rust::Slice<const uint8_t> value) const {
    impl->value = std::move(std::string(reinterpret_cast<const char *>(value.data()),value.length()));
}

const std::string& GetResponse::get_value() const {
    return impl->value;
}

size_t GetResponse::serialized_size() const {
    COUNTER_BUFFER buffer;
    std::basic_ostream<char> sstream(&buffer);
    cereal::BinaryOutputArchive Archive(sstream);
    return buffer.size();
}

void GetResponse::serialize_to_array(rust::Slice<uint8_t> buf) const {
    membuf stream_buffer = membuf(buf);
    std::basic_ostream<char> stream(&stream_buffer);
    cereal::BinaryOutputArchive oarchive(stream);
    save(oarchive);
}

// We must also explicitly instantiate our template
// functions for serialization
template void GetResponse::impl::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void GetResponse::impl::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

// Note that we need to instantiate for both loading and saving, even
// if we use a single serialize function
template void GetResponse::save<cereal::BinaryOutputArchive>( cereal::BinaryOutputArchive & ) const;
template void GetResponse::load<cereal::BinaryInputArchive>( cereal::BinaryInputArchive & );

std::unique_ptr<GetResponse> deserialize_get_response_from_array(rust::Slice<const uint8_t> buf) {
    membuf stream_buffer = membuf(buf);
    std::basic_istream<char> stream(&stream_buffer);
    cereal::BinaryInputArchive iarchive(stream);
    GetResponse cereal;
    try {
        cereal.load(iarchive);
    }
    catch(std::runtime_error e) {
        printf("Failed to deserialize GetResponse.\n");
        e.what();
    }
    std::unique_ptr<GetResponse> new_cereal = std::make_unique<GetResponse>(std::move(cereal));
    return new_cereal;
}

std::unique_ptr<GetResponse> new_get_response() {
    return std::make_unique<GetResponse>();
}


