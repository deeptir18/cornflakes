#pragma once
#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/polymorphic.hpp"
#include <memory>
#include <vector>
#include "rust/cxx.h"
#include <streambuf>
#include <cereal/access.hpp>

class COUNTER_BUFFER : public std::streambuf
{
    private :
        size_t mSize = 0;

    private :
        // TODO: this can potentially overflow?
        int_type overflow(int_type __attribute__((unused)) C)
        {
            return mSize++;
        }

    public :
        size_t size(void) const
        {
            return mSize;
        }
};

struct membuf : std::streambuf
{
  membuf( char* begin, char* end ) {
      this->setg( begin, begin, end );
  }

  membuf(rust::Slice<const uint8_t> buf) {
      const uint8_t *begin = reinterpret_cast<const uint8_t*>(buf.data());
      const uint8_t *end = reinterpret_cast<const uint8_t *>(buf.data() + buf.length());
      // set get pointer
      this->setg((char *)begin, (char *)begin, (char *)end);
      this->setp((char *)begin, (char *)end);
  }

  membuf(rust::Slice<uint8_t> buf) {
      char *begin = reinterpret_cast<char *>(buf.data());
      char *end = reinterpret_cast<char *>(buf.data() + buf.length());
      // set put pointer
      this->setp(begin, end);
  }
};

class GetRequest {
    public:
        GetRequest();
        
        ~GetRequest();

        GetRequest(GetRequest && other);
        
        void set_id(uint32_t id) const;
        
        uint32_t get_id() const;
        
        void set_key(rust::Slice<const uint8_t> key) const;
        
        const std::string& get_key() const;
        
        size_t serialized_size() const;

        void serialize_to_array(rust::Slice<uint8_t> buf) const;

        template <class Archive>
        void load(Archive & ar);

        template <class Archive>
        void save(Archive & ar) const;

    private:
        class impl;
        std::unique_ptr<impl> impl;
        friend class cereal::access;        
};

std::unique_ptr<GetRequest> new_get_request();
std::unique_ptr<GetRequest> deserialize_get_request_from_array(rust::Slice<const uint8_t> buf);

class GetResponse {
    public:
        GetResponse();
        
        ~GetResponse();

        GetResponse(GetResponse && other);
        
        void set_id(uint32_t id) const;
        
        uint32_t get_id() const;
        
        void set_value(rust::Slice<const uint8_t> value) const;
        
        const std::string& get_value() const;
        
        size_t serialized_size() const;

        void serialize_to_array(rust::Slice<uint8_t> buf) const;

        template <class Archive>
        void load(Archive & ar);

        template <class Archive>
        void save(Archive & ar) const;

    private:
        class impl;
        std::unique_ptr<impl> impl;
        friend class cereal::access;        
};

std::unique_ptr<GetResponse> new_get_response();
std::unique_ptr<GetResponse> deserialize_get_response_from_array(rust::Slice<const uint8_t> buf);

/*class GetMRequest {
};

class GetResponse {
};

class GetMResponse {
};

class PutRequest {
};

class PutMRequest {
};

class PutResponse {
};*/
