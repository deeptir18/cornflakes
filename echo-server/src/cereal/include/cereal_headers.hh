#pragma once
#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "echo-server/src/cereal/include/cxx.h"
#include <streambuf>

struct membuf : std::streambuf
{
  membuf( char* begin, char* end ) { 
      this->setg( begin, begin, end ); 
  }
};

class SingleCereal {
    public:
        SingleCereal();
        void set_data(rust::Slice<const uint8_t> data) const;
        const std::string& get_data() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;

        template <class Archive>
        void serialize(Archive & ar) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<SingleCereal> new_single_cereal();

class ListCereal {
    public:
        ListCereal();
        void append_string(rust::Slice<const uint8_t> data) const;
        const std::string& get(size_t idx) const;
        void set(size_t idx, rust::Slice<const uint8_t> data) const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;

        template<class Archive>
        void serialize(Archive & ar) const;
    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<ListCereal> new_list_cereal();

class Tree1Cereal {
    public:
        Tree1Cereal();
        void set_left(std::unique_ptr<SingleCereal> left) const;
        void set_right(std::unique_ptr<SingleCereal> right) const;
    private:
        class impl;
        std::shared_ptr<impl> impl;
};
