#pragma once
#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/polymorphic.hpp"
#include "rust/cxx.h"
//#include "echo-server/src/cereal/include/cxx.h"
#include <streambuf>

struct membuf : std::streambuf
{
  membuf( char* begin, char* end ) { 
      this->setg( begin, begin, end ); 
  }

  membuf(rust::Slice<const uint8_t> buf) {
      const uint8_t *begin = reinterpret_cast<const uint8_t*>(buf.data());
      const uint8_t *end = reinterpret_cast<const uint8_t *>(buf.data() + buf.length());
      this->setg((char *)begin, (char *)begin, (char *)end);
  }

  membuf(rust::Slice<uint8_t> buf) {
      char *begin = reinterpret_cast<char *>(buf.data());
      char *end = reinterpret_cast<char *>(buf.data() + buf.length());
      printf("Setting beginning of mbuf to be to %p, end to be %p, length is: %u\n", begin, end, (unsigned)buf.length());
      this->setg(begin, begin, end);
  }
};

class SingleCereal {
    public:
        SingleCereal();
        void set_data(rust::Slice<const uint8_t> data) const;
        void set_data(const std::string& data) const;
        const std::string& get_data() const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;
        bool equals(const std::unique_ptr<SingleCereal> other) const;
        bool equals(const std::shared_ptr<SingleCereal> other) const;

        template <class Archive>
        void serialize(Archive & ar) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<SingleCereal> new_single_cereal();
std::unique_ptr<SingleCereal> deserialize_single_cereal_from_array(rust::Slice<const uint8_t> buf);

class ListCereal {
    public:
        ListCereal();
        void append(rust::Slice<const uint8_t> data) const;
        const std::string& get(size_t idx) const;
        void set(size_t idx, rust::Slice<const uint8_t> data) const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;

        template<class Archive>
        void serialize(Archive & ar) const;
    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<ListCereal> new_list_cereal();
std::unique_ptr<ListCereal> deserialize_list_cereal_from_array(rust::Slice<const uint8_t> buf);

class Tree1Cereal {
    public:
        Tree1Cereal();
        std::shared_ptr<SingleCereal> get_left() const;
        void set_left(std::unique_ptr<SingleCereal> left) const;
        std::shared_ptr<SingleCereal> get_right() const;
        void set_right(std::unique_ptr<SingleCereal> right) const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;
        bool equals(std::unique_ptr<Tree1Cereal> other) const;
        bool equals(std::shared_ptr<Tree1Cereal> other) const;

        template<class Archive>
        void serialize(Archive & ar ) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<Tree1Cereal> new_tree1_cereal();
std::unique_ptr<Tree1Cereal> deserialize_tree1_cereal_from_array(rust::Slice<const uint8_t> buf);
std::unique_ptr<Tree1Cereal> reserialize_tree1(std::unique_ptr<Tree1Cereal> input);
std::unique_ptr<Tree1Cereal> reserialize_tree1(std::shared_ptr<Tree1Cereal> input);

class Tree2Cereal {
    public:
        Tree2Cereal();
        std::shared_ptr<Tree1Cereal> get_left() const;
        void set_left(std::unique_ptr<Tree1Cereal> left) const;
        std::shared_ptr<Tree1Cereal> get_right() const;
        void set_right(std::unique_ptr<Tree1Cereal> right) const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;
        bool equals(std::unique_ptr<Tree2Cereal> other) const;
        bool equals(std::shared_ptr<Tree2Cereal> other) const;

        template<class Archive>
        void serialize(Archive & ar ) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<Tree2Cereal> new_tree2_cereal();
std::unique_ptr<Tree2Cereal> deserialize_tree2_cereal_from_array(rust::Slice<const uint8_t> buf);
std::unique_ptr<Tree2Cereal> reserialize_tree2(std::unique_ptr<Tree2Cereal> input);
std::unique_ptr<Tree2Cereal> reserialize_tree2(std::shared_ptr<Tree2Cereal> input);

class Tree3Cereal {
    public:
        Tree3Cereal();
        std::shared_ptr<Tree2Cereal> get_left() const;
        void set_left(std::unique_ptr<Tree2Cereal> left) const;
        std::shared_ptr<Tree2Cereal> get_right() const;
        void set_right(std::unique_ptr<Tree2Cereal> right) const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;
        bool equals(std::unique_ptr<Tree3Cereal> other) const;
        bool equals(std::shared_ptr<Tree3Cereal> other) const;

        template<class Archive>
        void serialize(Archive & ar ) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<Tree3Cereal> new_tree3_cereal();
std::unique_ptr<Tree3Cereal> deserialize_tree3_cereal_from_array(rust::Slice<const uint8_t> buf);
std::unique_ptr<Tree3Cereal> reserialize_tree3(std::unique_ptr<Tree3Cereal> input);
std::unique_ptr<Tree3Cereal> reserialize_tree3(std::shared_ptr<Tree3Cereal> input);

class Tree4Cereal {
    public:
        Tree4Cereal();
        std::shared_ptr<Tree3Cereal> get_left() const;
        void set_left(std::unique_ptr<Tree3Cereal> left) const;
        std::shared_ptr<Tree3Cereal> get_right() const;
        void set_right(std::unique_ptr<Tree3Cereal> right) const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;
        bool equals(std::unique_ptr<Tree4Cereal> other) const;
        bool equals(std::shared_ptr<Tree4Cereal> other) const;

        template<class Archive>
        void serialize(Archive & ar ) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<Tree4Cereal> new_tree4_cereal();
std::unique_ptr<Tree4Cereal> deserialize_tree4_cereal_from_array(rust::Slice<const uint8_t> buf);
std::unique_ptr<Tree4Cereal> reserialize_tree4(std::unique_ptr<Tree4Cereal> input);
std::unique_ptr<Tree4Cereal> reserialize_tree4(std::shared_ptr<Tree4Cereal> input);

class Tree5Cereal {
    public:
        Tree5Cereal();
        std::shared_ptr<Tree4Cereal> get_left() const;
        void set_left(std::unique_ptr<Tree4Cereal> left) const;
        std::shared_ptr<Tree4Cereal> get_right() const;
        void set_right(std::unique_ptr<Tree4Cereal> right) const;
        size_t serialized_size() const;
        void serialize_to_array(rust::Slice<uint8_t> buf) const;
        bool equals(std::unique_ptr<Tree5Cereal> other) const;
        bool equals(std::shared_ptr<Tree5Cereal> other) const;

        template<class Archive>
        void serialize(Archive & ar ) const;

    private:
        class impl;
        std::shared_ptr<impl> impl;
};

std::unique_ptr<Tree5Cereal> new_tree5_cereal();
std::unique_ptr<Tree5Cereal> deserialize_tree5_cereal_from_array(rust::Slice<const uint8_t> buf);
std::unique_ptr<Tree5Cereal> reserialize_tree5(std::unique_ptr<Tree5Cereal> input);
std::unique_ptr<Tree5Cereal> reserialize_tree5(std::shared_ptr<Tree5Cereal> input);





