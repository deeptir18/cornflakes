## Cornflakes proto-codegen
- Input: a file that defines a protobuf format, right now, only support proto3
    - We don't support proto2 "required" fields
- Output: a header and {.cc} file that creates the code generated to deal with
  this new protobuf format, including:
    - Setters and getters
    - Serialization and Deserialization functions, into:
        - Demikernel scatter gather arrays
        - Linux iovecs

## Goal of codegen:
- For right now, we are just working with primitive types and Strings
- We will later add support for the following features:
    1. Repeated fields
        - Repeated strings
        - Repeated fixed width fields
        - Repeated nested structs
    - See the section below on repeated fields: I'm not actually sure what's the
      best way to deal with them!
    2. "Oneof" fields (these are basically unions)


### Proto Definition
- Consider these simple structs:
```
message PutMessage {
    int32 id = 1;
    String key = 2;
    String value = 3;
}
```
```
message NestedMessage {
    int32_t id = 1;
    PutMessage put = 2;
}
```
### In Memory Representation and allocation
- Note that a `String` is represented by a pointer and a length:
    ```
    struct BytesPointer {
        void *ptr;
        size_t length;
    }
    ```
- The compiler takes the above message definition and generates internally a
  struct that looks like this:
```
struct PutMessage {
    bool has_id;
    int32_t id;
    bool has_key;
    BytesPointer key;
    bool has_value;
    BytesPointer value;
}
```
```
struct NestedMessage {
    bool has_id;
    bool has_put;
    void* put;
}
```
- Therefore, allocation ends up being (in cpp format):
```
PutMessage* put = new PutMessage;
NestedMessage* nested = new NestedMessage;
```

**Generated setters and getters**:
1. Setters:
- For fixed width fields, like ints or bools, the value is copied into the
  header struct directly:
    ```
    // header:
    void set_id(int32_t id);
    // example:
    int32_t id = 1;
    put.set_id(id);
    ```
- For variable length fields, a length and pointer is recorded. The programmer
  needs to provide pointers to heap allocated data (or long-lived data that they
  know won't be deallocated while serialization is happening); for 0-copy cereal-ization,
  we can't have ephemeral values on the stack:
  ```
  // header:
  void set_key(void * ptr, size_t len);
  // example:
  put.set_key(str_ptr, str_len);
  ```
- For setting nested fields, the user needs to provide a pointer to the header
  struct of the parent message:
  ```
  // header:
  void set_put(PutMessage* put);
  // example:
  nested.set_put(put);
  ```

2. Getters:
- For fixed width fields, the value is copied from the struct:
```
int32_t id = put.get_id();
```
- For variable length fields (like strings or bytes), the programmer gets a
  pointer and length back:
```
    BytesPointer key = put.get_key();
    BytesPointer value = put.get_value();
```
- In addition, the following functions are generated to check if fields have
  been set:
```
    bool has_id();
    bool has_key();
    bool has_value();
```
- When fields aren't set, the getters will return default values, so
  programmers must check whether the field exists for full safety.

### Serialization to the on the wire format:
- Serialization/Deserialization relies on the OS to provide a "scatter-gather"
  abstraction, which defines different contiguous buffers and lengths to copy
  into the networking stack.
    - This is provided by both Demikernel SGAs as well as Linux's iovec.
    - The OS needs to provide a way to actually encode these SGAs, so we know
      where each buffer starts in the network message:
        - Demikernel provides this (because the networking stack encodes the
          length of each buffer in the message)
        - Linux iovec does not do this: we can write a shim for iovec that does
          do this
- The serialization encodes the information in the PutMessage to look like
      this:
    - The first buffer contains the header laid out exactly as it is in
      memory:
        ```
        // | has_id | has_key | has_value | id      | key len | key buffer ptr | value len | value buffer ptr |
        // | bool   | bool    | bool      | int64_t | int64_t | int64_t        | int64_t   | int64_t
        ```
        - The second buffer contains the key data
        - The third buffer has the value data
- For the nested message, the header is in the first buffer:
       ```
       | has id | has put | id   | put pointer |
       ```
    - The second buffer contains the header for the put data structure
        - The further buffers contain the nested pointers to the keys and values
          within the put message

- Deserialization:
    - The OS service should infer the lengths and pointers of each
      scatter-gather automatically: or maybe not?
    - Since the header is exactly what the struct looks like in memory, it can
      just deserialize the header to "deserialize" the data structure

## Freeing memory
- Ideally, eventually our library will do fun reference counting to free things
  automatically?

## General Questions:
1. What about 32 bit vs. 62 bit words?
    - I am a little confused about this point: can everything (pointers) be
      encoded as 64 bit?
2. Endian-ness?
    - What if the two sides have different endian-ness?
3. How do we deal with repeated fields?
    - For repeated strings or structs (variable length), can put each field into a new buffer ptr in the scatter
      gather and have an "iterator" interface as a getter
    - But what about repeated lists of fixed width types?
        - Capn proto and flatbuffers can handle this because the builder allows
          users to "initialize" the a list of the repeated field with a specific
          length
        - Protobuf handles this because the on the wire format includes a tag in
          front of each value for the deserializer to read in all repeated
          fields
        - We could somehow allocate/handle allocation for these fields
        - Or (easier to fit in with our current encoding scheme), require the
          user to provide a pointer to a list
        - I think we need to see real examples to see what the best way to
          handle this is
