@0x81c88560bad9aacd;

struct GetReq {
    id @0 :UInt32;
    key @1 :Text;
}

struct GetResp {
    id @0 :UInt32;
    vals @1 :List(Data);
}

struct PutReq {
    id @0 :UInt32;
    key @1 :Text;
    vals @2 :List(Data);
}

struct PutResp {
    id @0 :UInt32;
}
