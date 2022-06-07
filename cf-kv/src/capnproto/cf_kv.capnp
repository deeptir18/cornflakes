@0x81c88560bad9aacd;

struct GetReq {
    id @0 :UInt32;
    key @1 :Text;
}

struct GetResp {
    id @0 :UInt32;
    val @1 :Data;
}

struct PutReq {
    id @0 :UInt32;
    key @1 :Text;
    val @2 :Data;
}

struct PutResp {
    id @0 :UInt32;
}

struct GetMReq {
    id @0 :UInt32;
    keys @1 :List(Text);
}

struct GetMResp {
    id @0 :UInt32;
    vals @1 :List(Data);
}

struct PutMReq {
    id @0 :UInt32;
    keys @1 :List(Text);
    vals @2 :List(Data);
}

struct GetListReq {
    id @0 :UInt32;
    key @1 :Text;
}

struct GetListResp {
    id @0 :UInt32;
    vals @1 :List(Data);
}

struct PutMList {
    id @0 :UInt32;
    key @1 :Text;
    vals @2 :List(Data);
}

