@0x81c88560bad9aace;

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
    rangestart @2 :Int32;
    rangeend @3   :Int32;
}

struct GetListResp {
    id @0 :UInt32;
    vals @1 :List(Data);
}

struct PutListReq {
    id @0 :UInt32;
    key @1 :Text;
    vals @2 :List(Data);
}

struct AddUser {
    keys @0 :List(Text);
    vals @1 :List(Data);
}

struct AddUserResponse {
    firstVal @0:Data;
}

struct FollowUnfollow {
    keys @0 :List(Text);
    vals @1 :List(Data);
}

struct FollowUnfollowResponse {
    originalVals @0 :List(Data);
}

struct PostTweet {
    keys @0 :List(Text);
    vals @1 :List(Data);
}

struct PostTweetResponse {
    vals @0 :List(Data);
}

struct GetTimeline {
    keys @0 :List(Text);
}

struct GetTimelineResponse {
    vals @0 :List(Data);
}
