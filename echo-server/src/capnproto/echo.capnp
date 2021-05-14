@0x81c88560bad9aacc;

struct SingleBufferCP {
    message @0 :Data;
}

struct ListCP {
    messages @0 :List(Data);
}

struct Tree1LCP {
    left @0 :SingleBufferCP;
    right @1 :SingleBufferCP;
}

struct Tree2LCP {
    left @0 :Tree1LCP;
    right @1 :Tree1LCP;
}

struct Tree3LCP {
    left @0 :Tree2LCP;
    right @1 :Tree2LCP;
}

struct Tree4LCP {
    left @0 :Tree3LCP;
    right @1 :Tree3LCP;
}

struct Tree5LCP {
    left @0 :Tree4LCP;
    right @1 :Tree4LCP;
}
