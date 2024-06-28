include "accord.thrift"

namespace cpp rafdb

struct Pair {
    1:string key,
    2:string value
}

service RafdbService {
    bool Set(1:string dbname, 2:string key, 3:string value),
    bool MPSet(1:string dbname, 3:list<Pair> pairs),
    string Get(1:string dbname, 3:string key),
    list<string> MGet(1:string dbname, 2:list<string> keys),
    bool Delete(1:string dbname, 2:string key),
    bool MDelete(1:string dbname, 2:list<string> keys),

    i32 OpenIterator(1:string dbname), //return id > 0
    bool CloseIterator(1:i32 itID),
    bool SeekToFirst(1:i32 itID),
    bool SeekToLast(1:i32 itID),
    bool Valid(1:i32 itID),
    bool Seek(1:i32 itID, 2:string target),
    list<string> NextKeys(1:i32 itID, 2:i32 number),
    list<string> NextValues(1:i32 itID, 2:i32 number),
    list<Pair> NextPairs(1:i32 itID, 2:i32 number),
    bool DeleteDatabase(1:string dbname),
    bool LSet(1:string dbname, 2:string key, 3:string value);
    bool IsHealthy();
    bool IsLeader();
    i32 GetLeaderId(); //for client
    oneway void SendVote(1: accord.Message message);
    oneway void ReplyVote(1:  accord.Message message);
    oneway void SendHeartBeat(1:  accord.Message message);
    oneway void ReplyHeartBeat(1:  accord.Message message);
    oneway void QueryLeaderId(1:  accord.Message message);
    oneway void ReplyLeaderId(1:  accord.Message message);
   
}

