
#ifndef STORAGE_RAFDB_RAFDB_H_
#define STORAGE_RAFDB_RAFDB_H_

#include <time.h>
#include <stdexcept>
#include <fstream>

#include <vector>
#include <map>
#include <string>
#include <set>
#include "third_party/jsoncpp/include/json.h"
#include "storage/rafdb/manager.h"
#include "storage/rafdb/accord.h"
#include "storage/rafdb/peer.h"
#include "ts/base/crawl_config.h"

#include "base/logging.h"
#include "base/thrift.h"
#include "base/thread.h"
#include "base/mutex.h"
#include "base/concurrent_queue.h"
#include "base/flags.h"
#include "base/scoped_ptr.h"
#include "base/hash_tables.h"
#include "base/basictypes.h"
#include "third_party/leveldb/db.h"
#include "storage/rafdb/proto/gen-cpp/RafdbService.h"
#include "storage/rafdb/global.h"
//#include "storage/image_leveldbd/image_set.h"
//#include "storage/image_leveldbd/image_get.h"

DECLARE_string(rafdb_self);
namespace rafdb {

class IteratorChecker;
class Manager;
class Accord;

class RafDb : virtual public rafdb::RafdbServiceIf {
 public:
  RafDb();
  virtual ~RafDb();

  bool Set(const std::string &dbname, const std::string &key,
      const std::string &value);
  bool LSet(const std::string &dbname, const std::string &key,
      const std::string &value);
  bool MPSet(const std::string &dbname,
      const std::vector<rafdb::Pair> &pairs);
  void Get(std::string &result, const std::string &dbname,
      const std::string &key);
  void MGet(std::vector<std::string> &results, const std::string &dbname,
      const std::vector<std::string> &keys);
  bool Delete(const std::string &dbname, const std::string &key);
  bool MDelete(const std::string &dbname,
      const std::vector<std::string> &keys);
  int32_t OpenIterator(const std::string &dbname);
  bool CloseIterator(int32_t itID);
  bool SeekToFirst(int32_t itID);
  bool SeekToLast(int32_t itID);
  bool Valid(int32_t itID);
  bool Seek(int32_t itID, const std::string &target);
  void NextKeys(std::vector<std::string> &results, int32_t itID,
      int32_t number);
  void NextValues(std::vector<std::string> &results, int32_t itID,
      int32_t number);
  void NextPairs(std::vector<rafdb::Pair> &pairs, int32_t itID,
      int32_t number);
  bool DeleteDatabase(const std::string& dbname);
  bool IsHealthy();
    bool IsLeader() {
      base::MutexLock lock(&leader_mutex_);
      if (self_id_ == leader_id_)
        return true;
      else
        return false;
    }
    int32_t GetLeaderId() {
      base::MutexLock lock(&leader_mutex_);
      return leader_id_;
    }
    bool SelfHealthy() {
      base::MutexLock lock(&mutex_);
      //TODO
      return true;
    }
    void SendVote(const rafdb::Message& message);
    void ReplyVote(const rafdb::Message& message);
    void SendHeartBeat(const rafdb::Message& message);
    void ReplyHeartBeat(const rafdb::Message& message);
    void QueryLeaderId(const rafdb::Message& message);
    void ReplyLeaderId(const rafdb::Message& message);
    std::vector<NodeInfo> NodeList; 
 private:
  friend class IteratorChecker;
  friend class Manager;
  friend class Accord;
  friend class Peer;
  scoped_ptr<Manager> manager_;
  scoped_ptr<Accord> accord_;
  void SetLeaderId(int leader_id) {
    base::MutexLock lock(&leader_mutex_);
    VLOG(3) << "leveldb server leader_id is set " << leader_id;
    leader_id_ = leader_id;
  }
  void Init();

 // friend class ImageSet;
  //friend class ImageGet;

  struct WrapIterator {
    leveldb::Iterator *iterator_;
    time_t last_operation_;
    std::string dbname_;
    WrapIterator():iterator_(NULL), last_operation_(0) { }
    ~WrapIterator() { delete iterator_;}
  };

  leveldb::Options                            options_;
  leveldb::ReadOptions                        roptions_;
  leveldb::WriteOptions                       woptions_;
  int32_t                                     it_id_;
  base::hash_map<std::string, leveldb::DB*>   db_map_;
  base::hash_map<int32_t, WrapIterator*>      it_map_;
  base::Mutex                                 db_map_mutex_;
  base::Mutex                                 it_map_mutex_;
  base::Mutex leader_mutex_;
  base::Mutex mutex_;
  base::Mutex leader_data_sync_;
  base::ConcurrentQueue<Message> message_queue_;
  base::ConcurrentQueue<LKV*> lkv_queue_;
  int leader_id_;
  int self_id_;
  std::string ip_;
  int port_;

 // base::Mutex                                 get_room_image_mutex_;
 // base::Mutex                                 get_hotel_image_mutex_;
 // base::Mutex                                 update_room_image_mutex_;
 // base::Mutex                                 update_hotel_image_mutex_;
  scoped_ptr<IteratorChecker>                 it_checker_;

// image related

  //base::hash_map<uint64,std::vector<std::string> > image_resp_map_;
  //base::hash_map<std::string,std::vector<std::string> > image_resp_map_;
  //base::ConcurrentQueue<RoomImageInfo> image_set_queue_;
  //base::Mutex image_resp_map_mutex_;
  //scoped_ptr<ImageSet>                 image_set_;
  //scoped_ptr<ImageGet>                 image_get_;
// image related

  leveldb::DB* Open(const std::string &dbname);
  void LoadDB();

  DISALLOW_COPY_AND_ASSIGN(RafDb);
};

class IteratorChecker : public base::Thread {
 public:
  explicit IteratorChecker(RafDb* rafdb) : rafdb_(rafdb) { }
  virtual ~IteratorChecker() { }

 protected:
  virtual void Run();

 private:
  RafDb* rafdb_;

  DISALLOW_COPY_AND_ASSIGN(IteratorChecker);
};

}  // namespace rafdb

#endif 
