
#ifndef STORAGE_RAFDB_PEER_H_
#define STORAGE_RAFDB_PEER_H_

#include <string>
#include <vector>

#include "base/hash_tables.h"
#include "base/mutex.h"
#include "base/thread.h"
#include "base/scoped_ptr.h"
#include "base/thrift.h"
#include "base/logging.h"
#include "storage/rafdb/proto/gen-cpp/RafdbService.h"
#include "storage/rafdb/accord.h"



namespace rafdb {

class Accord;
//typedef struct NodeInfo{
//  std::string ip;
//  int port;
//}NodeInfo;

class Peer :public base::Thread  {
  public:
    explicit Peer(Accord* accord)
                         :accord_(accord)
    {
      disconnect_nums_ = 0;
      run_flag_ = false;
    }
    virtual ~Peer() {}
    void setDisconnNums(int nums) {
      base::MutexLock lock(&mutex_);
      disconnect_nums_ = nums;
    }
    int getDisconnNums() {
      base::MutexLock lock(&mutex_);
      return disconnect_nums_;
    }
    void SetRunFlag(bool flag) {
      base::MutexLock lock(&mutex_);
      if (flag) {
        VLOG(3) << "hear beat start";
        disconnect_nums_ = 0;
      }
      else
        VLOG(3) << "hear beat pause";
      run_flag_ = flag;
    }
    bool GetRunFlag() {
      base::MutexLock lock(&mutex_);
      return run_flag_;
    }
  protected:
    virtual void Run();
    
  private:
    int disconnect_nums_;
    bool run_flag_;
    Accord* accord_;
    base::Mutex mutex_;
    DISALLOW_COPY_AND_ASSIGN(Peer);
};
}





#endif
