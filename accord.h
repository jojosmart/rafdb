
#ifndef STORAGE_RAFDB_ACCORD_H_

#define STORAGE_RAFDB_ACCORD_H_

#include <string>
#include <vector>
#include <time.h>
#include <unistd.h>

#include "base/hash_tables.h"
#include "base/mutex.h"
#include "base/timer.h"
#include "base/flags.h"
#include "base/thread.h"
#include "base/scoped_ptr.h"
#include "storage/rafdb/rafdb.h"
#include "storage/rafdb/peer.h"
#include "base/thrift.h"
#include "base/logging.h"
#include "storage/rafdb/proto/gen-cpp/RafdbService.h"
//#include "ts/proto/gen-cpp/GeneralCrawlDocServlet.h"



namespace rafdb {

class RafDb;
class Peer;
class Accord :public base::Thread  {
  public:
    explicit Accord(RafDb* rafdb_p,std::string ip,int port)
                         :rafdb_(rafdb_p),ip_(ip),port_(port)
    {
      state_ = State::FOLLOWER;
      term_ = 0;
      vote_id_ = 0;
      leader_id_ = 0;
      Init();
    }
    void Init();
    virtual ~Accord() {}
  protected:
    virtual void Run();

  private:
    int quoramSize(); 
    void SetTerm(int64_t term) {
      base::MutexLock lock(&term_mutex_);
      term_ = term;
    }
    int64_t GetTerm() {
      base::MutexLock lock(&term_mutex_);
      return term_;
    }
    void SetVote(int leader_id) {
      base::MutexLock lock(&vote_mutex_);
      vote_id_ = leader_id;
    }
    int GetVote() {
      base::MutexLock lock(&vote_mutex_);
      return vote_id_;
    }
    int get_rand(int start,int end);
    void stepDown(int64_t term);
    void followerLoop();
    void candidateLoop();
    void leaderLoop();
    void sendQuery();
    void handleHeartRep(const Message& message);
    void sendVotes();
    bool handleMessage(Message& message);
    bool handleVoteReq(const Message& message);
    bool handleHeartReq(Message& message);
    bool handleQueryLeaderReq(const Message& message);
    bool sendRPC(const std::string ip,const int port,const Message& message,const std::string rpc_name);
    friend class Peer;
    scoped_ptr<Peer> peer_;
    RafDb* rafdb_;
    State::type state_;    
    base::Mutex term_mutex_;
    base::Mutex vote_mutex_;
    int64_t term_;
    int vote_id_;
    std::string ip_;
    int port_;
    int leader_id_;
    DISALLOW_COPY_AND_ASSIGN(Accord);
};
}





#endif
