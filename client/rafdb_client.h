
#ifndef STORAGE_RAFDB_CLIENT_RAFDB_CLIENT_H_
#define STORAGE_RAFDB_CLIENT_RAFDB_CLIENT_H_

#include <string>
#include <vector>
#include "base/hash_tables.h"
#include "base/mutex.h"
#include "base/thread.h"
#include "base/scoped_ptr.h"
#include "base/thrift.h"
#include "base/string_util.h"
#include "storage/rafdb/client/conhash_selector.h"
#include "storage/rafdb/proto/gen-cpp/RafdbService.h"


namespace rafdb {

class RafdbDispatcher;

class RafdbFeeder {
 public:
  RafdbFeeder(const std::string& host, int port)
      : host_(host),
        port_(port),
        connected_(false) {
    SStringPrintf(&server_, "%s:%d", host.c_str(), port);
  }
  ~RafdbFeeder() {
  };
  bool IsConnected() {
    return Connect();
  }
  const std::string& host() const {
    return host_;
  }
  const int& port() const {
    return port_;
  }
  const std::string& server() const {
    return server_;
  }
  void Get(std::string &result,
          const std::string key,
          const std::string dbname) {
    VLOG(3)<<"GET RPC,key is"<<key;
    service_->Get(result,dbname,key); 
  }

 private:
  bool Connect();

  std::string host_;
  int port_;
  bool connected_;
  std::string server_;
  base::Mutex mutex_feeder_;

  boost::shared_ptr<TSocket> socket_;
  boost::shared_ptr<TFramedTransport> transport_;
  boost::shared_ptr<TBinaryProtocol> protocol_;
  boost::shared_ptr<RafdbServiceClient> service_;

  DISALLOW_COPY_AND_ASSIGN(RafdbFeeder);
};

class RafdbFeederMonitor : public base::Thread {
 public:
  explicit RafdbFeederMonitor(RafdbDispatcher* dispatcher)
      : dispatcher_(dispatcher) {
  }
  virtual ~RafdbFeederMonitor() {
  }

 protected:
  virtual void Run();

 private:
  RafdbDispatcher* dispatcher_;

  DISALLOW_COPY_AND_ASSIGN(RafdbFeederMonitor);
};

class RafdbDispatcher {
 public:
  RafdbDispatcher(std::string rafdb_list);
  bool Connect();
  virtual ~RafdbDispatcher();
  bool Set(const std::string key,const std::string value,const std::string dbname);
  bool Get(std::string &result,const std::string key,const std::string dbname);

  void UpdateRdbNodeStatus();

 private:
  friend class RafdbFeederMonitor;

  void Init();
  //void LoadPriceConfig();
  void ConhashCons();
  //int64 GetLdbKeyStr(const char *instr) {
  //  return base::Fingerprint(instr);
  //}
  //int64
  std::string SelectRafdbFeeder(const std::string query);
  base::hash_map<std::string, RafdbFeeder*> feeders_;  // server : RafdbFeeder
  //base::hash_map<int16, ConhashSelector*> selectors_;  // ota_id: ConhashSelector
  ConhashSelector* conhash_selector_;
  std::string rafdb_list_;
//  scoped_ptr<ConhashSelector> selector_;
  scoped_ptr<RafdbFeederMonitor> monitor_;
  scoped_ptr<RafdbServiceClient> service_ldb_;
//  scoped_ptr<PriceServiceClient> request_parser_client_;
  //scoped_ptr<OtaIdProvider> ota_id_provider_;
  bool connected_;
  base::Mutex mutex_;
  //base::Mutex mutex_1_;
  //base::Mutex mutex_2_;
  //base::Mutex mutex_3_;
  //std::vector<std::string> no_hotelid_key_ota_vec_; // 不使用base hotel id来计算key的ota id list

  DISALLOW_COPY_AND_ASSIGN(RafdbDispatcher);
};

}  // namespace rafdb

#endif  // TS_SCHEDULER_INSTANT_PRICE_DISPATCHER_H_
