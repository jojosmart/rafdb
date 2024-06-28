#include "storage/rafdb/client/rafdb_client.h"

#include <time.h>
#include <string>
#include <vector>

#include "base/yr.h"
#include "base/flags.h"
#include "base/hash.h"
#include "base/logging.h"
#include "base/mutex.h"
#include "base/string_util.h"
#include "file/file.h"
#include "file/simple_line_reader.h"
#include "util/gtl/stl_util-inl.h"
#include "ts/base/util.h"
#include "base/time.h"


namespace {
const int kFrameSize = 128 * 1024;
const std::string kInvalidID = "NULL";
const int kMaxRafdbFeederSize = 65535;
const int kFlushInterval = 10;
}

namespace rafdb {

bool RafdbFeeder::Connect() {
  MutexLock lock(&mutex_feeder_);
  if (!connected_) {
    socket_.reset(new TSocket(host_, port_));
    socket_->setConnTimeout(500);
    socket_->setRecvTimeout(200);
    socket_->setSendTimeout(200);

    transport_.reset(new TFramedTransport(socket_, kFrameSize));
    protocol_.reset(new TBinaryProtocol(transport_));
    service_.reset(new RafdbServiceClient(protocol_));
    try {
      transport_->open();
      connected_ = true;
      VLOG(4) << "Success to connect " << host_ << ":" << port_;
    } catch (const TException &tx) {
      LOG(ERROR)<< "Fail to connect " << host_ << ":" << port_;
    }
  }
  return connected_;
}



void RafdbFeederMonitor::Run() {
  while (true) {
    dispatcher_->UpdateRdbNodeStatus();
    sleep(kFlushInterval);
  }
}

RafdbDispatcher::RafdbDispatcher(std::string rafdb_list) {
  rafdb_list_ = rafdb_list;
  conhash_selector_ = new ConhashSelector();
  connected_ = false;
  Init();
}

RafdbDispatcher::~RafdbDispatcher() {
  gtl::STLDeleteValues(&feeders_);
  delete conhash_selector_;
}

void RafdbDispatcher::Init() {
  //ota_id_provider_.reset(new OtaIdProvider);
  //LoadPriceConfig();
  ConhashCons();
  monitor_.reset(new RafdbFeederMonitor(this));
  monitor_->Start();
  //SplitString(FLAGS_key_no_hotelid_ota_list, ',', &no_hotelid_key_ota_vec_);
}

void RafdbDispatcher::ConhashCons() {
  std::vector<std::string> rafdb_vec;
  crawl::SplitFlagsEngineList(rafdb_list_,rafdb_vec);
  for (int i = 0;i<rafdb_vec.size();i++) {
    std::string ip;
    int port;
    int id;
    crawl::GetIpPortId(ip,port,id,rafdb_vec[i]);
    std::string cur_server;
    SStringPrintf(&cur_server,"%s:%d",ip.c_str(),port);
    feeders_[cur_server] = new RafdbFeeder(ip, port);
    if (feeders_[cur_server]->IsConnected()) {
      conhash_selector_->AddNode(cur_server);
      VLOG(4) << "add node: " << cur_server;
    } else {
      LOG(ERROR)<< "can't connect to server: " << cur_server;
    }
  }
  VLOG(3) << "feeders size: " << feeders_.size();

}



void RafdbDispatcher::UpdateRdbNodeStatus() {
  MutexLock lock(&mutex_);
  hash_map<std::string, RafdbFeeder*>::iterator it = feeders_.begin();
  for (; it != feeders_.end(); ++it) {
    if (it->second->IsConnected()) {
      conhash_selector_->AddNode(it->second->server());
    } else {
      conhash_selector_->DelNode(it->second->server());
    }
  }
}

bool RafdbDispatcher::Connect() {
  if (!connected_) {
    std::string leader_ldb_host = "";
    int leader_ldb_port = 0;
    std::vector<std::string> ldb_vec;
    crawl::SplitFlagsEngineList(rafdb_list_,ldb_vec);
    VLOG(5)<<"ldb list size is "<<ldb_vec.size();
    while( !crawl::GetLeaderEngine(leader_ldb_host,leader_ldb_port,ldb_vec,true) ) {
      VLOG(5)<<"get leader ldb failed,continue";
      sleep(1);//sleep 1s
    }
    VLOG(5)<<"get leader ldb success,host:"<<leader_ldb_host
      <<" port:"<<leader_ldb_port;
    boost::shared_ptr<TSocket> socket(
        new TSocket(leader_ldb_host, leader_ldb_port));
    boost::shared_ptr<TFramedTransport> transport(
        new TFramedTransport(socket, 1024 * 2));
    boost::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
    service_ldb_.reset(new RafdbServiceClient(protocol));

    try {
      transport->open();
      connected_ = true;
      LOG(INFO)<< "Connect " << leader_ldb_host
      << ":" << leader_ldb_port;
    } catch(...) {
      LOG(ERROR) << "Fail to connect " << leader_ldb_host
      << ":" << leader_ldb_port;
    }
  }
  return connected_;
}



std::string RafdbDispatcher::SelectRafdbFeeder( const std::string query) {
  MutexLock lock(&mutex_);
  std::string server;
  if (conhash_selector_->GetNode(server, query)) {
    //VLOG(4) << "server: " << server;
    return server;
  } else {
    return kInvalidID;
  }
}
//key:hash(url),value:content,dbname:token_id
bool RafdbDispatcher::Set(const std::string key,
                      const std::string value,
                      const std::string dbname) {
  while (!Connect()) {
    sleep(1);
  }
  try {
    if (!service_ldb_->IsLeader()) {
      VLOG(5)<<"leader changed";
      connected_ = false;
      return false;
    }
    return service_ldb_->LSet(dbname,key,value);
  }catch (const TException& tx) {
    connected_ = false;
    return false;
  
  }

  return true;
}

//dbname:token_id
bool RafdbDispatcher::Get(std::string &result,
                      const std::string key,
                    const std::string dbname) {
  std::string server = SelectRafdbFeeder(key);
  if (server == kInvalidID) {
    VLOG(6)<<"get server error"<<server
      <<"key is"<<key;
    return false;
  }
  try {
    feeders_[server]->Get(result,key,dbname);
    VLOG(6)<<"get server is"<<server;
    return true;
  }catch (const TException &tx) {
    VLOG(6)<<"get data exception,server is"<<server;
    return false;
  }
}

}
// namespace rafdb
