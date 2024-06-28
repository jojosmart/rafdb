
#include "storage/rafdb/accord.h"
namespace {
  //const int kFlushInterval = 5;
  static const int kElectionTimeout = 3000;// ms
}

namespace rafdb {
void Accord::Run() {
  while ( true ) {
    switch (state_) {
      case State::FOLLOWER:
        VLOG(5) << "now state: FOLLOWER";
        followerLoop();
        break;
      case State::CANDIDATE:
        VLOG(5) << "now state: CANDIDATE";
        candidateLoop();
        break;
      case State::LEADER:
        VLOG(5) << "now state: LEADER";
        leaderLoop();
        break;
      default:
        VLOG(5) << "Unknow state " << state_;
        break;
    }   
  }
}

void Accord::followerLoop() {
  int timeout = get_rand(kElectionTimeout, 2 * kElectionTimeout);// per ms
  VLOG(5) << "follower timeout is "<<timeout;
  base::Timer timer;
  timer.After(timeout);
  bool update;
  while (true) {
    update = false;
    if (timer.IsReadable()) {
      VLOG(5) << "follower timeout!!!, switch to candidate.";
      state_ = State::CANDIDATE;
      break;
    }
    Message tmp_message;
    if(rafdb_->message_queue_.TryPop(tmp_message)) {
      update = handleMessage(tmp_message);
    }
    if(update) {
      timer.After(get_rand(kElectionTimeout, 2 * kElectionTimeout));
    }
    usleep(20);//sleep 20us
  }
}

void Accord::candidateLoop() {
  int timeout = get_rand(kElectionTimeout, 2 * kElectionTimeout);// per ms
  VLOG(5) << "candidate timeout is "<<timeout;
  base::Timer timer;
  leader_id_ = 0;
  rafdb_->SetLeaderId(0);
  VLOG(5) << "candidate set  leader_id 0";
  int grants = 1;
  int noleader = 1;
  timer.After(timeout);
  VLOG(5) << "start query leader";
  sendQuery();
  while (true) {
    if (timer.IsReadable()) {
      VLOG(5) << "candidate timeout!!!";
      break;
    } 
    Message tmp_message;
    if (rafdb_->message_queue_.TryPop(tmp_message)) {
      if (tmp_message.message_type == MessageType::LEADERREP) {
        VLOG(5)<<"reveive message,type is LEADERREP";
        if (tmp_message.leader_id == 0) {
          VLOG(5)<<"leader_id is 0,noleader++";
          noleader++;
        }
        if (noleader >= quoramSize()) {
          VLOG(5) << "noleader gt half,start vote";
          SetTerm(GetTerm() + 1);
          SetVote(rafdb_->self_id_);
          VLOG(5)<<"start send vote";
          sendVotes();
        }
      }else if (tmp_message.message_type == MessageType::VOTEREP) {
        VLOG(5)<<"reveive message,type is VOTEREP";
        VLOG(5)<<"message.term_id is"<<tmp_message.term_id<<
          " current term is "<<GetTerm();
        if (tmp_message.term_id > GetTerm()) {
          VLOG(5)<<"message.term_id is"<<tmp_message.term_id
            <<" current term is "<<GetTerm()<<" stepDown to follower";
          stepDown(tmp_message.term_id);
        }
        if (tmp_message.granted) {
          VLOG(5) << "receive grant";
          grants++;
        }
        
      }else {
        handleMessage(tmp_message);
      }
    }
    if (grants >= quoramSize()) {
      VLOG(5) <<"grants is"<<grants<<"quoramSize is "<<quoramSize()<< " gt half agree,step leader";
      state_ = State::LEADER;
      break;
    }
    
    if (state_ != State::CANDIDATE) {
      break;
    }
    usleep(20);
  }
}

void Accord::sendQuery() {
  for (size_t i = 0;i < rafdb_->NodeList.size();i++) {
    std::string dest_ip = rafdb_->NodeList[i].ip;
    int dest_port = rafdb_->NodeList[i].port;
    VLOG(5) << "sendQuery" << "ip is "<<dest_ip<<" port is "<<dest_port;
    Message mess_send;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::LEADERREQ;
    sendRPC(dest_ip,dest_port,mess_send,"QueryLeaderId");
  }
}

void Accord::sendVotes() {
  Message mess_send;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.message_type= MessageType::VOTEREQ;
  mess_send.term_id = GetTerm();
  mess_send.self_healthy = rafdb_->SelfHealthy();
  mess_send.candidate_id = rafdb_->self_id_;
  for (size_t i = 0;i < rafdb_->NodeList.size();i++) {
    std::string dest_ip = rafdb_->NodeList[i].ip;
    int dest_port = rafdb_->NodeList[i].port;
    VLOG(5) << "sendVotes" << "ip is "<<dest_ip<<" port is "<<dest_port
      <<" term is "<<mess_send.term_id;
    sendRPC(dest_ip,dest_port,mess_send,"SendVote");
  }

}

void Accord::leaderLoop() {
  leader_id_ = rafdb_->self_id_;
  rafdb_->SetLeaderId(leader_id_);
  peer_->SetRunFlag(true);
  //pthread_t pid_t = peer_->tid();
  while (true) {
    if (rafdb_->SelfHealthy() == false) {
      VLOG(5) << "leader is not healthy,switch to follower";
      state_ = State::FOLLOWER;//suicide
    }
    if (peer_->getDisconnNums() >= quoramSize()) {
      VLOG(5)<< "followers disconnect num gt half,switch follower";
      state_ = State::FOLLOWER;
    }
    Message tmp_message; 
    if (rafdb_->message_queue_.TryPop(tmp_message)) {
      if (tmp_message.message_type == MessageType::HEARTREP) {
        VLOG(5)<<"leader receive heart reply";
        handleHeartRep(tmp_message);
      }else {
        handleMessage(tmp_message);
      }
    }
    if (state_ != State::LEADER) {
      peer_->SetRunFlag(false);
      break;
    }
    usleep(1000);
  }

}

void Accord::Init() {
  peer_.reset(new Peer(this));
  peer_->Start();//heart beat thread
}

bool Accord::handleMessage(Message& message) {
  if (message.message_type == MessageType::VOTEREQ) {
    return handleVoteReq(message);//process vote req
  }else if (message.message_type == MessageType::HEARTREQ) {
    return handleHeartReq(message);// process heartbeat
  }else if (message.message_type == MessageType::LEADERREQ) {
    return handleQueryLeaderReq(message);
  }
  return false;
}

bool Accord::handleHeartReq(Message& message) {
  VLOG(5) << "receive hear req "<<
    "current term is"<<GetTerm()<<" leader term is"<<message.term_id;
  int64_t currentTerm = GetTerm();
  int serverId = rafdb_->self_id_;
  std::string dest_ip = message.ip;
  int dest_port = message.port;
  if (message.term_id < currentTerm) {
    // from old leader
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.server_id = serverId;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::HEARTREP;
    mess_send.success = false;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyHeartBeat");
    return false;
  }  
 
  if (message.term_id >= currentTerm) {
    stepDown(message.term_id);
  }

  rafdb_->SetLeaderId(message.leader_id);
  //rafdb_->DataSyncLocal(message);
  //VLOG(5)<<"data sync,host_inst_count_map size is"<<
  //  message.data_sync.host_inst_count_map.size()<<
  //  "host_normal_count_map size is"<<message.data_sync.host_normal_count_map.size()<<
  //  "last_handler_index is " <<message.data_sync.last_handler_index<<
  //  "last_receiver_index is "<<message.data_sync.last_receiver_index;
  leader_id_ = message.leader_id;
  Message mess_send;
  mess_send.term_id = currentTerm;
  mess_send.server_id = serverId;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.message_type= MessageType::HEARTREP;
  mess_send.success = true;
  sendRPC(dest_ip,dest_port,mess_send,"ReplyHeartBeat");
  return true;

}

bool Accord::handleQueryLeaderReq(const Message& message) {
  int64_t currentTerm = GetTerm();
  if (state_ == State::CANDIDATE) {
    std::string dest_ip = message.ip;
    int dest_port = message.port;
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::LEADERREP;
    mess_send.leader_id = 0;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyLeaderId");
  }else {
    std::string dest_ip = message.ip;
    int dest_port = message.port;
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.message_type= MessageType::LEADERREP;
    mess_send.leader_id = leader_id_;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyLeaderId");
  }
  return false;
}

void Accord::stepDown(int64_t term) {
  if (term > GetTerm()) {
    SetTerm(term);
    SetVote(0);
  }
  state_ = State::FOLLOWER;
  //rafdb_->SetLeaderId(0);
}

bool Accord::handleVoteReq(const Message& message) {
  int64_t currentTerm = GetTerm();
  std::string dest_ip = message.ip;
  int dest_port = message.port;
  VLOG(5) << "receive vote,self term is "<<currentTerm
      <<" voter term is "<<message.term_id;
  if (message.term_id < currentTerm) {
    VLOG(5)<<"refuse";
    //VLOG(5) << ""
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.self_healthy = rafdb_->SelfHealthy();
    mess_send.granted = false;
    mess_send.message_type= MessageType::VOTEREP;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
    return false;
  }
  if (message.term_id > currentTerm) {
    VLOG(5)<<"step down";
    stepDown(message.term_id);
  }
  if (vote_id_ != 0 && vote_id_ != message.candidate_id) {
    VLOG(5)<<"had voted,but not this man,so refuse,because everyone have only one vote chance";
    VLOG(5)<<"vote_id is "<<vote_id_<<" message.candidate_id is "<<message.candidate_id;
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.granted = false;
    mess_send.message_type= MessageType::VOTEREP;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
    return false;
  }
  if (message.self_healthy) {
    VLOG(5)<<"he is healthy,i agree";
    SetVote(message.candidate_id);
    Message mess_send;
    mess_send.term_id = currentTerm;
    mess_send.ip = rafdb_->ip_;
    mess_send.port = rafdb_->port_;
    mess_send.granted = true;
    mess_send.message_type= MessageType::VOTEREP;
    sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
    return true;

  } // judge healthy
  VLOG(5) << "term_id gt than myself,but he is not healthy,so refuse";
  Message mess_send;
  mess_send.term_id = currentTerm;
  mess_send.ip = rafdb_->ip_;
  mess_send.port = rafdb_->port_;
  mess_send.granted = false;
  mess_send.message_type= MessageType::VOTEREP;
  sendRPC(dest_ip,dest_port,mess_send,"ReplyVote");
  return false;
}

bool Accord::sendRPC(const std::string ip,const int port,
    const Message& message,const std::string rpc_name) {
  
  base::ThriftClient<RafdbServiceClient> thrift_client(ip,port);
  try {
    if (thrift_client.GetService() == NULL) {
      thrift_client.GetTransport()->close();
      return false;
    }else {
      if (rpc_name == "SendVote") {
        thrift_client.GetService()->SendVote(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyVote") {
        thrift_client.GetService()->ReplyVote(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "SendHeartBeat") {
        thrift_client.GetService()->SendHeartBeat(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyHeartBeat") {
        thrift_client.GetService()->ReplyHeartBeat(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "QueryLeaderId") {
        thrift_client.GetService()->QueryLeaderId(message);
        thrift_client.GetTransport()->close();
        return true;
      }else if (rpc_name == "ReplyLeaderId") {
        thrift_client.GetService()->ReplyLeaderId(message);
        thrift_client.GetTransport()->close();
        return true;
      }else {
        thrift_client.GetTransport()->close();
        return false;
      }
    }
  } catch (const TException &tx) {
    thrift_client.GetTransport()->close();
    return false;
  }
}


int Accord::quoramSize() {
  return (rafdb_->NodeList.size()+1) / 2 + 1;
}

void Accord::handleHeartRep(const Message& message) {
  if (message.term_id < GetTerm()) {
    return;
  }
  if (message.term_id > GetTerm()) {
    stepDown(message.term_id);
  }
  if (!message.success) {
    return;
  }
}

int Accord::get_rand(int start,int end) {
      if (end <= start)
        return start;
      srand(rafdb_->self_id_);
      return (rand() % (end-start))+ start;
    }


}
