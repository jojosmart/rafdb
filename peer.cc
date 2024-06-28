
#include "storage/rafdb/peer.h"
namespace {
const int kHeartBeatInterval = 1000000;//us
}


namespace rafdb {

void Peer::Run() {
  while ( true ) {
    if (GetRunFlag()) {
      int num = 0;
      for(size_t i=0;i<accord_->rafdb_->NodeList.size();i++) {
        std::string dest_ip = accord_->rafdb_->NodeList[i].ip;
        int dest_port = accord_->rafdb_->NodeList[i].port;
        Message mess_send;
        mess_send.term_id = accord_->GetTerm();
        mess_send.ip = accord_->rafdb_->ip_;
        mess_send.port = accord_->rafdb_->port_;
        mess_send.message_type= MessageType::HEARTREQ;
        mess_send.leader_id = accord_->leader_id_;
        //accord_->rafdb_->FillDataSyncInfo(mess_send);//data sync
        bool ret = accord_->sendRPC(dest_ip,dest_port,mess_send,"SendHeartBeat");
        if (!ret) {
          VLOG(5) << "send heart beat fail,dest ip is "<<dest_ip<<" dest port is"<<dest_port;
          num++;
        }else {
          VLOG(5) << "send heart beat succ,dest ip is "<<dest_ip<<" dest port is"<<dest_port;
        }
      }
      setDisconnNums(num);
    }
    usleep(kHeartBeatInterval);
  }
}
}
