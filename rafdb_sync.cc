#include "storage/rafdb/rafdb_sync.h"

#include <string>
#include "base/yr.h"
#include "base/flags.h"
#include "base/logging.h"
#include "util/gtl/stl_util-inl.h"


namespace {
const int kFrameSize = 128 * 1024;
const std::string kInvalidID = "NULL";
const int kMaxRafdbSyncSize = 65535;
const int kFlushInterval = 10;
}

namespace rafdb {

bool RafdbSync::Connect() {
  if (!connected_) {
    socket_.reset(new TSocket(host_, port_));
    socket_->setConnTimeout(30);
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
      transport_->close();
    }
  }
  return connected_;
}

}
// namespace rafdb
