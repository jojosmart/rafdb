#include "storage/rafdb/client/conhash_selector.h"

#include <vector>
#include "base/basictypes.h"
#include "base/hash.h"
#include "base/hash_tables.h"
#include "base/shared_ptr.h"
#include "base/logging.h"
#include "third_party/libconhash/conhash.h"

namespace {
const uint32 kVirtualNodeSize = 10000;
const uint8 kMaxServerStringSize = 63;
}

namespace rafdb {

ConhashSelector::ConhashSelector() {
  // conhash_.reset(conhash_init(HashFunc));
  conhash_ = conhash_init(HashFunc);
}

ConhashSelector::~ConhashSelector() {
  conhash_fini(conhash_);
  for (int i = 0; i < g_nodes_.size(); ++i) {
    if (g_nodes_[i] != NULL) {
      delete g_nodes_[i];
      g_nodes_[i] = NULL;
    }
  }
}

int64 ConhashSelector::HashFunc(const char *instr) {
  return base::Fingerprint(instr);
}

bool ConhashSelector::AddNode(const std::string& server) {
  if (server.size() > kMaxServerStringSize) {
    LOG(ERROR)<< "server too long: " << server;
    return false;
  }
  // Set Node
  if (map_servers_.find(server) == map_servers_.end()) {
    node_s* node = new node_s;
    conhash_set_node(node, server.c_str(), kVirtualNodeSize);
    map_servers_[server] = g_nodes_.size();
    g_nodes_.push_back(node);
    VLOG(6) << "g_nodes index " << map_servers_[server];
  }
  // Add Node
  // 0 : success, -1 failure , so we +1 to convert to bool
  VLOG(6) << g_nodes_[map_servers_[server]]->iden;
  return conhash_add_node(conhash_, g_nodes_[map_servers_[server]]) + 1;
}

bool ConhashSelector::DelNode(const std::string& server) {
  if (map_servers_.find(server) == map_servers_.end()) {
    LOG(ERROR)<< "server not find: " << server;
    return false;
  }
  return conhash_del_node(conhash_, g_nodes_[map_servers_[server]]);
}
bool ConhashSelector::GetNode(std::string& server, const std::string& query) {
  //VLOG(6) << "server: " << server;
  //VLOG(6) << "query: " << query;
  const node_s* node = conhash_lookup(conhash_, query.c_str());
  if (node) {
    server = node->iden;
    VLOG(4) << "node->iden: " <<  node->iden;
    VLOG(4) << "server: " << server;
    return true;
  } else {
    LOG(ERROR) << "GetNode Error, query is " << query;
    return false;
  }
}

}  // namespace rafdb
