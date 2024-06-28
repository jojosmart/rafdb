
#ifndef STORAGE_RAFDB_CLIENT_CONHASH_SELECTOR_H_
#define STORAGE_RAFDB_CLIENT_CONHASH_SELECTOR_H_

#include <vector>
#include <string>
#include "base/basictypes.h"
#include "base/hash.h"
#include "base/hash_tables.h"
#include "base/shared_ptr.h"
#include "base/scoped_ptr.h"
#include "third_party/libconhash/conhash.h"
#include "third_party/libconhash/conhash_inter.h"

namespace rafdb {

class ConhashSelector {
 public:
  ConhashSelector();
  ~ConhashSelector();
  static int64 HashFunc(const char *instr);
  bool AddNode(const std::string& server);
  bool DelNode(const std::string& server);
  bool GetNode(std::string& server, const std::string& query);
 private:
  std::vector<node_s*> g_nodes_;
  base::hash_map<std::string, uint32> map_servers_;
  conhash_s* conhash_;

  DISALLOW_COPY_AND_ASSIGN(ConhashSelector);
};

}  // namespace rafdb

#endif  // TS_SCHEDULER_INSTANT_CONHASH_SELECTOR_H_
