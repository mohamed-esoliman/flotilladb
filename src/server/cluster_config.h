#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "raft/raft.h"

namespace flotilla::server {

struct NodeInfo {
  raft::NodeId id = 0;
  std::string client_addr;
  std::string raft_addr;
};

struct ClusterConfig {
  std::vector<NodeInfo> nodes;

  const NodeInfo* Find(raft::NodeId id) const {
    for (const auto& n : nodes) {
      if (n.id == id) return &n;
    }
    return nullptr;
  }
};

// Text format, one line per node, '#' comments:
//   node <id> <client_host:port> <raft_host:port>
Status ParseClusterConfig(const std::string& content, ClusterConfig* out);
Status LoadClusterConfig(const std::string& path, ClusterConfig* out);

}  // namespace flotilla::server
