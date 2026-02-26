#include "server/cluster_config.h"

#include <sstream>

#include "common/fs.h"
#include "net/socket.h"

namespace flotilla::server {

Status ParseClusterConfig(const std::string& content, ClusterConfig* out) {
  out->nodes.clear();
  std::istringstream in(content);
  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    lineno++;
    size_t hash = line.find('#');
    if (hash != std::string::npos) line = line.substr(0, hash);
    std::istringstream ls(line);
    std::string tag;
    if (!(ls >> tag)) continue;
    if (tag != "node") {
      return Status::InvalidArgument("config line " + std::to_string(lineno) +
                                     ": expected 'node'");
    }
    NodeInfo info;
    if (!(ls >> info.id >> info.client_addr >> info.raft_addr) || info.id == 0) {
      return Status::InvalidArgument("config line " + std::to_string(lineno) +
                                     ": expected 'node <id> <client_addr> <raft_addr>'");
    }
    std::string host;
    uint16_t port;
    if (!net::ParseAddr(info.client_addr, &host, &port).ok() ||
        !net::ParseAddr(info.raft_addr, &host, &port).ok()) {
      return Status::InvalidArgument("config line " + std::to_string(lineno) +
                                     ": bad address");
    }
    for (const auto& n : out->nodes) {
      if (n.id == info.id) {
        return Status::InvalidArgument("duplicate node id " + std::to_string(info.id));
      }
    }
    out->nodes.push_back(std::move(info));
  }
  if (out->nodes.empty()) return Status::InvalidArgument("empty cluster config");
  return Status::OK();
}

Status LoadClusterConfig(const std::string& path, ClusterConfig* out) {
  std::string content;
  Status s = ReadFileToString(path, &content);
  if (!s.ok()) return s;
  return ParseClusterConfig(content, out);
}

}  // namespace flotilla::server
