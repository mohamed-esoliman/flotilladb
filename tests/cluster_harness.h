#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"
#include "net/tcp_server.h"
#include "server/sharded_node.h"
#include "testutil.h"

namespace flotilla::server::testing {

// In-process n-node cluster over loopback with real TCP transports.
class HarnessCluster {
 public:
  explicit HarnessCluster(const std::string& name, int n = 3,
                          uint64_t snapshot_interval = 8192)
      : dir_(name), n_(n), snapshot_interval_(snapshot_interval) {
    std::vector<uint16_t> client_ports(static_cast<size_t>(n)),
        raft_ports(static_cast<size_t>(n));
    std::vector<int> fds;
    for (int i = 0; i < n; i++) {
      int fd;
      EXPECT_TRUE(
          net::Listen("127.0.0.1", 0, &fd, &client_ports[static_cast<size_t>(i)]).ok());
      fds.push_back(fd);
      EXPECT_TRUE(
          net::Listen("127.0.0.1", 0, &fd, &raft_ports[static_cast<size_t>(i)]).ok());
      fds.push_back(fd);
    }
    for (int fd : fds) net::CloseSocket(fd);

    for (int i = 1; i <= n; i++) {
      NodeInfo info;
      info.id = static_cast<raft::NodeId>(i);
      info.client_addr =
          "127.0.0.1:" + std::to_string(client_ports[static_cast<size_t>(i - 1)]);
      info.raft_addr =
          "127.0.0.1:" + std::to_string(raft_ports[static_cast<size_t>(i - 1)]);
      config_.nodes.push_back(info);
    }
    nodes_.resize(static_cast<size_t>(n) + 1);
    client_servers_.resize(static_cast<size_t>(n) + 1);
    for (int i = 1; i <= n; i++) StartNode(i);
  }

  ~HarnessCluster() {
    for (int i = 1; i <= n_; i++) StopNode(i);
  }

  void StartNode(int id) {
    ShardedNode::NodeOptions options;
    options.data_dir = dir_.file("node" + std::to_string(id));
    options.id = static_cast<raft::NodeId>(id);
    options.cluster = config_;
    options.db_options.fsync_writes = false;
    options.tick_ms = 5;
    options.request_timeout_ms = 3000;
    options.snapshot_interval_entries = snapshot_interval_;
    ASSERT_TRUE(ShardedNode::Start(options, &nodes_[static_cast<size_t>(id)]).ok());

    auto server = std::make_unique<net::TcpServer>();
    std::string host;
    uint16_t port;
    ASSERT_TRUE(net::ParseAddr(config_.nodes[static_cast<size_t>(id - 1)].client_addr,
                               &host, &port)
                    .ok());
    ShardedNode* node = nodes_[static_cast<size_t>(id)].get();
    ASSERT_TRUE(server
                    ->Start(host, port,
                            [node](std::string_view req, std::string* resp) {
                              return node->HandleClientFrame(req, resp);
                            })
                    .ok());
    client_servers_[static_cast<size_t>(id)] = std::move(server);
  }

  void StopNode(int id) {
    if (client_servers_[static_cast<size_t>(id)]) {
      client_servers_[static_cast<size_t>(id)]->Stop();
      client_servers_[static_cast<size_t>(id)].reset();
    }
    if (nodes_[static_cast<size_t>(id)]) {
      nodes_[static_cast<size_t>(id)]->Stop();
      nodes_[static_cast<size_t>(id)].reset();
    }
  }

  int WaitForLeader(int timeout_ms = 5000) {
    for (int waited = 0; waited < timeout_ms; waited += 20) {
      for (int i = 1; i <= n_; i++) {
        if (nodes_[static_cast<size_t>(i)] &&
            nodes_[static_cast<size_t>(i)]->IsLeader()) {
          return i;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return 0;
  }

  bool WaitForGroupCount(size_t count, int timeout_ms = 10000) {
    for (int waited = 0; waited < timeout_ms; waited += 50) {
      bool all = true;
      for (int i = 1; i <= n_; i++) {
        if (!nodes_[static_cast<size_t>(i)] ||
            nodes_[static_cast<size_t>(i)]->GroupCount() < count) {
          all = false;
        }
      }
      if (all) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
  }

  std::vector<std::string> ClientAddrs() const {
    std::vector<std::string> addrs;
    for (const auto& node : config_.nodes) addrs.push_back(node.client_addr);
    return addrs;
  }

  test::TempDir dir_;
  int n_;
  uint64_t snapshot_interval_;
  ClusterConfig config_;
  std::vector<std::unique_ptr<ShardedNode>> nodes_;
  std::vector<std::unique_ptr<net::TcpServer>> client_servers_;
};

}  // namespace flotilla::server::testing
