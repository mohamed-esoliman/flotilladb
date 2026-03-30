#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include "client/client.h"
#include "net/socket.h"
#include "net/tcp_server.h"
#include "server/sharded_node.h"
#include "testutil.h"

namespace flotilla::server {
namespace {

// Same in-process cluster as cluster_test, kept separate so shard scenarios
// can evolve independently.
class ShardCluster {
 public:
  explicit ShardCluster(const std::string& name, int n = 3) : dir_(name), n_(n) {
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

  ~ShardCluster() {
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
  ClusterConfig config_;
  std::vector<std::unique_ptr<ShardedNode>> nodes_;
  std::vector<std::unique_ptr<net::TcpServer>> client_servers_;
};

TEST(Shard, SplitLiveRangeKeepsAllData) {
  ShardCluster cluster("shard_split");
  ASSERT_NE(cluster.WaitForLeader(), 0);

  client::Client c(cluster.ClientAddrs());
  for (int i = 0; i < 60; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key%03d", i);
    ASSERT_TRUE(c.Put(buf, "v" + std::to_string(i)).ok());
  }

  ASSERT_TRUE(c.Split("key030").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));

  std::vector<std::pair<std::string, std::string>> ranges;
  ASSERT_TRUE(c.Ranges(&ranges).ok());
  ASSERT_EQ(ranges.size(), 2u);

  // Every key is still readable and writable, on both sides of the split.
  std::string v;
  ASSERT_TRUE(c.Get("key000", &v).ok());
  EXPECT_EQ(v, "v0");
  ASSERT_TRUE(c.Get("key030", &v).ok());
  EXPECT_EQ(v, "v30");
  ASSERT_TRUE(c.Get("key059", &v).ok());
  EXPECT_EQ(v, "v59");
  ASSERT_TRUE(c.Put("key010", "updated-left").ok());
  ASSERT_TRUE(c.Put("key045", "updated-right").ok());
  ASSERT_TRUE(c.Get("key010", &v).ok());
  EXPECT_EQ(v, "updated-left");
  ASSERT_TRUE(c.Get("key045", &v).ok());
  EXPECT_EQ(v, "updated-right");

  // Scans span both ranges in order.
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 60u);
  EXPECT_EQ(rows.front().first, "key000");
  EXPECT_EQ(rows.back().first, "key059");
  for (size_t i = 1; i < rows.size(); i++) EXPECT_GT(rows[i].first, rows[i - 1].first);

  rows.clear();
  ASSERT_TRUE(c.Scan("key025", "key035", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 10u);
  EXPECT_EQ(rows.front().first, "key025");
  EXPECT_EQ(rows.back().first, "key034");
}

TEST(Shard, MultipleSplitsAndDeepScan) {
  ShardCluster cluster("shard_multi");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());

  for (int i = 0; i < 90; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "m%03d", i);
    ASSERT_TRUE(c.Put(buf, std::to_string(i)).ok());
  }
  ASSERT_TRUE(c.Split("m030").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));
  ASSERT_TRUE(c.Split("m060").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(3));
  // Split the middle range again.
  ASSERT_TRUE(c.Split("m045").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(4));

  std::vector<std::pair<std::string, std::string>> ranges;
  ASSERT_TRUE(c.Ranges(&ranges).ok());
  EXPECT_EQ(ranges.size(), 4u);

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 90u);
  for (size_t i = 1; i < rows.size(); i++) EXPECT_GT(rows[i].first, rows[i - 1].first);

  std::string v;
  ASSERT_TRUE(c.Get("m044", &v).ok());
  EXPECT_EQ(v, "44");
  ASSERT_TRUE(c.Get("m046", &v).ok());
  EXPECT_EQ(v, "46");

  // Invalid splits are rejected.
  EXPECT_FALSE(c.Split("m030").ok());  // range start
}

TEST(Shard, SplitSurvivesNodeFailureAndRestart) {
  ShardCluster cluster("shard_failover");
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);
  client::Client c(cluster.ClientAddrs());

  for (int i = 0; i < 40; i++) {
    ASSERT_TRUE(c.Put("s" + std::to_string(100 + i), "v" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(c.Split("s120").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));
  ASSERT_TRUE(c.Put("s110", "left").ok());
  ASSERT_TRUE(c.Put("s130", "right").ok());

  // Kill a node entirely; both groups must keep serving.
  cluster.StopNode(leader);
  ASSERT_NE(cluster.WaitForLeader(10000), 0);
  std::string v;
  ASSERT_TRUE(c.Get("s110", &v).ok());
  EXPECT_EQ(v, "left");
  ASSERT_TRUE(c.Get("s130", &v).ok());
  EXPECT_EQ(v, "right");
  ASSERT_TRUE(c.Put("s135", "while-down").ok());

  // Restarted node must rebuild both groups from disk.
  cluster.StartNode(leader);
  ASSERT_TRUE(cluster.WaitForGroupCount(2));
  ASSERT_TRUE(c.Get("s135", &v).ok());
  EXPECT_EQ(v, "while-down");

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("s1", "s2", 0, &rows).ok());
  EXPECT_EQ(rows.size(), 40u);  // s100..s139; later writes hit existing keys
}

TEST(Shard, GroupsCanHaveDifferentLeaders) {
  ShardCluster cluster("shard_leaders");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());
  ASSERT_TRUE(c.Put("a1", "x").ok());
  ASSERT_TRUE(c.Put("z1", "y").ok());
  ASSERT_TRUE(c.Split("n").ok());
  ASSERT_TRUE(cluster.WaitForGroupCount(2));

  // Regardless of which node leads which group, ops on both sides work when
  // pointed at any single node (server redirects per group).
  for (int node = 1; node <= 3; node++) {
    client::Client single(
        {cluster.config_.nodes[static_cast<size_t>(node - 1)].client_addr});
    std::string v;
    ASSERT_TRUE(single.Put("a-n" + std::to_string(node), "1").ok());
    ASSERT_TRUE(single.Put("z-n" + std::to_string(node), "2").ok());
    ASSERT_TRUE(single.Get("a1", &v).ok());
    ASSERT_TRUE(single.Get("z1", &v).ok());
  }
}

}  // namespace
}  // namespace flotilla::server
