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

// In-process 3-node cluster over loopback with real TCP transports.
class TestCluster {
 public:
  explicit TestCluster(const std::string& name, int n = 3,
                       uint64_t snapshot_interval = 8192)
      : dir_(name), n_(n), snapshot_interval_(snapshot_interval) {
    // Reserve ephemeral ports by binding listeners, then closing them right
    // before the real servers start.
    std::vector<uint16_t> client_ports(static_cast<size_t>(n)),
        raft_ports(static_cast<size_t>(n));
    std::vector<int> fds;
    for (int i = 0; i < n; i++) {
      int fd;
      EXPECT_TRUE(net::Listen("127.0.0.1", 0, &fd, &client_ports[static_cast<size_t>(i)]).ok());
      fds.push_back(fd);
      EXPECT_TRUE(net::Listen("127.0.0.1", 0, &fd, &raft_ports[static_cast<size_t>(i)]).ok());
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

  ~TestCluster() {
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
        if (nodes_[static_cast<size_t>(i)] && nodes_[static_cast<size_t>(i)]->IsLeader()) {
          return i;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return 0;
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

TEST(Cluster, WritesAndLinearizableReads) {
  TestCluster cluster("cluster_basic");
  ASSERT_NE(cluster.WaitForLeader(), 0);

  client::Client c(cluster.ClientAddrs());
  ASSERT_TRUE(c.Put("k1", "v1").ok());
  ASSERT_TRUE(c.Put("k2", "v2").ok());
  std::string v;
  ASSERT_TRUE(c.Get("k1", &v).ok());
  EXPECT_EQ(v, "v1");
  ASSERT_TRUE(c.Delete("k1").ok());
  EXPECT_TRUE(c.Get("k1", &v).IsNotFound());

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 1u);
  EXPECT_EQ(rows[0].first, "k2");

  std::vector<std::pair<std::string, std::string>> fields;
  ASSERT_TRUE(c.GetStatus(&fields).ok());
  bool saw_role = false;
  for (const auto& [k, val] : fields) {
    if (k == "role") {
      saw_role = true;
      EXPECT_EQ(val, "leader");  // redirects land the client on the leader
    }
  }
  EXPECT_TRUE(saw_role);
}

TEST(Cluster, FollowerRedirectsToLeader) {
  TestCluster cluster("cluster_redirect");
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);

  // Talk to a follower only; the client must follow the hint.
  int follower = leader == 1 ? 2 : 1;
  client::Client c({cluster.config_.nodes[static_cast<size_t>(follower - 1)].client_addr});
  ASSERT_TRUE(c.Put("via-follower", "worked").ok());
  std::string v;
  ASSERT_TRUE(c.Get("via-follower", &v).ok());
  EXPECT_EQ(v, "worked");
}

TEST(Cluster, LeaderKillFailoverPreservesWrites) {
  TestCluster cluster("cluster_failover");
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);

  client::Client c(cluster.ClientAddrs());
  for (int i = 0; i < 20; i++) {
    ASSERT_TRUE(c.Put("key" + std::to_string(i), "v" + std::to_string(i)).ok());
  }

  cluster.StopNode(leader);
  int new_leader = cluster.WaitForLeader(10000);
  ASSERT_NE(new_leader, 0);
  ASSERT_NE(new_leader, leader);

  std::string v;
  for (int i = 0; i < 20; i++) {
    ASSERT_TRUE(c.Get("key" + std::to_string(i), &v).ok()) << i;
    EXPECT_EQ(v, "v" + std::to_string(i));
  }
  ASSERT_TRUE(c.Put("after-failover", "yes").ok());

  // Old leader rejoins as follower and the cluster still serves.
  cluster.StartNode(leader);
  ASSERT_TRUE(c.Get("after-failover", &v).ok());
  EXPECT_EQ(v, "yes");
  ASSERT_TRUE(c.Put("after-rejoin", "also").ok());
}

TEST(Cluster, RestartedNodeRecoversFromDisk) {
  TestCluster cluster("cluster_recover");
  ASSERT_NE(cluster.WaitForLeader(), 0);
  client::Client c(cluster.ClientAddrs());
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(c.Put("persist" + std::to_string(i), "x").ok());
  }

  // Bounce every node one at a time; data must survive throughout.
  for (int id = 1; id <= 3; id++) {
    cluster.StopNode(id);
    ASSERT_NE(cluster.WaitForLeader(10000), 0);
    cluster.StartNode(id);
    std::string v;
    ASSERT_TRUE(c.Get("persist5", &v).ok()) << "after bouncing node " << id;
  }
}

TEST(Cluster, SnapshotCatchesUpDownedFollowerOverTcp) {
  // Snapshot every 40 applied entries so a downed follower falls behind the
  // leader's compacted log and must be caught up via InstallSnapshot.
  TestCluster cluster("cluster_snapshot", 3, 40);
  int leader = cluster.WaitForLeader();
  ASSERT_NE(leader, 0);

  int laggard = leader == 1 ? 2 : 1;
  cluster.StopNode(laggard);
  ASSERT_NE(cluster.WaitForLeader(10000), 0);

  client::Client c(cluster.ClientAddrs());
  for (int i = 0; i < 150; i++) {
    ASSERT_TRUE(c.Put("snap" + std::to_string(i), "v" + std::to_string(i)).ok()) << i;
  }

  cluster.StartNode(laggard);

  // The laggard must reach the current applied index; poll its own status.
  client::Client lc({cluster.config_.nodes[static_cast<size_t>(laggard - 1)].client_addr});
  uint64_t snapshot_index = 0;
  bool caught_up = false;
  for (int waited = 0; waited < 15000 && !caught_up; waited += 200) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::vector<std::pair<std::string, std::string>> fields;
    net::Request req;
    req.type = net::MsgType::kStatus;
    net::Response resp;
    if (!lc.Call(req, &resp).ok()) continue;
    uint64_t applied = 0;
    for (const auto& [k, v] : resp.kvs) {
      if (k == "applied_index") applied = strtoull(v.c_str(), nullptr, 10);
      if (k == "snapshot_index") snapshot_index = strtoull(v.c_str(), nullptr, 10);
    }
    caught_up = applied >= 150 && snapshot_index > 0;
  }
  EXPECT_TRUE(caught_up);
  EXPECT_GT(snapshot_index, 0u) << "follower should have installed a snapshot";

  // And its data is correct: read via the cluster after killing another node
  // so the laggard's vote/data must participate.
  std::string v;
  ASSERT_TRUE(c.Get("snap149", &v).ok());
  EXPECT_EQ(v, "v149");
  int other = 6 - leader - laggard;
  cluster.StopNode(other);
  ASSERT_NE(cluster.WaitForLeader(10000), 0);
  ASSERT_TRUE(c.Get("snap75", &v).ok());
  EXPECT_EQ(v, "v75");
  ASSERT_TRUE(c.Put("post-snap", "ok").ok());
}

TEST(Cluster, SustainedLoadWithTinyBuffersAndSnapshots) {
  // Small write buffers force constant flush/compaction while snapshots
  // compact the raft log: the milestone 5 hardening scenario.
  TestCluster cluster("cluster_sustained", 3, 100);
  for (int i = 1; i <= 3; i++) {
    // Rebuild nodes with aggressive storage options.
    cluster.StopNode(i);
  }
  for (int i = 1; i <= 3; i++) {
    ShardedNode::NodeOptions options;
    options.data_dir = cluster.dir_.file("node" + std::to_string(i));
    options.id = static_cast<raft::NodeId>(i);
    options.cluster = cluster.config_;
    options.db_options.fsync_writes = false;
    options.db_options.write_buffer_size = 4096;
    options.db_options.l0_compaction_trigger = 4;
    options.db_options.level_base_bytes = 16 << 10;
    options.tick_ms = 5;
    options.request_timeout_ms = 3000;
    options.snapshot_interval_entries = 100;
    ASSERT_TRUE(ShardedNode::Start(options, &cluster.nodes_[static_cast<size_t>(i)]).ok());
    auto server = std::make_unique<net::TcpServer>();
    std::string host;
    uint16_t port;
    ASSERT_TRUE(net::ParseAddr(cluster.config_.nodes[static_cast<size_t>(i - 1)].client_addr,
                               &host, &port)
                    .ok());
    ShardedNode* node = cluster.nodes_[static_cast<size_t>(i)].get();
    ASSERT_TRUE(server
                    ->Start(host, port,
                            [node](std::string_view req, std::string* resp) {
                              return node->HandleClientFrame(req, resp);
                            })
                    .ok());
    cluster.client_servers_[static_cast<size_t>(i)] = std::move(server);
  }
  ASSERT_NE(cluster.WaitForLeader(), 0);

  client::Client c(cluster.ClientAddrs());
  std::string big(300, 'x');
  for (int i = 0; i < 500; i++) {
    ASSERT_TRUE(c.Put("load" + std::to_string(i % 50), big + std::to_string(i)).ok())
        << i;
  }
  std::string v;
  ASSERT_TRUE(c.Get("load49", &v).ok());
  EXPECT_EQ(v, big + "499");
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 0, &rows).ok());
  EXPECT_EQ(rows.size(), 50u);
}

TEST(Cluster, ConcurrentClientsSequentialKeys) {
  TestCluster cluster("cluster_concurrent");
  ASSERT_NE(cluster.WaitForLeader(), 0);

  constexpr int kThreads = 4;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&, t] {
      client::Client c(cluster.ClientAddrs());
      for (int i = 0; i < 15; i++) {
        std::string key = "t" + std::to_string(t) + "-" + std::to_string(i);
        if (!c.Put(key, "v").ok()) {
          failures.fetch_add(1);
          continue;
        }
        std::string v;
        if (!c.Get(key, &v).ok() || v != "v") failures.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(failures.load(), 0);

  client::Client c(cluster.ClientAddrs());
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(c.Scan("", "", 1000, &rows).ok());
  EXPECT_EQ(rows.size(), static_cast<size_t>(kThreads) * 15);
}

}  // namespace
}  // namespace flotilla::server
