#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>

#include <unistd.h>

#include "common/logger.h"
#include "net/socket.h"
#include "net/tcp_server.h"
#include "server/cluster_config.h"
#include "server/kv_service.h"
#include "server/sharded_node.h"
#include "storage/db.h"

namespace {

volatile sig_atomic_t g_stop = 0;

void OnSignal(int) { g_stop = 1; }

void Usage(const char* argv0) {
  fprintf(stderr,
          "usage:\n"
          "  cluster:    %s --config <cluster.conf> --node-id <n> --data-dir <dir>\n"
          "  standalone: %s --data-dir <dir> --listen <host:port>\n"
          "options: --verbose\n",
          argv0, argv0);
}

int RunStandalone(const std::string& data_dir, const std::string& listen_addr) {
  std::string host;
  uint16_t port = 0;
  if (auto s = flotilla::net::ParseAddr(listen_addr, &host, &port); !s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return 2;
  }
  std::unique_ptr<flotilla::storage::DB> db;
  flotilla::storage::Options options;
  if (auto s = flotilla::storage::DB::Open(options, data_dir, &db); !s.ok()) {
    fprintf(stderr, "open db: %s\n", s.ToString().c_str());
    return 1;
  }
  flotilla::server::LocalKvService service(db.get(), listen_addr);
  flotilla::net::TcpServer server;
  auto s = server.Start(host, port, [&service](std::string_view req, std::string* resp) {
    return service.HandleFrame(req, resp);
  });
  if (!s.ok()) {
    fprintf(stderr, "start server: %s\n", s.ToString().c_str());
    return 1;
  }
  FLOG_INFO("flotilladb single-node listening on %s data-dir %s", listen_addr.c_str(),
            data_dir.c_str());
  while (!g_stop) pause();
  FLOG_INFO("shutting down");
  server.Stop();
  return 0;
}

int RunCluster(const std::string& config_path, flotilla::raft::NodeId node_id,
               const std::string& data_dir, uint64_t snapshot_interval) {
  flotilla::server::ShardedNode::NodeOptions options;
  options.data_dir = data_dir;
  options.id = node_id;
  if (snapshot_interval > 0) options.snapshot_interval_entries = snapshot_interval;
  if (auto s = flotilla::server::LoadClusterConfig(config_path, &options.cluster);
      !s.ok()) {
    fprintf(stderr, "load config: %s\n", s.ToString().c_str());
    return 2;
  }
  const flotilla::server::NodeInfo* self = options.cluster.Find(node_id);
  if (self == nullptr) {
    fprintf(stderr, "node id %u not in %s\n", node_id, config_path.c_str());
    return 2;
  }

  std::unique_ptr<flotilla::server::ShardedNode> node;
  if (auto s = flotilla::server::ShardedNode::Start(options, &node); !s.ok()) {
    fprintf(stderr, "start raft node: %s\n", s.ToString().c_str());
    return 1;
  }

  std::string host;
  uint16_t port = 0;
  if (auto s = flotilla::net::ParseAddr(self->client_addr, &host, &port); !s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return 2;
  }
  flotilla::net::TcpServer client_server;
  auto s = client_server.Start(host, port,
                               [&node](std::string_view req, std::string* resp) {
                                 return node->HandleClientFrame(req, resp);
                               });
  if (!s.ok()) {
    fprintf(stderr, "start client server: %s\n", s.ToString().c_str());
    return 1;
  }
  FLOG_INFO("flotilladb node %u: clients %s raft %s data-dir %s", node_id,
            self->client_addr.c_str(), self->raft_addr.c_str(), data_dir.c_str());

  while (!g_stop) pause();
  FLOG_INFO("shutting down");
  client_server.Stop();
  node->Stop();
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::string data_dir;
  std::string listen_addr;
  std::string config_path;
  long node_id = 0;
  long snapshot_interval = 0;
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (arg == "--listen" && i + 1 < argc) {
      listen_addr = argv[++i];
    } else if (arg == "--config" && i + 1 < argc) {
      config_path = argv[++i];
    } else if (arg == "--node-id" && i + 1 < argc) {
      node_id = atol(argv[++i]);
    } else if (arg == "--snapshot-interval" && i + 1 < argc) {
      snapshot_interval = atol(argv[++i]);
    } else if (arg == "--verbose") {
      flotilla::Logger::MinLevel() = flotilla::LogLevel::kDebug;
    } else {
      Usage(argv[0]);
      return 2;
    }
  }
  if (data_dir.empty()) {
    Usage(argv[0]);
    return 2;
  }

  signal(SIGINT, OnSignal);
  signal(SIGTERM, OnSignal);
  signal(SIGPIPE, SIG_IGN);

  if (!config_path.empty()) {
    if (node_id <= 0) {
      Usage(argv[0]);
      return 2;
    }
    return RunCluster(config_path, static_cast<flotilla::raft::NodeId>(node_id),
                      data_dir, static_cast<uint64_t>(snapshot_interval));
  }
  if (listen_addr.empty()) listen_addr = "127.0.0.1:4001";
  return RunStandalone(data_dir, listen_addr);
}
