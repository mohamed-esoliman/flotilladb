#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>

#include <unistd.h>

#include "common/logger.h"
#include "net/socket.h"
#include "net/tcp_server.h"
#include "server/kv_service.h"
#include "storage/db.h"

namespace {

volatile sig_atomic_t g_stop = 0;

void OnSignal(int) { g_stop = 1; }

void Usage(const char* argv0) {
  fprintf(stderr,
          "usage: %s --data-dir <dir> --listen <host:port> [--verbose]\n"
          "Runs a standalone single-node FlotillaDB server (no consensus).\n",
          argv0);
}

}  // namespace

int main(int argc, char** argv) {
  std::string data_dir;
  std::string listen_addr = "127.0.0.1:4001";
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (arg == "--listen" && i + 1 < argc) {
      listen_addr = argv[++i];
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

  signal(SIGINT, OnSignal);
  signal(SIGTERM, OnSignal);
  while (!g_stop) pause();

  FLOG_INFO("shutting down");
  server.Stop();
  return 0;
}
