#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"

namespace flotilla::net {

// Thread-per-connection frame server. The handler is called once per request
// frame and must fill *resp (written back as one frame); returning false
// closes the connection. The handler runs on the connection's thread and may
// block; it must be safe to call concurrently from many threads.
class TcpServer {
 public:
  using FrameHandler = std::function<bool(std::string_view req, std::string* resp)>;

  TcpServer() = default;
  ~TcpServer() { Stop(); }

  Status Start(const std::string& host, uint16_t port, FrameHandler handler);
  void Stop();

  uint16_t port() const { return port_; }

 private:
  void AcceptLoop();
  void ConnLoop(int fd);

  FrameHandler handler_;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stopping_{false};
  std::thread accept_thread_;
  std::mutex mutex_;
  std::set<int> conn_fds_;
  std::vector<std::thread> conn_threads_;
};

}  // namespace flotilla::net
