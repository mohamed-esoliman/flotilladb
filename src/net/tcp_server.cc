#include "net/tcp_server.h"

#include <sys/socket.h>
#include <unistd.h>

#include "net/frame.h"
#include "net/socket.h"

namespace flotilla::net {

Status TcpServer::Start(const std::string& host, uint16_t port, FrameHandler handler) {
  handler_ = std::move(handler);
  Status s = Listen(host, port, &listen_fd_, &port_);
  if (!s.ok()) return s;
  accept_thread_ = std::thread(&TcpServer::AcceptLoop, this);
  return Status::OK();
}

void TcpServer::Stop() {
  if (stopping_.exchange(true)) return;
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (int fd : conn_fds_) ::shutdown(fd, SHUT_RDWR);
  }
  if (accept_thread_.joinable()) accept_thread_.join();
  std::vector<std::thread> threads;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    threads.swap(conn_threads_);
  }
  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }
}

void TcpServer::AcceptLoop() {
  while (!stopping_.load()) {
    int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      if (stopping_.load()) return;
      continue;
    }
    DisableSigpipe(fd);
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_.load()) {
      ::close(fd);
      return;
    }
    conn_fds_.insert(fd);
    conn_threads_.emplace_back(&TcpServer::ConnLoop, this, fd);
  }
}

void TcpServer::ConnLoop(int fd) {
  std::string req, resp;
  while (!stopping_.load()) {
    if (!ReadFrame(fd, &req).ok()) break;
    resp.clear();
    if (!handler_(req, &resp)) break;
    // An empty response means a one-way message (raft traffic): nothing to write.
    if (!resp.empty() && !WriteFrame(fd, resp).ok()) break;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    conn_fds_.erase(fd);
  }
  ::close(fd);
}

}  // namespace flotilla::net
