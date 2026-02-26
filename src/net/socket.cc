#include "net/socket.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace flotilla::net {

namespace {
Status Errno(const std::string& what) {
  return Status::IOError(what + ": " + std::strerror(errno));
}
}  // namespace

// A peer closing its end must surface as a write error, never SIGPIPE.
void DisableSigpipe(int fd) {
#ifdef SO_NOSIGPIPE
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#else
  (void)fd;
#endif
}

Status ParseAddr(const std::string& addr, std::string* host, uint16_t* port) {
  size_t colon = addr.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 == addr.size()) {
    return Status::InvalidArgument("bad address: " + addr);
  }
  *host = addr.substr(0, colon);
  char* end = nullptr;
  long p = strtol(addr.c_str() + colon + 1, &end, 10);
  if (*end != '\0' || p < 0 || p > 65535) {
    return Status::InvalidArgument("bad port in address: " + addr);
  }
  *port = static_cast<uint16_t>(p);
  return Status::OK();
}

Status Listen(const std::string& host, uint16_t port, int* fd, uint16_t* bound_port) {
  int s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) return Errno("socket");
  int one = 1;
  ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
    ::close(s);
    return Status::InvalidArgument("bad listen host: " + host);
  }
  if (::bind(s, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
    Status st = Errno("bind " + host + ":" + std::to_string(port));
    ::close(s);
    return st;
  }
  if (::listen(s, 128) != 0) {
    Status st = Errno("listen");
    ::close(s);
    return st;
  }
  if (bound_port != nullptr) {
    sockaddr_in got{};
    socklen_t len = sizeof(got);
    if (::getsockname(s, reinterpret_cast<sockaddr*>(&got), &len) != 0) {
      Status st = Errno("getsockname");
      ::close(s);
      return st;
    }
    *bound_port = ntohs(got.sin_port);
  }
  *fd = s;
  return Status::OK();
}

Status Connect(const std::string& host, uint16_t port, int* fd) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  std::string port_str = std::to_string(port);
  int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
  if (rc != 0) {
    return Status::IOError("resolve " + host + ": " + gai_strerror(rc));
  }
  Status status = Status::IOError("no addresses for " + host);
  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (s < 0) {
      status = Errno("socket");
      continue;
    }
    if (::connect(s, ai->ai_addr, ai->ai_addrlen) == 0) {
      int one = 1;
      ::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      DisableSigpipe(s);
      *fd = s;
      status = Status::OK();
      break;
    }
    status = Errno("connect " + host + ":" + port_str);
    ::close(s);
  }
  ::freeaddrinfo(res);
  return status;
}

Status Connect(const std::string& addr, int* fd) {
  std::string host;
  uint16_t port;
  Status s = ParseAddr(addr, &host, &port);
  if (!s.ok()) return s;
  return Connect(host, port, fd);
}

Status ReadFull(int fd, void* buf, size_t n) {
  char* p = static_cast<char*>(buf);
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::read(fd, p + got, n - got);
    if (r == 0) return Status::IOError("connection closed");
    if (r < 0) {
      if (errno == EINTR) continue;
      return Errno("read");
    }
    got += static_cast<size_t>(r);
  }
  return Status::OK();
}

Status WriteFull(int fd, const void* buf, size_t n) {
  const char* p = static_cast<const char*>(buf);
  size_t sent = 0;
#ifdef MSG_NOSIGNAL
  constexpr int kFlags = MSG_NOSIGNAL;
#else
  constexpr int kFlags = 0;
#endif
  while (sent < n) {
    ssize_t r = ::send(fd, p + sent, n - sent, kFlags);
    if (r < 0) {
      if (errno == EINTR) continue;
      return Errno("write");
    }
    sent += static_cast<size_t>(r);
  }
  return Status::OK();
}

void CloseSocket(int fd) {
  if (fd >= 0) ::close(fd);
}

}  // namespace flotilla::net
