#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"

namespace flotilla::net {

// Parses "host:port". Host may be a hostname or dotted quad.
Status ParseAddr(const std::string& addr, std::string* host, uint16_t* port);

// Returns a listening fd bound to host:port. Port 0 picks an ephemeral port;
// *bound_port reports the actual one.
Status Listen(const std::string& host, uint16_t port, int* fd, uint16_t* bound_port);

Status Connect(const std::string& host, uint16_t port, int* fd);
Status Connect(const std::string& addr, int* fd);

Status ReadFull(int fd, void* buf, size_t n);
Status WriteFull(int fd, const void* buf, size_t n);

void CloseSocket(int fd);

// Suppresses SIGPIPE for this socket where supported (macOS); Linux relies on
// MSG_NOSIGNAL inside WriteFull. Call on every accepted socket.
void DisableSigpipe(int fd);

}  // namespace flotilla::net
