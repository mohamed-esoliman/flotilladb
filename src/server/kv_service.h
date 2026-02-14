#pragma once

#include <string>

#include "net/messages.h"
#include "storage/db.h"

namespace flotilla::server {

// Single-node request handler: applies client ops directly to one storage
// engine, no consensus. Used in standalone mode and by milestone 2 tests;
// the Raft-backed service supersedes it for clusters.
class LocalKvService {
 public:
  LocalKvService(storage::DB* db, std::string self_addr)
      : db_(db), self_addr_(std::move(self_addr)) {}

  // TcpServer::FrameHandler; returns false on malformed frames.
  bool HandleFrame(std::string_view payload, std::string* out);

  net::Response Handle(const net::Request& req);

 private:
  storage::DB* db_;
  std::string self_addr_;
};

inline constexpr uint32_t kDefaultScanLimit = 1000;

}  // namespace flotilla::server
