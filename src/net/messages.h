#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"

namespace flotilla::net {

enum class MsgType : uint8_t {
  kGet = 1,
  kPut = 2,
  kDelete = 3,
  kScan = 4,
  kStatus = 5,
  kSplit = 6,   // admin: split the range containing key at key
  kRanges = 7,  // list range descriptors (this node's view)
  kResponse = 16,
  kRaft = 32,
};

struct Request {
  MsgType type = MsgType::kGet;
  std::string key;
  std::string value;    // PUT
  std::string end_key;  // SCAN, exclusive, empty = unbounded
  uint32_t limit = 0;   // SCAN, 0 = server default
  uint8_t flags = 0;    // bit 0: kNoForward (single-range sub-scan)
};

inline constexpr uint8_t kRequestNoForward = 1;

struct Response {
  uint8_t code = 0;  // 0 = OK, else mirrors Status::Code
  std::string leader_addr;
  std::string message;
  bool found = false;
  std::string value;
  std::vector<std::pair<std::string, std::string>> kvs;  // SCAN rows / STATUS fields

  bool ok() const { return code == 0; }
  Status ToStatus() const;
  static Response FromStatus(const Status& s);
};

std::string EncodeRequest(const Request& req);
bool DecodeRequest(std::string_view payload, Request* req);

std::string EncodeResponse(const Response& resp);
bool DecodeResponse(std::string_view payload, Response* resp);

// Raft messages ride the same framing with their own opaque body.
std::string EncodeRaftFrame(std::string_view body);
bool DecodeIsRaftFrame(std::string_view payload, std::string_view* body);

}  // namespace flotilla::net
