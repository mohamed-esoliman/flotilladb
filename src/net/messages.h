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
  kTxnTs = 8,        // allocate a timestamp
  kTxnGet = 9,       // snapshot read at ts
  kTxnPrewrite = 10, // percolator prewrite (ts=start_ts, ts2=wall_ms)
  kTxnCommit = 11,   // percolator commit (ts=start_ts, ts2=commit_ts)
  kTxnRollback = 12, // percolator rollback (ts=start_ts)
  kTxnScan = 13,     // snapshot range scan at ts
  kTxnResolve = 14,  // resolve a lock via its primary (key=primary, ts=start_ts)
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
  uint64_t ts = 0;      // txn: start_ts / read ts
  uint64_t ts2 = 0;     // txn: commit_ts (COMMIT) or wall_ms (PREWRITE)
  std::string primary;  // txn: primary key (PREWRITE)
  uint8_t wop = 0;      // txn PREWRITE: 1 put, 2 delete
};

inline constexpr uint8_t kRequestNoForward = 1;

struct Response {
  uint8_t code = 0;  // 0 = OK, else mirrors Status::Code
  std::string leader_addr;
  std::string message;
  bool found = false;
  std::string value;
  std::vector<std::pair<std::string, std::string>> kvs;  // SCAN rows / STATUS fields
  uint64_t ts = 0;           // TXN_TS result / TXN_RESOLVE commit_ts (0 = rolled back)
  uint64_t lock_ts = 0;      // blocking lock's start_ts (with a Conflict code)
  std::string lock_primary;  // blocking lock's primary key
  std::string lock_key;      // the key the lock was found on

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
