#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace flotilla::txn {

// Transactional keyspace layout (disjoint from raw KV user keys):
//   "!l" + ekey                 lock record
//   "!w" + ekey + ~commit_ts    write record
//   "!d" + ekey + ~start_ts     data record
// ekey escape-encodes the user key (0x00 -> 0x00 0xFF, terminator 0x00 0x01)
// so concatenated suffixes preserve user-key ordering; ~ts is the bitwise NOT
// of the big-endian timestamp so newer versions sort first.

inline constexpr char kLockPrefix[] = "!l";
inline constexpr char kWritePrefix[] = "!w";
inline constexpr char kDataPrefix[] = "!d";

std::string EscapeKey(std::string_view user_key);
// Decodes an escaped key; returns bytes consumed via *consumed, or false.
bool UnescapeKey(std::string_view data, std::string* user_key, size_t* consumed);

std::string LockKey(std::string_view user_key);
std::string WriteKey(std::string_view user_key, uint64_t commit_ts);
std::string DataKey(std::string_view user_key, uint64_t start_ts);
// Parses "!w"-prefixed keys back into (user_key, commit_ts).
bool ParseWriteKey(std::string_view key, std::string* user_key, uint64_t* commit_ts);

struct LockRecord {
  uint64_t start_ts = 0;
  uint8_t op = 0;  // 1 put, 2 delete
  uint64_t wall_ms = 0;
  std::string primary;
};

enum WriteKind : uint8_t { kWritePut = 1, kWriteDelete = 2, kWriteRollback = 3 };

struct WriteRecord {
  WriteKind kind = kWritePut;
  uint64_t start_ts = 0;
};

std::string EncodeLock(const LockRecord& lock);
bool DecodeLock(std::string_view data, LockRecord* lock);
std::string EncodeWrite(const WriteRecord& write);
bool DecodeWrite(std::string_view data, WriteRecord* write);

}  // namespace flotilla::txn
