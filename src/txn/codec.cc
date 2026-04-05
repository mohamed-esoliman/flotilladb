#include "txn/codec.h"

#include "common/coding.h"

namespace flotilla::txn {

std::string EscapeKey(std::string_view user_key) {
  std::string out;
  out.reserve(user_key.size() + 2);
  for (char c : user_key) {
    if (c == '\0') {
      out += '\0';
      out += '\xff';
    } else {
      out += c;
    }
  }
  out += '\0';
  out += '\x01';
  return out;
}

bool UnescapeKey(std::string_view data, std::string* user_key, size_t* consumed) {
  user_key->clear();
  for (size_t i = 0; i < data.size(); i++) {
    if (data[i] != '\0') {
      user_key->push_back(data[i]);
      continue;
    }
    if (i + 1 >= data.size()) return false;
    if (data[i + 1] == '\x01') {
      *consumed = i + 2;
      return true;
    }
    if (data[i + 1] == '\xff') {
      user_key->push_back('\0');
      i++;
      continue;
    }
    return false;
  }
  return false;
}

namespace {
void AppendInvertedTs(std::string* out, uint64_t ts) {
  uint64_t inv = ~ts;
  for (int i = 7; i >= 0; i--) out->push_back(static_cast<char>(inv >> (8 * i)));
}

uint64_t ReadInvertedTs(std::string_view bytes) {
  uint64_t inv = 0;
  for (int i = 0; i < 8; i++) inv = (inv << 8) | static_cast<uint8_t>(bytes[static_cast<size_t>(i)]);
  return ~inv;
}
}  // namespace

std::string LockKey(std::string_view user_key) {
  return kLockPrefix + EscapeKey(user_key);
}

std::string WriteKey(std::string_view user_key, uint64_t commit_ts) {
  std::string out = kWritePrefix + EscapeKey(user_key);
  AppendInvertedTs(&out, commit_ts);
  return out;
}

std::string DataKey(std::string_view user_key, uint64_t start_ts) {
  std::string out = kDataPrefix + EscapeKey(user_key);
  AppendInvertedTs(&out, start_ts);
  return out;
}

bool ParseWriteKey(std::string_view key, std::string* user_key, uint64_t* commit_ts) {
  if (key.size() < 2 || key.substr(0, 2) != kWritePrefix) return false;
  size_t consumed = 0;
  if (!UnescapeKey(key.substr(2), user_key, &consumed)) return false;
  std::string_view rest = key.substr(2 + consumed);
  if (rest.size() != 8) return false;
  *commit_ts = ReadInvertedTs(rest);
  return true;
}

std::string EncodeLock(const LockRecord& lock) {
  std::string out;
  PutFixed64(&out, lock.start_ts);
  PutFixed8(&out, lock.op);
  PutFixed64(&out, lock.wall_ms);
  PutLengthPrefixed(&out, lock.primary);
  return out;
}

bool DecodeLock(std::string_view data, LockRecord* lock) {
  Decoder dec(data);
  lock->start_ts = dec.U64();
  lock->op = dec.U8();
  lock->wall_ms = dec.U64();
  lock->primary = dec.Str();
  return dec.ok() && dec.remaining() == 0;
}

std::string EncodeWrite(const WriteRecord& write) {
  std::string out;
  PutFixed8(&out, write.kind);
  PutFixed64(&out, write.start_ts);
  return out;
}

bool DecodeWrite(std::string_view data, WriteRecord* write) {
  Decoder dec(data);
  write->kind = static_cast<WriteKind>(dec.U8());
  write->start_ts = dec.U64();
  return dec.ok() && dec.remaining() == 0 && write->kind >= 1 && write->kind <= 3;
}

}  // namespace flotilla::txn
