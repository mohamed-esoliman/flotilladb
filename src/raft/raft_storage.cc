#include "raft/raft_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/fs.h"

namespace flotilla::raft {

namespace {

Status Errno(const std::string& what) {
  return Status::IOError(what + ": " + std::strerror(errno));
}

std::string EncodeLogRecord(const LogEntry& e) {
  std::string payload;
  PutFixed64(&payload, e.term);
  PutFixed64(&payload, e.index);
  PutLengthPrefixed(&payload, e.command);
  std::string rec;
  PutFixed32(&rec, static_cast<uint32_t>(payload.size()));
  PutFixed32(&rec, Crc32(payload));
  rec += payload;
  return rec;
}

}  // namespace

std::string RaftStorage::Path(const char* name) const {
  return (std::filesystem::path(dir_) / name).string();
}

RaftStorage::~RaftStorage() {
  if (log_fd_ >= 0) ::close(log_fd_);
}

Status RaftStorage::Open(const std::string& dir, std::unique_ptr<RaftStorage>* out) {
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) return Status::IOError("create dir " + dir + ": " + ec.message());
  std::unique_ptr<RaftStorage> rs(new RaftStorage(dir));
  Status s = rs->Load();
  if (!s.ok()) return s;
  s = rs->OpenLogForAppend();
  if (!s.ok()) return s;
  *out = std::move(rs);
  return Status::OK();
}

Status RaftStorage::Load() {
  std::string data;
  Status s = ReadFileToString(Path("state"), &data);
  if (s.ok()) {
    Decoder dec(data);
    hard_.term = dec.U64();
    hard_.voted_for = dec.U32();
    uint32_t crc = dec.U32();
    if (!dec.ok() || dec.remaining() != 0 || crc != Crc32(data.substr(0, 12))) {
      return Status::Corruption("bad raft state file");
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  s = ReadFileToString(Path("snap"), &data);
  if (s.ok()) {
    Decoder dec(data);
    snap_.last_index = dec.U64();
    snap_.last_term = dec.U64();
    snap_.data = dec.Str();
    uint32_t crc = dec.U32();
    if (!dec.ok() || dec.remaining() != 0 ||
        crc != Crc32(std::string_view(data).substr(0, data.size() - 4))) {
      return Status::Corruption("bad raft snapshot file");
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  s = ReadFileToString(Path("log"), &data);
  if (s.IsNotFound()) return Status::OK();
  if (!s.ok()) return s;

  size_t pos = 0;
  size_t valid_end = 0;
  uint64_t expect = snap_.last_index + 1;
  while (data.size() - pos >= 8) {
    uint32_t len = DecodeFixed32(data.data() + pos);
    uint32_t crc = DecodeFixed32(data.data() + pos + 4);
    if (data.size() - pos - 8 < len) break;
    std::string_view payload(data.data() + pos + 8, len);
    if (Crc32(payload) != crc) break;
    Decoder dec(payload);
    LogEntry e;
    e.term = dec.U64();
    e.index = dec.U64();
    e.command = dec.Str();
    if (!dec.ok() || dec.remaining() != 0) break;
    pos += 8 + len;
    valid_end = pos;
    if (e.index < expect) continue;  // covered by a later snapshot; skip
    if (e.index != expect) break;    // gap: treat as torn tail
    entries_.push_back(std::move(e));
    expect++;
  }
  if (valid_end < data.size()) {
    if (::truncate(Path("log").c_str(), static_cast<off_t>(valid_end)) != 0) {
      return Errno("truncate raft log");
    }
  }
  return Status::OK();
}

Status RaftStorage::OpenLogForAppend() {
  log_fd_ = ::open(Path("log").c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (log_fd_ < 0) return Errno("open raft log");
  return Status::OK();
}

Status RaftStorage::SaveHardState(const HardState& hs) {
  std::string data;
  PutFixed64(&data, hs.term);
  PutFixed32(&data, hs.voted_for);
  PutFixed32(&data, Crc32(data));
  Status s = WriteFileAtomic(Path("state"), data);
  if (!s.ok()) return s;
  hard_ = hs;
  return Status::OK();
}

Status RaftStorage::Append(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return Status::OK();
  std::string buf;
  for (const auto& e : entries) buf += EncodeLogRecord(e);
  size_t off = 0;
  while (off < buf.size()) {
    ssize_t n = ::write(log_fd_, buf.data() + off, buf.size() - off);
    if (n < 0) return Errno("write raft log");
    off += static_cast<size_t>(n);
  }
  if (::fsync(log_fd_) != 0) return Errno("fsync raft log");
  for (const auto& e : entries) entries_.push_back(e);
  return Status::OK();
}

Status RaftStorage::RewriteLog() {
  std::string buf;
  for (const auto& e : entries_) buf += EncodeLogRecord(e);
  if (log_fd_ >= 0) {
    ::close(log_fd_);
    log_fd_ = -1;
  }
  Status s = WriteFileAtomic(Path("log"), buf);
  if (!s.ok()) return s;
  return OpenLogForAppend();
}

Status RaftStorage::TruncateFrom(uint64_t index) {
  while (!entries_.empty() && entries_.back().index >= index) entries_.pop_back();
  return RewriteLog();
}

Status RaftStorage::SaveSnapshot(const Snapshot& snap, bool drop_all) {
  std::string data;
  PutFixed64(&data, snap.last_index);
  PutFixed64(&data, snap.last_term);
  PutLengthPrefixed(&data, snap.data);
  PutFixed32(&data, Crc32(data));
  Status s = WriteFileAtomic(Path("snap"), data);
  if (!s.ok()) return s;
  snap_ = snap;

  if (drop_all) {
    entries_.clear();
  } else {
    size_t drop = 0;
    while (drop < entries_.size() && entries_[drop].index <= snap.last_index) drop++;
    entries_.erase(entries_.begin(), entries_.begin() + static_cast<long>(drop));
  }
  return RewriteLog();
}

Status RaftStorage::Persist(const Ready& ready) {
  if (ready.snapshot_to_apply.has_value()) {
    Status s = SaveSnapshot(*ready.snapshot_to_apply, true);
    if (!s.ok()) return s;
  }
  if (ready.truncate_from != 0) {
    Status s = TruncateFrom(ready.truncate_from);
    if (!s.ok()) return s;
  }
  Status s = Append(ready.entries_to_append);
  if (!s.ok()) return s;
  if (ready.hard_state_changed) {
    s = SaveHardState(ready.hard_state);
    if (!s.ok()) return s;
  }
  return Status::OK();
}

}  // namespace flotilla::raft
