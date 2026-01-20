#include "storage/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/fs.h"

namespace flotilla::storage {

namespace {

Status Errno(const std::string& what) {
  return Status::IOError(what + ": " + std::strerror(errno));
}

std::string EncodePayload(const Entry& e) {
  std::string p;
  PutFixed64(&p, e.seqno);
  PutFixed8(&p, e.op);
  PutLengthPrefixed(&p, e.key);
  PutLengthPrefixed(&p, e.value);
  return p;
}

bool DecodePayload(std::string_view p, Entry* e) {
  Decoder dec(p);
  e->seqno = dec.U64();
  e->op = static_cast<Op>(dec.U8());
  e->key = dec.Str();
  e->value = dec.Str();
  if (!dec.ok() || dec.remaining() != 0) return false;
  return e->op == kPut || e->op == kDelete;
}

}  // namespace

Status WalWriter::Open(const std::string& path, std::unique_ptr<WalWriter>* out) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return Errno("open wal " + path);
  out->reset(new WalWriter(fd, path));
  return Status::OK();
}

WalWriter::~WalWriter() {
  if (fd_ >= 0) ::close(fd_);
}

Status WalWriter::Append(const Entry& entry) {
  std::string payload = EncodePayload(entry);
  std::string rec;
  PutFixed32(&rec, static_cast<uint32_t>(payload.size()));
  PutFixed32(&rec, Crc32(payload));
  rec += payload;
  size_t off = 0;
  while (off < rec.size()) {
    ssize_t n = ::write(fd_, rec.data() + off, rec.size() - off);
    if (n < 0) return Errno("write wal " + path_);
    off += static_cast<size_t>(n);
  }
  return Status::OK();
}

Status WalWriter::Sync() {
  if (::fsync(fd_) != 0) return Errno("fsync wal " + path_);
  return Status::OK();
}

Status RecoverWal(const std::string& path, std::vector<Entry>* entries) {
  std::string data;
  Status s = ReadFileToString(path, &data);
  if (s.IsNotFound()) return Status::OK();
  if (!s.ok()) return s;

  size_t pos = 0;
  size_t valid_end = 0;
  while (data.size() - pos >= 8) {
    uint32_t len = DecodeFixed32(data.data() + pos);
    uint32_t crc = DecodeFixed32(data.data() + pos + 4);
    if (data.size() - pos - 8 < len) break;
    std::string_view payload(data.data() + pos + 8, len);
    if (Crc32(payload) != crc) break;
    Entry e;
    if (!DecodePayload(payload, &e)) break;
    entries->push_back(std::move(e));
    pos += 8 + len;
    valid_end = pos;
  }

  if (valid_end < data.size()) {
    if (::truncate(path.c_str(), static_cast<off_t>(valid_end)) != 0) {
      return Errno("truncate wal " + path);
    }
  }
  return Status::OK();
}

}  // namespace flotilla::storage
