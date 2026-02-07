#include "storage/sstable.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/coding.h"
#include "storage/bloom.h"

namespace flotilla::storage {

namespace {

constexpr uint64_t kTableMagic = 0xF107111ADBull;
constexpr uint32_t kTableVersion = 1;
constexpr size_t kFooterSize = 8 + 4 + 8 + 4 + 8 + 4 + 8;  // 44 bytes

Status Errno(const std::string& what) {
  return Status::IOError(what + ": " + std::strerror(errno));
}

void AppendEntry(std::string* dst, const Entry& e) {
  PutLengthPrefixed(dst, e.key);
  PutFixed64(dst, e.seqno);
  PutFixed8(dst, e.op);
  PutLengthPrefixed(dst, e.value);
}

bool ParseEntry(Decoder* dec, Entry* e) {
  e->key = dec->Str();
  e->seqno = dec->U64();
  e->op = static_cast<Op>(dec->U8());
  e->value = dec->Str();
  return dec->ok() && (e->op == kPut || e->op == kDelete);
}

}  // namespace

TableBuilder::TableBuilder(std::string path, TableOptions opts)
    : path_(std::move(path)), opts_(opts) {
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd_ < 0) status_ = Errno("open sst " + path_);
}

TableBuilder::~TableBuilder() {
  if (fd_ >= 0) ::close(fd_);
}

Status TableBuilder::WriteRaw(std::string_view data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::write(fd_, data.data() + off, data.size() - off);
    if (n < 0) return Errno("write sst " + path_);
    off += static_cast<size_t>(n);
  }
  offset_ += data.size();
  return Status::OK();
}

Status TableBuilder::FlushBlock() {
  if (block_.empty()) return Status::OK();
  PutLengthPrefixed(&index_, block_first_key_);
  PutFixed64(&index_, offset_);
  PutFixed32(&index_, static_cast<uint32_t>(block_.size()));
  Status s = WriteRaw(block_);
  block_.clear();
  block_first_key_.clear();
  return s;
}

Status TableBuilder::Add(const Entry& e) {
  if (!status_.ok()) return status_;
  if (block_.empty()) block_first_key_ = e.key;
  if (entries_ == 0) smallest_ = e.key;
  largest_ = e.key;
  if (keys_.empty() || keys_.back() != e.key) keys_.push_back(e.key);
  AppendEntry(&block_, e);
  entries_++;
  if (block_.size() >= opts_.block_size) status_ = FlushBlock();
  return status_;
}

Status TableBuilder::Finish(TableMeta* meta) {
  if (!status_.ok()) return status_;
  status_ = FlushBlock();
  if (!status_.ok()) return status_;

  uint64_t index_off = offset_;
  uint32_t index_size = static_cast<uint32_t>(index_.size());
  status_ = WriteRaw(index_);
  if (!status_.ok()) return status_;

  std::vector<std::string_view> key_views(keys_.begin(), keys_.end());
  std::string bloom = BuildBloom(key_views, opts_.bloom_bits_per_key);
  uint64_t bloom_off = offset_;
  uint32_t bloom_size = static_cast<uint32_t>(bloom.size());
  status_ = WriteRaw(bloom);
  if (!status_.ok()) return status_;

  std::string footer;
  PutFixed64(&footer, index_off);
  PutFixed32(&footer, index_size);
  PutFixed64(&footer, bloom_off);
  PutFixed32(&footer, bloom_size);
  PutFixed64(&footer, entries_);
  PutFixed32(&footer, kTableVersion);
  PutFixed64(&footer, kTableMagic);
  status_ = WriteRaw(footer);
  if (!status_.ok()) return status_;

  if (::fsync(fd_) != 0) return status_ = Errno("fsync sst " + path_);
  ::close(fd_);
  fd_ = -1;
  finished_ = true;

  if (meta != nullptr) {
    meta->smallest = smallest_;
    meta->largest = largest_;
    meta->entries = entries_;
    meta->file_bytes = offset_;
  }
  return Status::OK();
}

void TableBuilder::Abandon() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  ::unlink(path_.c_str());
}

Status Table::Open(const std::string& path, std::shared_ptr<Table>* out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Errno("open sst " + path);
  auto table = std::shared_ptr<Table>(new Table());
  table->fd_ = fd;
  table->path_ = path;

  off_t file_size = ::lseek(fd, 0, SEEK_END);
  if (file_size < static_cast<off_t>(kFooterSize)) {
    return Status::Corruption("sst too small: " + path);
  }
  std::string footer;
  Status s = table->ReadBlock(static_cast<uint64_t>(file_size) - kFooterSize,
                              kFooterSize, &footer);
  if (!s.ok()) return s;

  Decoder dec(footer);
  uint64_t index_off = dec.U64();
  uint32_t index_size = dec.U32();
  uint64_t bloom_off = dec.U64();
  uint32_t bloom_size = dec.U32();
  table->num_entries_ = dec.U64();
  uint32_t version = dec.U32();
  uint64_t magic = dec.U64();
  if (!dec.ok() || magic != kTableMagic || version != kTableVersion) {
    return Status::Corruption("bad sst footer: " + path);
  }

  std::string index_data;
  s = table->ReadBlock(index_off, index_size, &index_data);
  if (!s.ok()) return s;
  Decoder idx(index_data);
  while (idx.remaining() > 0) {
    IndexEntry ie;
    ie.first_key = idx.Str();
    ie.offset = idx.U64();
    ie.size = idx.U32();
    if (!idx.ok()) return Status::Corruption("bad sst index: " + path);
    table->index_.push_back(std::move(ie));
  }

  s = table->ReadBlock(bloom_off, bloom_size, &table->bloom_);
  if (!s.ok()) return s;

  *out = table;
  return Status::OK();
}

Table::~Table() {
  if (fd_ >= 0) ::close(fd_);
}

Status Table::ReadBlock(uint64_t off, uint32_t size, std::string* out) const {
  out->resize(size);
  size_t got = 0;
  while (got < size) {
    ssize_t n = ::pread(fd_, out->data() + got, size - got,
                        static_cast<off_t>(off + got));
    if (n < 0) return Errno("pread sst " + path_);
    if (n == 0) return Status::Corruption("short read sst " + path_);
    got += static_cast<size_t>(n);
  }
  return Status::OK();
}

int Table::FindBlock(std::string_view key) const {
  int lo = 0, hi = static_cast<int>(index_.size()) - 1, ans = -1;
  while (lo <= hi) {
    int mid = (lo + hi) / 2;
    if (index_[static_cast<size_t>(mid)].first_key < key) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

class TableIterator : public InternalIterator {
 public:
  explicit TableIterator(std::shared_ptr<const Table> table) : table_(std::move(table)) {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    block_idx_ = 0;
    LoadBlockAndParse(0);
  }

  void Seek(std::string_view key) override {
    int slot = table_->FindBlock(key);
    if (slot < 0) slot = 0;
    block_idx_ = static_cast<size_t>(slot);
    LoadBlockAndParse(block_idx_);
    while (valid_ && entry_.key < key) Next();
  }

  void Next() override {
    if (!valid_) return;
    if (!ParseNext()) {
      block_idx_++;
      LoadBlockAndParse(block_idx_);
    }
  }

  const Entry& entry() const override { return entry_; }

 private:
  void LoadBlockAndParse(size_t idx) {
    valid_ = false;
    if (idx >= table_->index_.size()) return;
    const auto& ie = table_->index_[idx];
    if (!table_->ReadBlock(ie.offset, ie.size, &block_).ok()) return;
    pos_ = 0;
    if (!ParseNext()) valid_ = false;
  }

  bool ParseNext() {
    if (pos_ >= block_.size()) return false;
    Decoder dec(std::string_view(block_).substr(pos_));
    size_t before = dec.remaining();
    if (!ParseEntry(&dec, &entry_)) {
      valid_ = false;
      return false;
    }
    pos_ += before - dec.remaining();
    valid_ = true;
    return true;
  }

  std::shared_ptr<const Table> table_;
  size_t block_idx_ = 0;
  std::string block_;
  size_t pos_ = 0;
  Entry entry_;
  bool valid_ = false;
};

Status Table::Get(std::string_view key, Entry* out) const {
  if (!BloomMayContain(bloom_, key)) return Status::NotFound();
  TableIterator it(shared_from_this());
  it.Seek(key);
  if (it.Valid() && it.entry().key == key) {
    // Entries for one user key are sorted newest first, so Seek lands on the
    // newest version this table holds.
    *out = it.entry();
    return Status::OK();
  }
  return Status::NotFound();
}

InternalIterator* Table::NewIterator() const {
  return new TableIterator(shared_from_this());
}

}  // namespace flotilla::storage
