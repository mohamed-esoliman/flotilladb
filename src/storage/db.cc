#include "storage/db.h"

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <set>

#include "storage/merging_iterator.h"

namespace flotilla::storage {

namespace {

constexpr int kNumLevels = 7;

bool ParseFileName(const std::string& name, uint64_t* number, std::string* ext) {
  size_t dot = name.find('.');
  if (dot == std::string::npos || dot == 0) return false;
  for (size_t i = 0; i < dot; i++) {
    if (!isdigit(static_cast<unsigned char>(name[i]))) return false;
  }
  *number = strtoull(name.substr(0, dot).c_str(), nullptr, 10);
  *ext = name.substr(dot + 1);
  return true;
}

}  // namespace

void DB::SortLevel(std::vector<TableHandle>* files, int level) {
  if (level == 0) {
    std::sort(files->begin(), files->end(),
              [](const auto& a, const auto& b) { return a.meta.number > b.meta.number; });
  } else {
    std::sort(files->begin(), files->end(),
              [](const auto& a, const auto& b) { return a.meta.smallest < b.meta.smallest; });
  }
}

DB::DB(Options options, std::string dir) : options_(options), dir_(std::move(dir)) {
  auto v = std::make_shared<Version>();
  v->levels.resize(kNumLevels);
  version_ = v;
  mem_ = std::make_shared<MemTable>();
}

DB::~DB() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
    cv_.notify_all();
  }
  if (bg_thread_.joinable()) bg_thread_.join();
}

Status DB::Open(const Options& options, const std::string& dir, std::unique_ptr<DB>* out) {
  std::unique_ptr<DB> db(new DB(options, dir));
  Status s = db->Recover();
  if (!s.ok()) return s;
  db->bg_thread_ = std::thread(&DB::BackgroundLoop, db.get());
  *out = std::move(db);
  return Status::OK();
}

Status DB::Recover() {
  std::error_code ec;
  std::filesystem::create_directories(dir_, ec);
  if (ec) return Status::IOError("create dir " + dir_ + ": " + ec.message());

  ManifestData md;
  Status s = LoadManifest(dir_, &md);
  if (s.IsNotFound()) {
    md = ManifestData{};
  } else if (!s.ok()) {
    return s;
  }
  next_file_ = md.next_file;
  last_seq_ = md.last_seq;

  auto v = std::make_shared<Version>();
  v->levels.resize(kNumLevels);
  std::set<uint64_t> live;
  for (const auto& meta : md.files) {
    if (meta.level < 0 || meta.level >= kNumLevels) {
      return Status::Corruption("bad level in manifest");
    }
    TableHandle h;
    h.meta = meta;
    s = Table::Open(SstPath(dir_, meta.number), &h.table);
    if (!s.ok()) return s;
    v->levels[static_cast<size_t>(meta.level)].push_back(std::move(h));
    live.insert(meta.number);
  }
  for (int level = 0; level < kNumLevels; level++) {
    SortLevel(&v->levels[static_cast<size_t>(level)], level);
  }
  version_ = v;

  // Scan the directory: delete crash leftovers, find WALs to replay, and make
  // sure the file counter is ahead of every file on disk.
  std::vector<uint64_t> wals;
  uint64_t max_number = 0;
  for (const auto& de : std::filesystem::directory_iterator(dir_)) {
    std::string name = de.path().filename().string();
    if (name == "MANIFEST") continue;
    uint64_t number = 0;
    std::string ext;
    if (!ParseFileName(name, &number, &ext)) {
      if (name.ends_with(".tmp")) std::filesystem::remove(de.path());
      continue;
    }
    max_number = std::max(max_number, number);
    if (ext == "wal") {
      wals.push_back(number);
    } else if (ext == "sst" && live.find(number) == live.end()) {
      std::filesystem::remove(de.path());
    }
  }
  next_file_ = std::max(next_file_, max_number + 1);

  std::sort(wals.begin(), wals.end());
  for (uint64_t number : wals) {
    std::vector<Entry> entries;
    s = RecoverWal(WalPath(dir_, number), &entries);
    if (!s.ok()) return s;
    for (const auto& e : entries) {
      mem_->Add(e.seqno, e.op, e.key, e.value);
      last_seq_ = std::max(last_seq_, e.seqno);
    }
  }

  // Flush replayed data straight to L0 so old WALs can be deleted; keeping
  // them until a later background flush would tie WAL lifetime to memtable
  // generations that no longer exist.
  if (mem_->Count() > 0) {
    std::unique_ptr<InternalIterator> it(mem_->NewIterator());
    std::vector<TableHandle> outputs;
    s = BuildTableFromIterator(it.get(), 0, *version_, UINT64_MAX, &outputs);
    if (!s.ok()) return s;
    auto nv = std::make_shared<Version>(*version_);
    for (auto& h : outputs) nv->levels[0].push_back(std::move(h));
    SortLevel(&nv->levels[0], 0);
    version_ = nv;
    mem_ = std::make_shared<MemTable>();
  }

  ManifestData save = CurrentManifestData();
  s = SaveManifest(dir_, save);
  if (!s.ok()) return s;
  for (uint64_t number : wals) std::filesystem::remove(WalPath(dir_, number));

  wal_number_ = next_file_++;
  return WalWriter::Open(WalPath(dir_, wal_number_), &wal_);
}

ManifestData DB::CurrentManifestData() {
  ManifestData md;
  md.next_file = next_file_;
  md.last_seq = last_seq_;
  for (const auto& level : version_->levels) {
    for (const auto& h : level) md.files.push_back(h.meta);
  }
  return md;
}

Status DB::Put(std::string_view key, std::string_view value) {
  return WriteImpl(kPut, key, value);
}

Status DB::Delete(std::string_view key) { return WriteImpl(kDelete, key, ""); }

Status DB::WriteImpl(Op op, std::string_view key, std::string_view value) {
  std::unique_lock<std::mutex> lock(mutex_);
  while (imm_.size() >= 2 && bg_error_.ok() && !shutdown_) cv_.wait(lock);
  if (!bg_error_.ok()) return bg_error_;
  if (shutdown_) return Status::Aborted("db closing");

  Entry e;
  e.key = std::string(key);
  e.seqno = last_seq_ + 1;
  e.op = op;
  e.value = std::string(value);
  Status s = wal_->Append(e);
  if (s.ok() && options_.fsync_writes) s = wal_->Sync();
  if (!s.ok()) return s;

  last_seq_ = e.seqno;
  mem_->Add(e.seqno, op, key, value);

  if (mem_->ApproximateBytes() >= options_.write_buffer_size) {
    s = SwitchMemtable();
    if (!s.ok()) return s;
    cv_.notify_all();
  }
  return Status::OK();
}

Status DB::SwitchMemtable() {
  uint64_t new_wal = next_file_++;
  std::unique_ptr<WalWriter> writer;
  Status s = WalWriter::Open(WalPath(dir_, new_wal), &writer);
  if (!s.ok()) return s;
  imm_.push_back({mem_, wal_number_});
  mem_ = std::make_shared<MemTable>();
  wal_ = std::move(writer);
  wal_number_ = new_wal;
  return Status::OK();
}

Status DB::Get(std::string_view key, std::string* value) {
  std::shared_ptr<MemTable> mem;
  std::vector<std::shared_ptr<MemTable>> imms;
  std::shared_ptr<const Version> ver;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!bg_error_.ok()) return bg_error_;
    mem = mem_;
    for (const auto& ie : imm_) imms.push_back(ie.mem);
    ver = version_;
  }

  auto resolve = [&](const Entry& e) {
    if (e.op == kDelete) return Status::NotFound();
    *value = e.value;
    return Status::OK();
  };

  Entry e;
  if (mem->Get(key, &e)) return resolve(e);
  for (auto it = imms.rbegin(); it != imms.rend(); ++it) {
    if ((*it)->Get(key, &e)) return resolve(e);
  }
  // L0 files are sorted newest-first and flushed sequentially, so the first
  // hit is the newest version on disk.
  for (const auto& h : ver->levels[0]) {
    Status s = h.table->Get(key, &e);
    if (s.ok()) return resolve(e);
    if (!s.IsNotFound()) return s;
  }
  for (size_t level = 1; level < ver->levels.size(); level++) {
    const auto& files = ver->levels[level];
    size_t lo = 0, hi = files.size();
    while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if (files[mid].meta.largest < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo < files.size() && files[lo].meta.smallest <= key) {
      Status s = files[lo].table->Get(key, &e);
      if (s.ok()) return resolve(e);
      if (!s.IsNotFound()) return s;
    }
  }
  return Status::NotFound();
}

namespace {

// Hides versioning: surfaces the newest visible value per user key, skips
// tombstoned keys. Keeps the snapshot sources alive.
class DBIterator : public Iterator {
 public:
  DBIterator(std::unique_ptr<InternalIterator> inner, std::shared_ptr<MemTable> mem,
             std::vector<std::shared_ptr<MemTable>> imms,
             std::shared_ptr<const void> version)
      : inner_(std::move(inner)),
        mem_(std::move(mem)),
        imms_(std::move(imms)),
        version_(std::move(version)) {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    inner_->SeekToFirst();
    FindNextVisible();
  }

  void Seek(std::string_view key) override {
    inner_->Seek(key);
    FindNextVisible();
  }

  void Next() override {
    SkipCurrentKey();
    FindNextVisible();
  }

  std::string_view key() const override { return key_; }
  std::string_view value() const override { return value_; }

 private:
  void SkipCurrentKey() {
    while (inner_->Valid() && inner_->entry().key == key_) inner_->Next();
  }

  void FindNextVisible() {
    valid_ = false;
    while (inner_->Valid()) {
      // The first entry of a user-key run is the newest version.
      key_ = inner_->entry().key;
      if (inner_->entry().op == kPut) {
        value_ = inner_->entry().value;
        valid_ = true;
        return;
      }
      SkipCurrentKey();
    }
  }

  std::unique_ptr<InternalIterator> inner_;
  std::shared_ptr<MemTable> mem_;
  std::vector<std::shared_ptr<MemTable>> imms_;
  std::shared_ptr<const void> version_;
  std::string key_, value_;
  bool valid_ = false;
};

}  // namespace

std::unique_ptr<Iterator> DB::NewIterator() {
  std::shared_ptr<MemTable> mem;
  std::vector<std::shared_ptr<MemTable>> imms;
  std::shared_ptr<const Version> ver;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    mem = mem_;
    for (const auto& ie : imm_) imms.push_back(ie.mem);
    ver = version_;
  }

  std::vector<std::unique_ptr<InternalIterator>> children;
  children.emplace_back(mem->NewIterator());
  for (const auto& m : imms) children.emplace_back(m->NewIterator());
  for (const auto& level : ver->levels) {
    for (const auto& h : level) children.emplace_back(h.table->NewIterator());
  }
  std::unique_ptr<InternalIterator> merged(NewMergingIterator(std::move(children)));
  return std::make_unique<DBIterator>(std::move(merged), std::move(mem), std::move(imms),
                                      std::static_pointer_cast<const void>(ver));
}

Status DB::Flush() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (mem_->Count() > 0) {
    Status s = SwitchMemtable();
    if (!s.ok()) return s;
    cv_.notify_all();
  }
  while (!imm_.empty() && bg_error_.ok() && !shutdown_) cv_.wait(lock);
  if (!bg_error_.ok()) return bg_error_;
  if (shutdown_) return Status::Aborted("db closing");
  return Status::OK();
}

Status DB::Checkpoint(const std::string& dir) {
  Status s = Flush();
  if (!s.ok()) return s;

  std::unique_lock<std::mutex> lock(mutex_);
  std::error_code ec;
  if (std::filesystem::exists(dir, ec)) {
    return Status::InvalidArgument("checkpoint dir exists: " + dir);
  }
  std::filesystem::create_directories(dir, ec);
  if (ec) return Status::IOError("create dir " + dir + ": " + ec.message());

  for (const auto& level : version_->levels) {
    for (const auto& h : level) {
      std::string src = SstPath(dir_, h.meta.number);
      std::string dst = SstPath(dir, h.meta.number);
      if (::link(src.c_str(), dst.c_str()) != 0) {
        std::filesystem::copy_file(src, dst, ec);
        if (ec) return Status::IOError("copy " + src + ": " + ec.message());
      }
    }
  }
  return SaveManifest(dir, CurrentManifestData());
}

uint64_t DB::LastSequence() {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_seq_;
}

DB::Stats DB::GetStats() {
  std::lock_guard<std::mutex> lock(mutex_);
  Stats stats;
  stats.last_seq = last_seq_;
  stats.memtable_bytes = mem_->ApproximateBytes();
  stats.immutable_count = imm_.size();
  for (const auto& level : version_->levels) stats.files_per_level.push_back(level.size());
  return stats;
}

uint64_t DB::MaxLevelBytes(int level) const {
  uint64_t bytes = options_.level_base_bytes;
  for (int i = 1; i < level; i++) bytes *= 10;
  return bytes;
}

bool DB::KeyMayExistBelow(const Version& v, int below_level, std::string_view key) {
  for (size_t level = static_cast<size_t>(below_level) + 1; level < v.levels.size();
       level++) {
    for (const auto& h : v.levels[level]) {
      if (h.meta.smallest <= key && key <= h.meta.largest) return true;
    }
  }
  return false;
}

Status DB::BuildTableFromIterator(InternalIterator* iter, int output_level,
                                  const Version& base, uint64_t max_bytes,
                                  std::vector<TableHandle>* outputs) {
  TableOptions topts;
  topts.block_size = options_.block_size;
  topts.bloom_bits_per_key = options_.bloom_bits_per_key;

  std::unique_ptr<TableBuilder> builder;
  uint64_t number = 0;
  std::string last_key;
  bool has_last = false;

  auto finish_current = [&]() -> Status {
    if (builder == nullptr) return Status::OK();
    if (builder->Entries() == 0) {
      builder->Abandon();
      builder.reset();
      return Status::OK();
    }
    TableMeta meta;
    Status s = builder->Finish(&meta);
    builder.reset();
    if (!s.ok()) return s;
    meta.number = number;
    meta.level = output_level;
    TableHandle h;
    h.meta = meta;
    s = Table::Open(SstPath(dir_, number), &h.table);
    if (!s.ok()) return s;
    outputs->push_back(std::move(h));
    return Status::OK();
  };

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    const Entry& e = iter->entry();
    // Only the newest version of each user key survives; there are no
    // storage-level snapshots to keep older versions for.
    if (has_last && e.key == last_key) continue;
    last_key = e.key;
    has_last = true;
    if (e.op == kDelete && !KeyMayExistBelow(base, output_level, e.key)) continue;

    if (builder == nullptr) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        number = next_file_++;
      }
      builder = std::make_unique<TableBuilder>(SstPath(dir_, number), topts);
    }
    Status s = builder->Add(e);
    if (!s.ok()) {
      builder->Abandon();
      return s;
    }
    if (builder->BytesWritten() >= max_bytes) {
      s = finish_current();
      if (!s.ok()) return s;
    }
  }
  return finish_current();
}

void DB::FlushOne(std::unique_lock<std::mutex>& lock) {
  ImmEntry ie = imm_.front();
  auto base = version_;
  lock.unlock();

  std::unique_ptr<InternalIterator> it(ie.mem->NewIterator());
  std::vector<TableHandle> outputs;
  Status s = BuildTableFromIterator(it.get(), 0, *base, UINT64_MAX, &outputs);

  lock.lock();
  if (!s.ok()) {
    bg_error_ = s;
    return;
  }

  auto nv = std::make_shared<Version>(*version_);
  for (auto& h : outputs) nv->levels[0].push_back(std::move(h));
  SortLevel(&nv->levels[0], 0);

  ManifestData md;
  md.next_file = next_file_;
  md.last_seq = last_seq_;
  for (const auto& level : nv->levels) {
    for (const auto& h : level) md.files.push_back(h.meta);
  }
  s = SaveManifest(dir_, md);
  if (!s.ok()) {
    bg_error_ = s;
    return;
  }
  version_ = nv;
  imm_.pop_front();
  std::filesystem::remove(WalPath(dir_, ie.wal_number));
}

bool DB::PickCompaction(Compaction* c) {
  const Version& v = *version_;
  if (v.levels[0].size() >= static_cast<size_t>(options_.l0_compaction_trigger)) {
    c->output_level = 1;
    c->inputs = v.levels[0];
    std::string smallest = c->inputs[0].meta.smallest;
    std::string largest = c->inputs[0].meta.largest;
    for (const auto& h : c->inputs) {
      smallest = std::min(smallest, h.meta.smallest);
      largest = std::max(largest, h.meta.largest);
    }
    for (const auto& h : v.levels[1]) {
      if (h.meta.largest >= smallest && h.meta.smallest <= largest) {
        c->inputs.push_back(h);
      }
    }
    return true;
  }

  for (int level = 1; level < kNumLevels - 1; level++) {
    const auto& files = v.levels[static_cast<size_t>(level)];
    uint64_t total = 0;
    for (const auto& h : files) total += h.meta.file_bytes;
    if (total <= MaxLevelBytes(level) || files.empty()) continue;

    const TableHandle& pick = files[compact_ptr_[level] % files.size()];
    compact_ptr_[level]++;
    c->output_level = level + 1;
    c->inputs = {pick};
    for (const auto& h : v.levels[static_cast<size_t>(level + 1)]) {
      if (h.meta.largest >= pick.meta.smallest && h.meta.smallest <= pick.meta.largest) {
        c->inputs.push_back(h);
      }
    }
    return true;
  }
  return false;
}

void DB::DoCompaction(const Compaction& c, std::unique_lock<std::mutex>& lock) {
  auto base = version_;
  lock.unlock();

  std::vector<std::unique_ptr<InternalIterator>> children;
  for (const auto& h : c.inputs) children.emplace_back(h.table->NewIterator());
  std::unique_ptr<InternalIterator> merged(NewMergingIterator(std::move(children)));

  std::vector<TableHandle> outputs;
  Status s = BuildTableFromIterator(merged.get(), c.output_level, *base,
                                    options_.max_output_file_bytes, &outputs);

  lock.lock();
  if (!s.ok()) {
    bg_error_ = s;
    return;
  }

  std::set<uint64_t> gone;
  for (const auto& h : c.inputs) gone.insert(h.meta.number);

  auto nv = std::make_shared<Version>();
  nv->levels.resize(kNumLevels);
  for (size_t level = 0; level < version_->levels.size(); level++) {
    for (const auto& h : version_->levels[level]) {
      if (gone.find(h.meta.number) == gone.end()) nv->levels[level].push_back(h);
    }
  }
  for (auto& h : outputs) {
    nv->levels[static_cast<size_t>(c.output_level)].push_back(std::move(h));
  }
  for (int level = 0; level < kNumLevels; level++) {
    SortLevel(&nv->levels[static_cast<size_t>(level)], level);
  }

  ManifestData md;
  md.next_file = next_file_;
  md.last_seq = last_seq_;
  for (const auto& level : nv->levels) {
    for (const auto& h : level) md.files.push_back(h.meta);
  }
  s = SaveManifest(dir_, md);
  if (!s.ok()) {
    bg_error_ = s;
    return;
  }
  version_ = nv;
  for (uint64_t number : gone) std::filesystem::remove(SstPath(dir_, number));
}

void DB::BackgroundLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!shutdown_) {
    if (!bg_error_.ok()) {
      cv_.wait(lock);
      continue;
    }
    if (!imm_.empty()) {
      FlushOne(lock);
      cv_.notify_all();
      continue;
    }
    Compaction c;
    if (PickCompaction(&c)) {
      DoCompaction(c, lock);
      cv_.notify_all();
      continue;
    }
    cv_.wait(lock);
  }
}

}  // namespace flotilla::storage
