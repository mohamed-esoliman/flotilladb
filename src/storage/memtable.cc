#include "storage/memtable.h"

#include <cassert>

namespace flotilla::storage {

struct MemTable::Node {
  Entry entry;
  int height;
  Node* next[1];  // over-allocated to height
};

MemTable::MemTable() {
  Entry head_entry;
  head_ = NewNode(head_entry, kMaxHeight);
  for (int i = 0; i < kMaxHeight; i++) head_->next[i] = nullptr;
}

MemTable::~MemTable() {
  Node* n = head_;
  while (n != nullptr) {
    Node* next = n->next[0];
    n->entry.~Entry();
    ::operator delete(n);
    n = next;
  }
}

MemTable::Node* MemTable::NewNode(const Entry& e, int height) {
  size_t size = sizeof(Node) + sizeof(Node*) * (static_cast<size_t>(height) - 1);
  Node* n = static_cast<Node*>(::operator new(size));
  new (&n->entry) Entry(e);
  n->height = height;
  return n;
}

int MemTable::RandomHeight() {
  int h = 1;
  while (h < kMaxHeight && (rng_() & 3) == 0) h++;
  return h;
}

MemTable::Node* MemTable::FindGreaterOrEqual(std::string_view key, uint64_t seqno,
                                             Node** prev) const {
  Node* x = head_;
  int level = max_height_ - 1;
  while (true) {
    Node* next = x->next[level];
    bool advance = next != nullptr &&
                   InternalCompare(next->entry.key, next->entry.seqno, key, seqno) < 0;
    if (advance) {
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) return next;
      level--;
    }
  }
}

void MemTable::Add(uint64_t seqno, Op op, std::string_view key, std::string_view value) {
  Entry e;
  e.key = std::string(key);
  e.seqno = seqno;
  e.op = op;
  e.value = std::string(value);

  Node* prev[kMaxHeight];
  for (int i = 0; i < kMaxHeight; i++) prev[i] = head_;
  FindGreaterOrEqual(key, seqno, prev);

  int height = RandomHeight();
  if (height > max_height_) max_height_ = height;

  Node* n = NewNode(e, height);
  for (int i = 0; i < height; i++) {
    n->next[i] = prev[i]->next[i];
    prev[i]->next[i] = n;
  }
  bytes_ += key.size() + value.size() + 32;
  count_++;
}

bool MemTable::Get(std::string_view key, Entry* out) const {
  // Seek to (key, max seqno): the first entry for this user key is the newest.
  Node* n = FindGreaterOrEqual(key, UINT64_MAX, nullptr);
  if (n == nullptr || n->entry.key != key) return false;
  *out = n->entry;
  return true;
}

class MemTableIterator : public InternalIterator {
 public:
  explicit MemTableIterator(const MemTable* mem) : mem_(mem) {}

  bool Valid() const override { return node_ != nullptr; }
  void SeekToFirst() override;
  void Seek(std::string_view key) override;
  void Next() override { node_ = node_->next[0]; }
  const Entry& entry() const override { return node_->entry; }

 private:
  const MemTable* mem_;
  const MemTable::Node* node_ = nullptr;
};

void MemTableIterator::SeekToFirst() { node_ = mem_->head_->next[0]; }

void MemTableIterator::Seek(std::string_view key) {
  node_ = mem_->FindGreaterOrEqual(key, UINT64_MAX, nullptr);
}

InternalIterator* MemTable::NewIterator() const { return new MemTableIterator(this); }

}  // namespace flotilla::storage
