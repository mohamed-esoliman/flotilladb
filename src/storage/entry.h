#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace flotilla::storage {

enum Op : uint8_t { kPut = 1, kDelete = 2 };

struct Entry {
  std::string key;
  uint64_t seqno = 0;
  Op op = kPut;
  std::string value;
};

// Orders by user key ascending, then seqno descending (newest first).
// Seqnos are globally unique, so this is a total order over live entries.
inline int InternalCompare(std::string_view ak, uint64_t aseq, std::string_view bk,
                           uint64_t bseq) {
  int c = ak.compare(bk);
  if (c != 0) return c;
  if (aseq > bseq) return -1;
  if (aseq < bseq) return 1;
  return 0;
}

// Iterator over internal entries in internal-key order.
class InternalIterator {
 public:
  virtual ~InternalIterator() = default;
  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  // Positions at the first entry with user key >= key.
  virtual void Seek(std::string_view key) = 0;
  virtual void Next() = 0;
  virtual const Entry& entry() const = 0;
};

}  // namespace flotilla::storage
