#include "storage/merging_iterator.h"

namespace flotilla::storage {

namespace {

class MergingIterator : public InternalIterator {
 public:
  explicit MergingIterator(std::vector<std::unique_ptr<InternalIterator>> children)
      : children_(std::move(children)) {}

  bool Valid() const override { return current_ != nullptr; }

  void SeekToFirst() override {
    for (auto& c : children_) c->SeekToFirst();
    PickCurrent();
  }

  void Seek(std::string_view key) override {
    for (auto& c : children_) c->Seek(key);
    PickCurrent();
  }

  void Next() override {
    current_->Next();
    PickCurrent();
  }

  const Entry& entry() const override { return current_->entry(); }

 private:
  void PickCurrent() {
    current_ = nullptr;
    for (auto& c : children_) {
      if (!c->Valid()) continue;
      if (current_ == nullptr ||
          InternalCompare(c->entry().key, c->entry().seqno, current_->entry().key,
                          current_->entry().seqno) < 0) {
        current_ = c.get();
      }
    }
  }

  std::vector<std::unique_ptr<InternalIterator>> children_;
  InternalIterator* current_ = nullptr;
};

}  // namespace

InternalIterator* NewMergingIterator(
    std::vector<std::unique_ptr<InternalIterator>> children) {
  return new MergingIterator(std::move(children));
}

}  // namespace flotilla::storage
