#pragma once

#include <memory>
#include <vector>

#include "storage/entry.h"

namespace flotilla::storage {

// K-way merge over child iterators in internal-key order. Seqnos are globally
// unique across a consistent snapshot, so no tie-breaking is needed.
InternalIterator* NewMergingIterator(std::vector<std::unique_ptr<InternalIterator>> children);

}  // namespace flotilla::storage
