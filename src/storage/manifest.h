#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "storage/sstable.h"

namespace flotilla::storage {

struct ManifestData {
  uint64_t next_file = 1;
  uint64_t last_seq = 0;
  std::vector<TableMeta> files;
};

// The manifest is a text file rewritten whole on every version change via
// atomic rename. Missing manifest returns NotFound (fresh database).
Status LoadManifest(const std::string& dir, ManifestData* out);
Status SaveManifest(const std::string& dir, const ManifestData& data);

std::string SstPath(const std::string& dir, uint64_t number);
std::string WalPath(const std::string& dir, uint64_t number);

}  // namespace flotilla::storage
