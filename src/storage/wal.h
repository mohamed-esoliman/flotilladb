#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/entry.h"

namespace flotilla::storage {

class WalWriter {
 public:
  static Status Open(const std::string& path, std::unique_ptr<WalWriter>* out);
  ~WalWriter();

  Status Append(const Entry& entry);
  Status Sync();

 private:
  WalWriter(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}

  int fd_;
  std::string path_;
};

// Reads all valid records into *entries. A torn or corrupt tail is expected
// after a crash: the file is truncated back to the last valid record boundary
// and OK is returned. Missing file is OK with no entries.
Status RecoverWal(const std::string& path, std::vector<Entry>* entries);

}  // namespace flotilla::storage
