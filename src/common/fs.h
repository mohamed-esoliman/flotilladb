#pragma once

#include <string>

#include "common/status.h"

namespace flotilla {

Status ReadFileToString(const std::string& path, std::string* out);

// Writes via a temp file, fsyncs, renames into place, fsyncs the directory.
Status WriteFileAtomic(const std::string& path, const std::string& data);

Status SyncDir(const std::string& dir);

}  // namespace flotilla
