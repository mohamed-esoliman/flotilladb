#include "common/status.h"

namespace flotilla {

std::string Status::ToString() const {
  const char* name = "unknown";
  switch (code_) {
    case Code::kOk: return "OK";
    case Code::kNotFound: name = "not found"; break;
    case Code::kCorruption: name = "corruption"; break;
    case Code::kIOError: name = "io error"; break;
    case Code::kInvalidArgument: name = "invalid argument"; break;
    case Code::kNotLeader: name = "not leader"; break;
    case Code::kTimeout: name = "timeout"; break;
    case Code::kAborted: name = "aborted"; break;
    case Code::kConflict: name = "conflict"; break;
  }
  if (msg_.empty()) return name;
  return std::string(name) + ": " + msg_;
}

}  // namespace flotilla
