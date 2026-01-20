#pragma once

#include <string>
#include <utility>

namespace flotilla {

class Status {
 public:
  enum class Code {
    kOk = 0,
    kNotFound,
    kCorruption,
    kIOError,
    kInvalidArgument,
    kNotLeader,
    kTimeout,
    kAborted,
    kConflict,
  };

  Status() : code_(Code::kOk) {}

  static Status OK() { return Status(); }
  static Status NotFound(std::string msg = "") { return Status(Code::kNotFound, std::move(msg)); }
  static Status Corruption(std::string msg = "") { return Status(Code::kCorruption, std::move(msg)); }
  static Status IOError(std::string msg = "") { return Status(Code::kIOError, std::move(msg)); }
  static Status InvalidArgument(std::string msg = "") { return Status(Code::kInvalidArgument, std::move(msg)); }
  static Status NotLeader(std::string msg = "") { return Status(Code::kNotLeader, std::move(msg)); }
  static Status Timeout(std::string msg = "") { return Status(Code::kTimeout, std::move(msg)); }
  static Status Aborted(std::string msg = "") { return Status(Code::kAborted, std::move(msg)); }
  static Status Conflict(std::string msg = "") { return Status(Code::kConflict, std::move(msg)); }

  bool ok() const { return code_ == Code::kOk; }
  bool IsNotFound() const { return code_ == Code::kNotFound; }
  bool IsCorruption() const { return code_ == Code::kCorruption; }
  bool IsNotLeader() const { return code_ == Code::kNotLeader; }
  bool IsConflict() const { return code_ == Code::kConflict; }
  bool IsAborted() const { return code_ == Code::kAborted; }

  Code code() const { return code_; }
  const std::string& message() const { return msg_; }
  std::string ToString() const;

 private:
  Status(Code code, std::string msg) : code_(code), msg_(std::move(msg)) {}

  Code code_;
  std::string msg_;
};

}  // namespace flotilla
