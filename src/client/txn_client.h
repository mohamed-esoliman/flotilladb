#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "client/client.h"

namespace flotilla::client {

// Client-coordinated snapshot-isolation transaction (Percolator 2PC).
// Reads see the database as of Begin(); writes buffer locally until Commit().
// Commit returns Conflict when another transaction won a write-write race;
// the caller retries the whole transaction.
class Txn {
 public:
  explicit Txn(Client* client) : client_(client) {}

  Status Begin();
  Status Get(const std::string& key, std::string* value);
  Status Scan(const std::string& start, const std::string& end, uint32_t limit,
              std::vector<std::pair<std::string, std::string>>* rows);
  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  Status Commit();
  Status Rollback();

  uint64_t start_ts() const { return start_ts_; }

 private:
  struct Mutation {
    uint8_t op;  // 1 put, 2 delete
    std::string value;
  };

  Status AllocateTs(uint64_t* ts);
  // Retries an operation that can be blocked by another txn's lock,
  // resolving abandoned locks via their primary.
  Status ResolveLock(const net::Response& resp);

  Client* client_;
  uint64_t start_ts_ = 0;
  bool active_ = false;
  std::map<std::string, Mutation> mutations_;
  std::vector<std::string> prewritten_;
};

}  // namespace flotilla::client
