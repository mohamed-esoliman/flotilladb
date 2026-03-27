#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "net/messages.h"

namespace flotilla::client {

// Synchronous client for one cluster. Connects to any given node; on
// NOT_LEADER responses it follows the leader hint and retries; on connection
// failures it rotates to the next known address.
class Client {
 public:
  explicit Client(std::vector<std::string> addrs);
  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  Status Get(const std::string& key, std::string* value);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Scan(const std::string& start, const std::string& end, uint32_t limit,
              std::vector<std::pair<std::string, std::string>>* rows);
  Status GetStatus(std::vector<std::pair<std::string, std::string>>* fields);
  Status Split(const std::string& key);
  Status Ranges(std::vector<std::pair<std::string, std::string>>* ranges);

  // Last address a successful call went to.
  const std::string& current_addr() const { return current_addr_; }
  // Set to the redirect target when the previous call followed a leader hint.
  const std::string& last_redirect() const { return last_redirect_; }

  Status Call(const net::Request& req, net::Response* resp);

 private:
  Status EnsureConnected();
  void Disconnect();
  Status CallOnce(const net::Request& req, net::Response* resp);

  std::vector<std::string> addrs_;
  size_t next_addr_ = 0;
  std::string current_addr_;
  std::string last_redirect_;
  int fd_ = -1;
};

}  // namespace flotilla::client
