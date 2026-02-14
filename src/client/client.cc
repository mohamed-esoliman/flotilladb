#include "client/client.h"

#include <unistd.h>

#include "net/frame.h"
#include "net/socket.h"

namespace flotilla::client {

using net::MsgType;
using net::Request;
using net::Response;

Client::Client(std::vector<std::string> addrs) : addrs_(std::move(addrs)) {}

Client::~Client() { Disconnect(); }

void Client::Disconnect() {
  if (fd_ >= 0) {
    net::CloseSocket(fd_);
    fd_ = -1;
  }
}

Status Client::EnsureConnected() {
  if (fd_ >= 0) return Status::OK();
  if (addrs_.empty()) return Status::InvalidArgument("no addresses");
  Status last = Status::IOError("unreachable");
  for (size_t i = 0; i < addrs_.size(); i++) {
    const std::string& addr = addrs_[next_addr_ % addrs_.size()];
    next_addr_++;
    last = net::Connect(addr, &fd_);
    if (last.ok()) {
      current_addr_ = addr;
      return Status::OK();
    }
  }
  return last;
}

Status Client::CallOnce(const Request& req, Response* resp) {
  Status s = EnsureConnected();
  if (!s.ok()) return s;
  s = net::WriteFrame(fd_, EncodeRequest(req));
  if (!s.ok()) {
    Disconnect();
    return s;
  }
  std::string payload;
  s = net::ReadFrame(fd_, &payload);
  if (!s.ok()) {
    Disconnect();
    return s;
  }
  if (!DecodeResponse(payload, resp)) {
    Disconnect();
    return Status::Corruption("bad response");
  }
  return Status::OK();
}

Status Client::Call(const Request& req, Response* resp) {
  last_redirect_.clear();
  Status s;
  // Enough attempts to rotate through every node twice plus follow redirects
  // during an election.
  size_t attempts = addrs_.size() * 2 + 4;
  for (size_t attempt = 0; attempt < attempts; attempt++) {
    s = CallOnce(req, resp);
    if (s.ok()) {
      if (resp->code == static_cast<uint8_t>(Status::Code::kNotLeader)) {
        if (!resp->leader_addr.empty() && resp->leader_addr != current_addr_) {
          Disconnect();
          last_redirect_ = resp->leader_addr;
          // Try the leader next without dropping the known address list.
          bool known = false;
          for (const auto& a : addrs_) {
            if (a == resp->leader_addr) known = true;
          }
          if (!known) addrs_.push_back(resp->leader_addr);
          for (size_t i = 0; i < addrs_.size(); i++) {
            if (addrs_[i] == resp->leader_addr) next_addr_ = i;
          }
        }
        ::usleep(50 * 1000);
        continue;
      }
      return Status::OK();
    }
    ::usleep(50 * 1000);
  }
  return s.ok() ? Status::Timeout("no leader found") : s;
}

Status Client::Get(const std::string& key, std::string* value) {
  Request req;
  req.type = MsgType::kGet;
  req.key = key;
  Response resp;
  Status s = Call(req, &resp);
  if (!s.ok()) return s;
  if (!resp.ok()) return resp.ToStatus();
  if (!resp.found) return Status::NotFound(key);
  *value = resp.value;
  return Status::OK();
}

Status Client::Put(const std::string& key, const std::string& value) {
  Request req;
  req.type = MsgType::kPut;
  req.key = key;
  req.value = value;
  Response resp;
  Status s = Call(req, &resp);
  if (!s.ok()) return s;
  return resp.ToStatus();
}

Status Client::Delete(const std::string& key) {
  Request req;
  req.type = MsgType::kDelete;
  req.key = key;
  Response resp;
  Status s = Call(req, &resp);
  if (!s.ok()) return s;
  return resp.ToStatus();
}

Status Client::Scan(const std::string& start, const std::string& end, uint32_t limit,
                    std::vector<std::pair<std::string, std::string>>* rows) {
  Request req;
  req.type = MsgType::kScan;
  req.key = start;
  req.end_key = end;
  req.limit = limit;
  Response resp;
  Status s = Call(req, &resp);
  if (!s.ok()) return s;
  if (!resp.ok()) return resp.ToStatus();
  *rows = std::move(resp.kvs);
  return Status::OK();
}

Status Client::GetStatus(std::vector<std::pair<std::string, std::string>>* fields) {
  Request req;
  req.type = MsgType::kStatus;
  Response resp;
  Status s = Call(req, &resp);
  if (!s.ok()) return s;
  if (!resp.ok()) return resp.ToStatus();
  *fields = std::move(resp.kvs);
  return Status::OK();
}

}  // namespace flotilla::client
