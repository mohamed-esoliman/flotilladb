#include "server/kv_service.h"

namespace flotilla::server {

using net::MsgType;
using net::Request;
using net::Response;

bool LocalKvService::HandleFrame(std::string_view payload, std::string* out) {
  Request req;
  if (!DecodeRequest(payload, &req)) return false;
  *out = EncodeResponse(Handle(req));
  return true;
}

Response LocalKvService::Handle(const Request& req) {
  Response resp;
  switch (req.type) {
    case MsgType::kGet: {
      Status s = db_->Get(req.key, &resp.value);
      if (s.ok()) {
        resp.found = true;
      } else if (s.IsNotFound()) {
        resp.found = false;
      } else {
        return Response::FromStatus(s);
      }
      break;
    }
    case MsgType::kPut:
      if (Status s = db_->Put(req.key, req.value); !s.ok()) {
        return Response::FromStatus(s);
      }
      break;
    case MsgType::kDelete:
      if (Status s = db_->Delete(req.key); !s.ok()) {
        return Response::FromStatus(s);
      }
      break;
    case MsgType::kScan: {
      uint32_t limit = req.limit == 0 ? kDefaultScanLimit : req.limit;
      auto it = db_->NewIterator();
      if (req.key.empty()) {
        it->SeekToFirst();
      } else {
        it->Seek(req.key);
      }
      while (it->Valid() && resp.kvs.size() < limit) {
        if (!req.end_key.empty() && std::string_view(it->key()) >= req.end_key) break;
        resp.kvs.emplace_back(std::string(it->key()), std::string(it->value()));
        it->Next();
      }
      break;
    }
    case MsgType::kStatus: {
      auto stats = db_->GetStats();
      resp.kvs.emplace_back("mode", "single-node");
      resp.kvs.emplace_back("addr", self_addr_);
      resp.kvs.emplace_back("last_seq", std::to_string(stats.last_seq));
      resp.kvs.emplace_back("memtable_bytes", std::to_string(stats.memtable_bytes));
      std::string levels;
      for (size_t n : stats.files_per_level) {
        if (!levels.empty()) levels += " ";
        levels += std::to_string(n);
      }
      resp.kvs.emplace_back("files_per_level", levels);
      break;
    }
    default:
      return Response::FromStatus(Status::InvalidArgument("unsupported request type"));
  }
  return resp;
}

}  // namespace flotilla::server
