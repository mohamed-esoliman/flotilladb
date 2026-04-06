#include "net/messages.h"

#include "common/coding.h"

namespace flotilla::net {

Status Response::ToStatus() const {
  auto c = static_cast<Status::Code>(code);
  switch (c) {
    case Status::Code::kOk: return Status::OK();
    case Status::Code::kNotFound: return Status::NotFound(message);
    case Status::Code::kCorruption: return Status::Corruption(message);
    case Status::Code::kIOError: return Status::IOError(message);
    case Status::Code::kInvalidArgument: return Status::InvalidArgument(message);
    case Status::Code::kNotLeader: return Status::NotLeader(message);
    case Status::Code::kTimeout: return Status::Timeout(message);
    case Status::Code::kAborted: return Status::Aborted(message);
    case Status::Code::kConflict: return Status::Conflict(message);
  }
  return Status::IOError("unknown response code " + std::to_string(code));
}

Response Response::FromStatus(const Status& s) {
  Response resp;
  resp.code = static_cast<uint8_t>(s.code());
  resp.message = s.message();
  return resp;
}

std::string EncodeRequest(const Request& req) {
  std::string out;
  PutFixed8(&out, static_cast<uint8_t>(req.type));
  PutLengthPrefixed(&out, req.key);
  PutLengthPrefixed(&out, req.value);
  PutLengthPrefixed(&out, req.end_key);
  PutFixed32(&out, req.limit);
  PutFixed8(&out, req.flags);
  PutFixed64(&out, req.ts);
  PutFixed64(&out, req.ts2);
  PutLengthPrefixed(&out, req.primary);
  PutFixed8(&out, req.wop);
  return out;
}

bool DecodeRequest(std::string_view payload, Request* req) {
  Decoder dec(payload);
  uint8_t type = dec.U8();
  switch (static_cast<MsgType>(type)) {
    case MsgType::kGet:
    case MsgType::kPut:
    case MsgType::kDelete:
    case MsgType::kScan:
    case MsgType::kStatus:
    case MsgType::kSplit:
    case MsgType::kRanges:
    case MsgType::kTxnTs:
    case MsgType::kTxnGet:
    case MsgType::kTxnPrewrite:
    case MsgType::kTxnCommit:
    case MsgType::kTxnRollback:
    case MsgType::kTxnScan:
    case MsgType::kTxnResolve:
      break;
    default:
      return false;
  }
  req->type = static_cast<MsgType>(type);
  req->key = dec.Str();
  req->value = dec.Str();
  req->end_key = dec.Str();
  req->limit = dec.U32();
  req->flags = dec.U8();
  req->ts = dec.U64();
  req->ts2 = dec.U64();
  req->primary = dec.Str();
  req->wop = dec.U8();
  return dec.ok() && dec.remaining() == 0;
}

std::string EncodeResponse(const Response& resp) {
  std::string out;
  PutFixed8(&out, static_cast<uint8_t>(MsgType::kResponse));
  PutFixed8(&out, resp.code);
  PutLengthPrefixed(&out, resp.leader_addr);
  PutLengthPrefixed(&out, resp.message);
  PutFixed8(&out, resp.found ? 1 : 0);
  PutLengthPrefixed(&out, resp.value);
  PutFixed32(&out, static_cast<uint32_t>(resp.kvs.size()));
  for (const auto& [k, v] : resp.kvs) {
    PutLengthPrefixed(&out, k);
    PutLengthPrefixed(&out, v);
  }
  PutFixed64(&out, resp.ts);
  PutFixed64(&out, resp.lock_ts);
  PutLengthPrefixed(&out, resp.lock_primary);
  PutLengthPrefixed(&out, resp.lock_key);
  return out;
}

bool DecodeResponse(std::string_view payload, Response* resp) {
  Decoder dec(payload);
  if (dec.U8() != static_cast<uint8_t>(MsgType::kResponse)) return false;
  resp->code = dec.U8();
  resp->leader_addr = dec.Str();
  resp->message = dec.Str();
  resp->found = dec.U8() != 0;
  resp->value = dec.Str();
  uint32_t count = dec.U32();
  resp->kvs.clear();
  for (uint32_t i = 0; i < count && dec.ok(); i++) {
    std::string k = dec.Str();
    std::string v = dec.Str();
    resp->kvs.emplace_back(std::move(k), std::move(v));
  }
  resp->ts = dec.U64();
  resp->lock_ts = dec.U64();
  resp->lock_primary = dec.Str();
  resp->lock_key = dec.Str();
  return dec.ok() && dec.remaining() == 0;
}

std::string EncodeRaftFrame(std::string_view body) {
  std::string out;
  PutFixed8(&out, static_cast<uint8_t>(MsgType::kRaft));
  out.append(body);
  return out;
}

bool DecodeIsRaftFrame(std::string_view payload, std::string_view* body) {
  if (payload.empty() || payload[0] != static_cast<char>(MsgType::kRaft)) return false;
  *body = payload.substr(1);
  return true;
}

}  // namespace flotilla::net
