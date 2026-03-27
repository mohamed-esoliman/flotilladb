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
