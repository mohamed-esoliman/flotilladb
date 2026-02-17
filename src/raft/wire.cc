#include "raft/wire.h"

#include "common/coding.h"

namespace flotilla::raft {

std::string EncodeMessage(const Message& m) {
  std::string out;
  PutFixed8(&out, static_cast<uint8_t>(m.kind));
  PutFixed32(&out, m.from);
  PutFixed32(&out, m.to);
  PutFixed64(&out, m.term);
  PutFixed64(&out, m.last_log_index);
  PutFixed64(&out, m.last_log_term);
  PutFixed8(&out, m.granted ? 1 : 0);
  PutFixed64(&out, m.prev_index);
  PutFixed64(&out, m.prev_term);
  PutFixed64(&out, m.commit);
  PutFixed64(&out, m.hb_seq);
  PutFixed8(&out, m.success ? 1 : 0);
  PutFixed64(&out, m.match_index);
  PutFixed64(&out, m.hint_index);
  PutFixed64(&out, m.snap_index);
  PutFixed64(&out, m.snap_term);
  PutLengthPrefixed(&out, m.snap_data);
  PutFixed32(&out, static_cast<uint32_t>(m.entries.size()));
  for (const auto& e : m.entries) {
    PutFixed64(&out, e.term);
    PutFixed64(&out, e.index);
    PutLengthPrefixed(&out, e.command);
  }
  return out;
}

bool DecodeMessage(std::string_view data, Message* m) {
  Decoder dec(data);
  uint8_t kind = dec.U8();
  if (kind < 1 || kind > 6) return false;
  m->kind = static_cast<MsgKind>(kind);
  m->from = dec.U32();
  m->to = dec.U32();
  m->term = dec.U64();
  m->last_log_index = dec.U64();
  m->last_log_term = dec.U64();
  m->granted = dec.U8() != 0;
  m->prev_index = dec.U64();
  m->prev_term = dec.U64();
  m->commit = dec.U64();
  m->hb_seq = dec.U64();
  m->success = dec.U8() != 0;
  m->match_index = dec.U64();
  m->hint_index = dec.U64();
  m->snap_index = dec.U64();
  m->snap_term = dec.U64();
  m->snap_data = dec.Str();
  uint32_t count = dec.U32();
  m->entries.clear();
  for (uint32_t i = 0; i < count && dec.ok(); i++) {
    LogEntry e;
    e.term = dec.U64();
    e.index = dec.U64();
    e.command = dec.Str();
    m->entries.push_back(std::move(e));
  }
  return dec.ok() && dec.remaining() == 0;
}

}  // namespace flotilla::raft
