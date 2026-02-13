#include "net/frame.h"

#include "common/coding.h"
#include "net/socket.h"

namespace flotilla::net {

Status ReadFrame(int fd, std::string* payload) {
  char header[4];
  Status s = ReadFull(fd, header, sizeof(header));
  if (!s.ok()) return s;
  uint32_t len = DecodeFixed32(header);
  if (len > kMaxFrameBytes) {
    return Status::Corruption("frame too large: " + std::to_string(len));
  }
  payload->resize(len);
  return ReadFull(fd, payload->data(), len);
}

Status WriteFrame(int fd, std::string_view payload) {
  if (payload.size() > kMaxFrameBytes) {
    return Status::InvalidArgument("frame too large");
  }
  std::string buf;
  buf.reserve(4 + payload.size());
  PutFixed32(&buf, static_cast<uint32_t>(payload.size()));
  buf.append(payload);
  return WriteFull(fd, buf.data(), buf.size());
}

}  // namespace flotilla::net
