#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace flotilla {

inline void PutFixed8(std::string* dst, uint8_t v) { dst->push_back(static_cast<char>(v)); }

inline void PutFixed16(std::string* dst, uint16_t v) {
  char buf[2];
  buf[0] = static_cast<char>(v);
  buf[1] = static_cast<char>(v >> 8);
  dst->append(buf, 2);
}

inline void PutFixed32(std::string* dst, uint32_t v) {
  char buf[4];
  for (int i = 0; i < 4; i++) buf[i] = static_cast<char>(v >> (8 * i));
  dst->append(buf, 4);
}

inline void PutFixed64(std::string* dst, uint64_t v) {
  char buf[8];
  for (int i = 0; i < 8; i++) buf[i] = static_cast<char>(v >> (8 * i));
  dst->append(buf, 8);
}

inline uint8_t DecodeFixed8(const char* p) { return static_cast<uint8_t>(p[0]); }

inline uint16_t DecodeFixed16(const char* p) {
  return static_cast<uint16_t>(static_cast<uint8_t>(p[0])) |
         (static_cast<uint16_t>(static_cast<uint8_t>(p[1])) << 8);
}

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  for (int i = 3; i >= 0; i--) v = (v << 8) | static_cast<uint8_t>(p[i]);
  return v;
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 7; i >= 0; i--) v = (v << 8) | static_cast<uint8_t>(p[i]);
  return v;
}

inline void PutLengthPrefixed(std::string* dst, std::string_view s) {
  PutFixed32(dst, static_cast<uint32_t>(s.size()));
  dst->append(s.data(), s.size());
}

// Sequential decoder over a byte buffer; sets a failed flag on underflow
// instead of throwing so callers can check once at the end.
class Decoder {
 public:
  explicit Decoder(std::string_view data) : data_(data) {}

  bool ok() const { return !failed_; }
  size_t remaining() const { return data_.size() - pos_; }

  uint8_t U8() { return Have(1) ? DecodeFixed8(Advance(1)) : 0; }
  uint16_t U16() { return Have(2) ? DecodeFixed16(Advance(2)) : 0; }
  uint32_t U32() { return Have(4) ? DecodeFixed32(Advance(4)) : 0; }
  uint64_t U64() { return Have(8) ? DecodeFixed64(Advance(8)) : 0; }

  std::string_view Bytes(size_t n) {
    if (!Have(n)) return {};
    return {Advance(n), n};
  }

  std::string_view LengthPrefixed() {
    uint32_t n = U32();
    return Bytes(n);
  }

  std::string Str() { return std::string(LengthPrefixed()); }

 private:
  bool Have(size_t n) {
    if (failed_ || data_.size() - pos_ < n) {
      failed_ = true;
      return false;
    }
    return true;
  }

  const char* Advance(size_t n) {
    const char* p = data_.data() + pos_;
    pos_ += n;
    return p;
  }

  std::string_view data_;
  size_t pos_ = 0;
  bool failed_ = false;
};

}  // namespace flotilla
