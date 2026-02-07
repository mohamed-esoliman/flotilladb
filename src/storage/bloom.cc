#include "storage/bloom.h"

#include <cmath>

#include "common/coding.h"

namespace flotilla::storage {

namespace {

uint64_t Fnv1a64(std::string_view s) {
  uint64_t h = 0xcbf29ce484222325ull;
  for (unsigned char c : s) {
    h ^= c;
    h *= 0x100000001b3ull;
  }
  return h;
}

// Double hashing: g_i = h1 + i * h2.
void Hashes(std::string_view key, uint64_t* h1, uint64_t* h2) {
  uint64_t h = Fnv1a64(key);
  *h1 = h;
  *h2 = (h >> 33) | (h << 31);
  if (*h2 == 0) *h2 = 0x9e3779b97f4a7c15ull;
}

}  // namespace

std::string BuildBloom(const std::vector<std::string_view>& keys, int bits_per_key) {
  size_t nbits = keys.size() * static_cast<size_t>(bits_per_key);
  if (nbits < 64) nbits = 64;
  uint32_t k = static_cast<uint32_t>(bits_per_key * 0.69);
  if (k < 1) k = 1;
  if (k > 30) k = 30;

  std::vector<uint8_t> bits((nbits + 7) / 8, 0);
  nbits = bits.size() * 8;
  for (std::string_view key : keys) {
    uint64_t h1, h2;
    Hashes(key, &h1, &h2);
    for (uint32_t i = 0; i < k; i++) {
      uint64_t bit = (h1 + i * h2) % nbits;
      bits[bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
    }
  }

  std::string out;
  PutFixed32(&out, k);
  PutFixed32(&out, static_cast<uint32_t>(nbits));
  out.append(reinterpret_cast<const char*>(bits.data()), bits.size());
  return out;
}

bool BloomMayContain(std::string_view filter, std::string_view key) {
  if (filter.size() < 8) return true;
  uint32_t k = DecodeFixed32(filter.data());
  uint64_t nbits = DecodeFixed32(filter.data() + 4);
  if (nbits == 0 || filter.size() - 8 < (nbits + 7) / 8) return true;
  const uint8_t* bits = reinterpret_cast<const uint8_t*>(filter.data() + 8);

  uint64_t h1, h2;
  Hashes(key, &h1, &h2);
  for (uint32_t i = 0; i < k; i++) {
    uint64_t bit = (h1 + i * h2) % nbits;
    if ((bits[bit / 8] & (1u << (bit % 8))) == 0) return false;
  }
  return true;
}

}  // namespace flotilla::storage
