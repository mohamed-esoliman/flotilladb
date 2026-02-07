#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace flotilla::storage {

// Builds a bloom filter block: [u32 k][u32 nbits][bit bytes].
std::string BuildBloom(const std::vector<std::string_view>& keys, int bits_per_key);

// False on malformed filter; a well-formed filter never returns false for a
// key that was added.
bool BloomMayContain(std::string_view filter, std::string_view key);

}  // namespace flotilla::storage
