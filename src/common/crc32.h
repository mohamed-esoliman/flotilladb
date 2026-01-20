#pragma once

#include <cstdint>
#include <string_view>

namespace flotilla {

uint32_t Crc32(std::string_view data);

}  // namespace flotilla
