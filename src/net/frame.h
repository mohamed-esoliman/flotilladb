#pragma once

#include <string>

#include "common/status.h"

namespace flotilla::net {

inline constexpr size_t kMaxFrameBytes = 32u << 20;

Status ReadFrame(int fd, std::string* payload);
Status WriteFrame(int fd, std::string_view payload);

}  // namespace flotilla::net
