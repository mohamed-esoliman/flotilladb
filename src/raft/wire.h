#pragma once

#include <string>
#include <string_view>

#include "raft/raft.h"

namespace flotilla::raft {

std::string EncodeMessage(const Message& m);
bool DecodeMessage(std::string_view data, Message* m);

}  // namespace flotilla::raft
