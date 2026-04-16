#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace flotilla::sql {

enum class TokenType {
  kKeyword,     // normalized upper-case
  kIdentifier,  // as written
  kInt,
  kString,
  kSymbol,  // ( ) , ; * = != < <= > >=
  kEnd,
};

struct Token {
  TokenType type = TokenType::kEnd;
  std::string text;
  int64_t int_value = 0;
  size_t pos = 0;  // byte offset in the statement, for error messages
};

// Tokenizes one statement. Keywords are recognized case-insensitively.
Status Lex(const std::string& input, std::vector<Token>* tokens);

}  // namespace flotilla::sql
