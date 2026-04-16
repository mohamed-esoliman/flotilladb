#include "sql/lexer.h"

#include <cctype>
#include <set>

namespace flotilla::sql {

namespace {

const std::set<std::string>& Keywords() {
  static const std::set<std::string> keywords = {
      "CREATE", "TABLE", "INT",    "TEXT",   "PRIMARY", "KEY",  "INSERT",
      "INTO",   "VALUES", "SELECT", "FROM",  "WHERE",   "AND",  "UPDATE",
      "SET",    "DELETE", "DROP"};
  return keywords;
}

}  // namespace

Status Lex(const std::string& input, std::vector<Token>* tokens) {
  tokens->clear();
  size_t i = 0;
  while (i < input.size()) {
    char c = input[i];
    if (isspace(static_cast<unsigned char>(c))) {
      i++;
      continue;
    }
    Token token;
    token.pos = i;
    if (isalpha(static_cast<unsigned char>(c)) || c == '_') {
      size_t start = i;
      while (i < input.size() &&
             (isalnum(static_cast<unsigned char>(input[i])) || input[i] == '_')) {
        i++;
      }
      token.text = input.substr(start, i - start);
      std::string upper;
      for (char ch : token.text) upper += static_cast<char>(toupper(ch));
      if (Keywords().count(upper) > 0) {
        token.type = TokenType::kKeyword;
        token.text = upper;
      } else {
        token.type = TokenType::kIdentifier;
      }
    } else if (isdigit(static_cast<unsigned char>(c)) ||
               (c == '-' && i + 1 < input.size() &&
                isdigit(static_cast<unsigned char>(input[i + 1])))) {
      size_t start = i;
      if (c == '-') i++;
      while (i < input.size() && isdigit(static_cast<unsigned char>(input[i]))) i++;
      token.type = TokenType::kInt;
      token.text = input.substr(start, i - start);
      token.int_value = strtoll(token.text.c_str(), nullptr, 10);
    } else if (c == '\'') {
      i++;
      std::string value;
      bool closed = false;
      while (i < input.size()) {
        if (input[i] == '\'') {
          if (i + 1 < input.size() && input[i + 1] == '\'') {
            value += '\'';  // '' escapes a quote
            i += 2;
            continue;
          }
          closed = true;
          i++;
          break;
        }
        value += input[i++];
      }
      if (!closed) {
        return Status::InvalidArgument("unterminated string literal at offset " +
                                       std::to_string(token.pos));
      }
      token.type = TokenType::kString;
      token.text = value;
    } else if (c == '<' || c == '>' || c == '!') {
      token.type = TokenType::kSymbol;
      token.text = std::string(1, c);
      i++;
      if (i < input.size() && input[i] == '=') {
        token.text += '=';
        i++;
      } else if (c == '!') {
        return Status::InvalidArgument("expected != at offset " +
                                       std::to_string(token.pos));
      }
    } else if (c == '(' || c == ')' || c == ',' || c == ';' || c == '*' || c == '=') {
      token.type = TokenType::kSymbol;
      token.text = std::string(1, c);
      i++;
    } else {
      return Status::InvalidArgument("unexpected character '" + std::string(1, c) +
                                     "' at offset " + std::to_string(i));
    }
    tokens->push_back(std::move(token));
  }
  Token end;
  end.type = TokenType::kEnd;
  end.pos = input.size();
  tokens->push_back(end);
  return Status::OK();
}

}  // namespace flotilla::sql
