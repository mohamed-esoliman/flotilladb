#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "client/client.h"

namespace {

using flotilla::Status;
using flotilla::client::Client;

// Splits on whitespace; double quotes group tokens ("a b") and \" escapes.
std::vector<std::string> Tokenize(const std::string& line) {
  std::vector<std::string> tokens;
  std::string cur;
  bool in_quotes = false, have = false;
  for (size_t i = 0; i < line.size(); i++) {
    char c = line[i];
    if (c == '\\' && i + 1 < line.size() && line[i + 1] == '"') {
      cur += '"';
      i++;
      have = true;
    } else if (c == '"') {
      in_quotes = !in_quotes;
      have = true;
    } else if (!in_quotes && isspace(static_cast<unsigned char>(c))) {
      if (have) tokens.push_back(cur);
      cur.clear();
      have = false;
    } else {
      cur += c;
      have = true;
    }
  }
  if (have) tokens.push_back(cur);
  return tokens;
}

void PrintTable(const std::vector<std::pair<std::string, std::string>>& rows,
                const char* left, const char* right) {
  size_t width = strlen(left);
  for (const auto& [k, v] : rows) width = std::max(width, k.size());
  printf("%-*s  %s\n", static_cast<int>(width), left, right);
  for (size_t i = 0; i < width; i++) putchar('-');
  printf("  ");
  for (size_t i = 0; i < strlen(right); i++) putchar('-');
  putchar('\n');
  for (const auto& [k, v] : rows) {
    printf("%-*s  %s\n", static_cast<int>(width), k.c_str(), v.c_str());
  }
}

void Help() {
  printf(
      "commands:\n"
      "  get <key>                  read a key\n"
      "  put <key> <value>          write a key\n"
      "  del <key>                  delete a key\n"
      "  scan [start] [end] [n]     range scan, end exclusive, n row limit\n"
      "  status                     node status\n"
      "  split <key>                split the range containing key at key\n"
      "  ranges                     list range descriptors\n"
      "  help                       this help\n"
      "  quit                       exit\n"
      "quote values containing spaces: put greeting \"hello world\"\n");
}

// Returns 0 ok, 1 error, 2 unknown command.
int RunCommand(Client& client, const std::vector<std::string>& tokens) {
  const std::string& cmd = tokens[0];
  auto report = [](const Status& s) {
    printf("(error) %s\n", s.ToString().c_str());
    return 1;
  };

  if (cmd == "get" && tokens.size() == 2) {
    std::string value;
    Status s = client.Get(tokens[1], &value);
    if (s.IsNotFound()) {
      printf("(not found)\n");
      return 0;
    }
    if (!s.ok()) return report(s);
    printf("%s\n", value.c_str());
  } else if (cmd == "put" && tokens.size() == 3) {
    Status s = client.Put(tokens[1], tokens[2]);
    if (!s.ok()) return report(s);
    printf("OK\n");
  } else if (cmd == "del" && tokens.size() == 2) {
    Status s = client.Delete(tokens[1]);
    if (!s.ok()) return report(s);
    printf("OK\n");
  } else if (cmd == "scan" && tokens.size() <= 4) {
    std::string start = tokens.size() > 1 ? tokens[1] : "";
    std::string end = tokens.size() > 2 ? tokens[2] : "";
    uint32_t limit = 0;
    if (tokens.size() > 3) limit = static_cast<uint32_t>(atoi(tokens[3].c_str()));
    std::vector<std::pair<std::string, std::string>> rows;
    Status s = client.Scan(start, end, limit, &rows);
    if (!s.ok()) return report(s);
    if (rows.empty()) {
      printf("(empty)\n");
    } else {
      PrintTable(rows, "key", "value");
      printf("(%zu rows)\n", rows.size());
    }
  } else if (cmd == "status" && tokens.size() == 1) {
    std::vector<std::pair<std::string, std::string>> fields;
    Status s = client.GetStatus(&fields);
    if (!s.ok()) return report(s);
    PrintTable(fields, "field", "value");
  } else if (cmd == "split" && tokens.size() == 2) {
    Status s = client.Split(tokens[1]);
    if (!s.ok()) return report(s);
    printf("OK\n");
  } else if (cmd == "ranges" && tokens.size() == 1) {
    std::vector<std::pair<std::string, std::string>> ranges;
    Status s = client.Ranges(&ranges);
    if (!s.ok()) return report(s);
    PrintTable(ranges, "range", "bounds");
  } else if (cmd == "help") {
    Help();
  } else {
    return 2;
  }
  if (!client.last_redirect().empty()) {
    printf("(redirected to leader %s)\n", client.last_redirect().c_str());
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<std::string> addrs;
  std::string host = "127.0.0.1";
  std::string exec;
  int port = -1;
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--host" && i + 1 < argc) {
      host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      port = atoi(argv[++i]);
    } else if (arg == "--exec" && i + 1 < argc) {
      exec = argv[++i];
    } else if (arg[0] != '-') {
      addrs.push_back(arg);
    } else {
      fprintf(stderr,
              "usage: %s [addr...] [--host h --port p] [--exec \"cmd args\"]\n",
              argv[0]);
      return 2;
    }
  }
  if (port > 0) addrs.push_back(host + ":" + std::to_string(port));
  if (addrs.empty()) addrs.push_back("127.0.0.1:4001");

  Client client(addrs);

  if (!exec.empty()) {
    auto tokens = Tokenize(exec);
    if (tokens.empty()) return 2;
    int rc = RunCommand(client, tokens);
    if (rc == 2) {
      fprintf(stderr, "unknown command: %s\n", tokens[0].c_str());
      return 2;
    }
    return rc;
  }

  printf("flotilla-cli connected targets:");
  for (const auto& a : addrs) printf(" %s", a.c_str());
  printf("\ntype help for commands\n");

  std::string line;
  while (true) {
    printf("flotilla> ");
    fflush(stdout);
    if (!std::getline(std::cin, line)) break;
    auto tokens = Tokenize(line);
    if (tokens.empty()) continue;
    if (tokens[0] == "quit" || tokens[0] == "exit") break;
    if (RunCommand(client, tokens) == 2) {
      printf("unknown command (try help)\n");
    }
  }
  return 0;
}
