#include "storage/manifest.h"

#include <cinttypes>
#include <cstdio>
#include <filesystem>
#include <sstream>

#include "common/fs.h"

namespace flotilla::storage {

namespace {

std::string ToHex(std::string_view s) {
  static const char* digits = "0123456789abcdef";
  std::string out;
  out.reserve(s.size() * 2);
  for (unsigned char c : s) {
    out.push_back(digits[c >> 4]);
    out.push_back(digits[c & 0xF]);
  }
  return out.empty() ? "-" : out;
}

bool FromHex(const std::string& hex, std::string* out) {
  out->clear();
  if (hex == "-") return true;
  if (hex.size() % 2 != 0) return false;
  auto val = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
  };
  for (size_t i = 0; i < hex.size(); i += 2) {
    int hi = val(hex[i]), lo = val(hex[i + 1]);
    if (hi < 0 || lo < 0) return false;
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

}  // namespace

std::string SstPath(const std::string& dir, uint64_t number) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%06" PRIu64 ".sst", number);
  return (std::filesystem::path(dir) / buf).string();
}

std::string WalPath(const std::string& dir, uint64_t number) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%06" PRIu64 ".wal", number);
  return (std::filesystem::path(dir) / buf).string();
}

Status LoadManifest(const std::string& dir, ManifestData* out) {
  std::string content;
  Status s = ReadFileToString((std::filesystem::path(dir) / "MANIFEST").string(), &content);
  if (!s.ok()) return s;

  *out = ManifestData{};
  std::istringstream in(content);
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    std::istringstream ls(line);
    std::string tag;
    ls >> tag;
    if (tag == "next_file") {
      ls >> out->next_file;
    } else if (tag == "last_seq") {
      ls >> out->last_seq;
    } else if (tag == "file") {
      TableMeta m;
      std::string smallest_hex, largest_hex;
      ls >> m.number >> m.level >> smallest_hex >> largest_hex >> m.entries >> m.file_bytes;
      if (ls.fail() || !FromHex(smallest_hex, &m.smallest) ||
          !FromHex(largest_hex, &m.largest)) {
        return Status::Corruption("bad manifest line: " + line);
      }
      out->files.push_back(std::move(m));
    } else {
      return Status::Corruption("unknown manifest tag: " + tag);
    }
  }
  return Status::OK();
}

Status SaveManifest(const std::string& dir, const ManifestData& data) {
  std::ostringstream out;
  out << "next_file " << data.next_file << "\n";
  out << "last_seq " << data.last_seq << "\n";
  for (const auto& m : data.files) {
    out << "file " << m.number << " " << m.level << " " << ToHex(m.smallest) << " "
        << ToHex(m.largest) << " " << m.entries << " " << m.file_bytes << "\n";
  }
  return WriteFileAtomic((std::filesystem::path(dir) / "MANIFEST").string(), out.str());
}

}  // namespace flotilla::storage
