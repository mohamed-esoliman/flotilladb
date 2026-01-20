#pragma once

#include <filesystem>
#include <string>

namespace flotilla::test {

// Creates a fresh empty directory under the system temp dir, removed on
// destruction unless kept.
class TempDir {
 public:
  explicit TempDir(const std::string& name) {
    path_ = (std::filesystem::temp_directory_path() / ("flotilla_" + name)).string();
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }
  ~TempDir() { std::filesystem::remove_all(path_); }

  const std::string& path() const { return path_; }
  std::string file(const std::string& name) const {
    return (std::filesystem::path(path_) / name).string();
  }

 private:
  std::string path_;
};

}  // namespace flotilla::test
