#include "common/fs.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>

namespace flotilla {

namespace {
Status Errno(const std::string& what) {
  return Status::IOError(what + ": " + std::strerror(errno));
}
}  // namespace

Status ReadFileToString(const std::string& path, std::string* out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) return Status::NotFound(path);
    return Errno("open " + path);
  }
  out->clear();
  char buf[1 << 16];
  ssize_t n;
  while ((n = ::read(fd, buf, sizeof(buf))) > 0) out->append(buf, static_cast<size_t>(n));
  int saved = errno;
  ::close(fd);
  if (n < 0) {
    errno = saved;
    return Errno("read " + path);
  }
  return Status::OK();
}

Status WriteFileAtomic(const std::string& path, const std::string& data) {
  std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Errno("open " + tmp);
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::write(fd, data.data() + off, data.size() - off);
    if (n < 0) {
      ::close(fd);
      return Errno("write " + tmp);
    }
    off += static_cast<size_t>(n);
  }
  if (::fsync(fd) != 0) {
    ::close(fd);
    return Errno("fsync " + tmp);
  }
  ::close(fd);
  if (::rename(tmp.c_str(), path.c_str()) != 0) return Errno("rename " + tmp);
  return SyncDir(std::filesystem::path(path).parent_path().string());
}

Status SyncDir(const std::string& dir) {
  int fd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY);
  if (fd < 0) return Errno("open dir " + dir);
  int rc = ::fsync(fd);
  ::close(fd);
  if (rc != 0) return Errno("fsync dir " + dir);
  return Status::OK();
}

}  // namespace flotilla
