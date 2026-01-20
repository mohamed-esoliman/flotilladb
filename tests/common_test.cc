#include <gtest/gtest.h>

#include <filesystem>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/fs.h"
#include "common/status.h"

namespace flotilla {
namespace {

TEST(Coding, FixedRoundtrip) {
  std::string buf;
  PutFixed8(&buf, 0xAB);
  PutFixed16(&buf, 0xBEEF);
  PutFixed32(&buf, 0xDEADBEEFu);
  PutFixed64(&buf, 0x0123456789ABCDEFull);
  PutLengthPrefixed(&buf, "hello");

  Decoder dec(buf);
  EXPECT_EQ(dec.U8(), 0xAB);
  EXPECT_EQ(dec.U16(), 0xBEEF);
  EXPECT_EQ(dec.U32(), 0xDEADBEEFu);
  EXPECT_EQ(dec.U64(), 0x0123456789ABCDEFull);
  EXPECT_EQ(dec.Str(), "hello");
  EXPECT_TRUE(dec.ok());
  EXPECT_EQ(dec.remaining(), 0u);
}

TEST(Coding, DecoderUnderflow) {
  std::string buf;
  PutFixed32(&buf, 7);
  Decoder dec(buf);
  dec.U64();
  EXPECT_FALSE(dec.ok());
}

TEST(Coding, LengthPrefixTooLong) {
  std::string buf;
  PutFixed32(&buf, 100);
  buf += "short";
  Decoder dec(buf);
  dec.LengthPrefixed();
  EXPECT_FALSE(dec.ok());
}

TEST(Crc32, KnownVectors) {
  EXPECT_EQ(Crc32(""), 0x00000000u);
  EXPECT_EQ(Crc32("123456789"), 0xCBF43926u);
  EXPECT_NE(Crc32("abc"), Crc32("abd"));
}

TEST(Status, Basics) {
  EXPECT_TRUE(Status::OK().ok());
  Status s = Status::NotFound("k1");
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(s.ToString(), "not found: k1");
}

TEST(Fs, AtomicWriteAndRead) {
  auto dir = std::filesystem::temp_directory_path() / "flotilla_fs_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  std::string path = (dir / "f.txt").string();

  ASSERT_TRUE(WriteFileAtomic(path, "v1").ok());
  ASSERT_TRUE(WriteFileAtomic(path, "v2").ok());
  std::string out;
  ASSERT_TRUE(ReadFileToString(path, &out).ok());
  EXPECT_EQ(out, "v2");
  EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));

  Status s = ReadFileToString((dir / "missing").string(), &out);
  EXPECT_TRUE(s.IsNotFound());
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace flotilla
