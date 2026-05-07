#include "storage/db.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

#include "testutil.h"

namespace flotilla::storage {
namespace {

Options SmallOptions() {
  Options o;
  o.write_buffer_size = 4096;  // force frequent flushes
  o.block_size = 512;
  o.l0_compaction_trigger = 4;
  o.level_base_bytes = 16 << 10;
  o.max_output_file_bytes = 8 << 10;
  o.fsync_writes = false;  // most tests don't need durability
  return o;
}

TEST(Db, PutGetDelete) {
  test::TempDir dir("db_basic");
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), dir.path(), &db).ok());

  ASSERT_TRUE(db->Put("k1", "v1").ok());
  ASSERT_TRUE(db->Put("k2", "v2").ok());
  std::string v;
  ASSERT_TRUE(db->Get("k1", &v).ok());
  EXPECT_EQ(v, "v1");
  ASSERT_TRUE(db->Put("k1", "v1b").ok());
  ASSERT_TRUE(db->Get("k1", &v).ok());
  EXPECT_EQ(v, "v1b");
  ASSERT_TRUE(db->Delete("k1").ok());
  EXPECT_TRUE(db->Get("k1", &v).IsNotFound());
  ASSERT_TRUE(db->Get("k2", &v).ok());
  EXPECT_TRUE(db->Get("never", &v).IsNotFound());
}

TEST(Db, SurvivesFlushesAndCompactions) {
  test::TempDir dir("db_churn");
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), dir.path(), &db).ok());

  const int kKeys = 500;
  // Write everything three times so compaction has versions to collapse.
  for (int round = 0; round < 3; round++) {
    for (int i = 0; i < kKeys; i++) {
      std::string k = "key" + std::to_string(i);
      ASSERT_TRUE(db->Put(k, "r" + std::to_string(round) + "-" + std::to_string(i)).ok());
    }
  }
  // Delete a slice.
  for (int i = 0; i < kKeys; i += 5) {
    ASSERT_TRUE(db->Delete("key" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(db->Flush().ok());

  std::string v;
  for (int i = 0; i < kKeys; i++) {
    std::string k = "key" + std::to_string(i);
    Status s = db->Get(k, &v);
    if (i % 5 == 0) {
      EXPECT_TRUE(s.IsNotFound()) << k;
    } else {
      ASSERT_TRUE(s.ok()) << k << " " << s.ToString();
      EXPECT_EQ(v, "r2-" + std::to_string(i));
    }
  }
  auto stats = db->GetStats();
  size_t total_files = 0;
  for (size_t n : stats.files_per_level) total_files += n;
  EXPECT_GT(total_files, 0u);
}

TEST(Db, TombstoneSurvivesFlushWithOlderVersionInL0) {
  // Regression: a tombstone flushed to L0 must not be dropped while an older
  // version of the key still sits in a sibling L0 file.
  test::TempDir dir("db_tombstone_l0");
  Options opts = SmallOptions();
  opts.l0_compaction_trigger = 100;  // keep everything in L0
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());

  ASSERT_TRUE(db->Put("k", "v").ok());
  ASSERT_TRUE(db->Flush().ok());  // L0 file with the put
  ASSERT_TRUE(db->Delete("k").ok());
  ASSERT_TRUE(db->Flush().ok());  // L0 file with the tombstone

  std::string v;
  EXPECT_TRUE(db->Get("k", &v).IsNotFound());
  auto it = db->NewIterator();
  it->Seek("k");
  EXPECT_TRUE(!it->Valid() || it->key() != "k");
}

TEST(Db, ScanSeesNewestAndSkipsTombstones) {
  test::TempDir dir("db_scan");
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), dir.path(), &db).ok());

  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%04d", i);
    ASSERT_TRUE(db->Put(buf, "old").ok());
  }
  ASSERT_TRUE(db->Flush().ok());
  for (int i = 0; i < 100; i += 2) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%04d", i);
    ASSERT_TRUE(db->Put(buf, "new").ok());
  }
  for (int i = 1; i < 100; i += 4) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%04d", i);
    ASSERT_TRUE(db->Delete(buf).ok());
  }

  auto it = db->NewIterator();
  int count = 0;
  std::string prev;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string k(it->key());
    EXPECT_GT(k, prev);
    prev = k;
    int i = atoi(k.substr(1).c_str());
    if (i % 4 == 1) FAIL() << "tombstoned key visible: " << k;
    EXPECT_EQ(it->value(), i % 2 == 0 ? "new" : "old");
    count++;
  }
  EXPECT_EQ(count, 75);

  it->Seek("k0050");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key(), "k0050");
  it->Seek("k0049");  // deleted (49 % 4 == 1)
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key(), "k0050");
}

TEST(Db, RecoversFromWalAfterUncleanStop) {
  test::TempDir dir("db_wal_recover");
  Options opts = SmallOptions();
  opts.fsync_writes = true;
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());
    ASSERT_TRUE(db->Put("a", "1").ok());
    ASSERT_TRUE(db->Put("b", "2").ok());
    ASSERT_TRUE(db->Delete("a").ok());
    // No flush: data lives only in WAL + memtable. Destructor does not flush.
  }
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());
    std::string v;
    EXPECT_TRUE(db->Get("a", &v).IsNotFound());
    ASSERT_TRUE(db->Get("b", &v).ok());
    EXPECT_EQ(v, "2");
    EXPECT_GE(db->LastSequence(), 3u);
    // Sequence numbers keep increasing after recovery.
    ASSERT_TRUE(db->Put("c", "3").ok());
    ASSERT_TRUE(db->Get("c", &v).ok());
  }
}

TEST(Db, RecoversAcrossFlushedAndUnflushedData) {
  test::TempDir dir("db_mixed_recover");
  Options opts = SmallOptions();
  opts.fsync_writes = true;
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());
    for (int i = 0; i < 300; i++) {
      ASSERT_TRUE(db->Put("k" + std::to_string(i), "v" + std::to_string(i)).ok());
    }
    ASSERT_TRUE(db->Flush().ok());
    for (int i = 0; i < 50; i++) {
      ASSERT_TRUE(db->Put("k" + std::to_string(i), "updated").ok());
    }
  }
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());
    std::string v;
    ASSERT_TRUE(db->Get("k10", &v).ok());
    EXPECT_EQ(v, "updated");
    ASSERT_TRUE(db->Get("k200", &v).ok());
    EXPECT_EQ(v, "v200");
  }
}

TEST(Db, TornWalTailLosesOnlyTail) {
  test::TempDir dir("db_torn");
  Options opts = SmallOptions();
  opts.fsync_writes = true;
  opts.write_buffer_size = 1 << 20;  // keep everything in one WAL
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());
    ASSERT_TRUE(db->Put("a", "1").ok());
    ASSERT_TRUE(db->Put("b", "2").ok());
  }
  // Corrupt the WAL tail: chop off the last 3 bytes of the newest WAL.
  std::filesystem::path wal;
  for (const auto& de : std::filesystem::directory_iterator(dir.path())) {
    if (de.path().extension() == ".wal" &&
        (wal.empty() || de.path().filename() > wal.filename())) {
      wal = de.path();
    }
  }
  ASSERT_FALSE(wal.empty());
  auto size = std::filesystem::file_size(wal);
  ASSERT_GT(size, 3u);
  std::filesystem::resize_file(wal, size - 3);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, dir.path(), &db).ok());
  std::string v;
  ASSERT_TRUE(db->Get("a", &v).ok());
  EXPECT_EQ(v, "1");
  EXPECT_TRUE(db->Get("b", &v).IsNotFound());
}

TEST(Db, CheckpointOpensAsIndependentDb) {
  test::TempDir dir("db_ckpt");
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), dir.path(), &db).ok());
  for (int i = 0; i < 200; i++) {
    ASSERT_TRUE(db->Put("k" + std::to_string(i), "v" + std::to_string(i)).ok());
  }
  std::string ckpt = dir.file("checkpoint");
  ASSERT_TRUE(db->Checkpoint(ckpt).ok());

  // Writes after the checkpoint must not leak into it.
  ASSERT_TRUE(db->Put("k5", "changed").ok());
  ASSERT_TRUE(db->Delete("k6").ok());

  std::unique_ptr<DB> copy;
  ASSERT_TRUE(DB::Open(SmallOptions(), ckpt, &copy).ok());
  std::string v;
  ASSERT_TRUE(copy->Get("k5", &v).ok());
  EXPECT_EQ(v, "v5");
  ASSERT_TRUE(copy->Get("k6", &v).ok());
  ASSERT_TRUE(copy->Get("k199", &v).ok());

  // And the copy is writable on its own.
  ASSERT_TRUE(copy->Put("own", "x").ok());
  ASSERT_TRUE(copy->Get("own", &v).ok());
  EXPECT_TRUE(db->Get("own", &v).IsNotFound());
}

TEST(Db, ConcurrentReadersDuringWrites) {
  test::TempDir dir("db_concurrent");
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), dir.path(), &db).ok());

  std::atomic<bool> stop{false};
  std::atomic<int> read_errors{0};
  std::thread reader([&] {
    std::string v;
    while (!stop.load()) {
      for (int i = 0; i < 50; i++) {
        Status s = db->Get("k" + std::to_string(i), &v);
        if (!s.ok() && !s.IsNotFound()) read_errors.fetch_add(1);
      }
      auto it = db->NewIterator();
      std::string prev;
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string k(it->key());
        if (k <= prev) read_errors.fetch_add(1);
        prev = k;
      }
    }
  });

  for (int round = 0; round < 20; round++) {
    for (int i = 0; i < 50; i++) {
      ASSERT_TRUE(db->Put("k" + std::to_string(i), std::string(100, 'x')).ok());
    }
  }
  stop.store(true);
  reader.join();
  EXPECT_EQ(read_errors.load(), 0);
}

}  // namespace
}  // namespace flotilla::storage
