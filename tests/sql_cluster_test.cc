#include <gtest/gtest.h>

#include "client/client.h"
#include "cluster_harness.h"
#include "sql/executor.h"

namespace flotilla::server {
namespace {

using testing::HarnessCluster;

class SqlClusterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cluster_ = std::make_unique<HarnessCluster>("sql_cluster");
    ASSERT_NE(cluster_->WaitForLeader(), 0);
    client_ = std::make_unique<client::Client>(cluster_->ClientAddrs());
  }

  sql::ResultSet Must(const std::string& statement) {
    sql::ResultSet result;
    Status s = sql::Execute(client_.get(), statement, &result);
    EXPECT_TRUE(s.ok()) << statement << ": " << s.ToString();
    return result;
  }

  Status Try(const std::string& statement) {
    sql::ResultSet result;
    return sql::Execute(client_.get(), statement, &result);
  }

  std::unique_ptr<HarnessCluster> cluster_;
  std::unique_ptr<client::Client> client_;
};

TEST_F(SqlClusterTest, EndToEndCrud) {
  Must("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)");
  auto r = Must("INSERT INTO users VALUES (1, 'ada', 36), (2, 'grace', 45), "
                "(3, 'edsger', 72)");
  EXPECT_EQ(r.affected, 3u);

  r = Must("SELECT * FROM users");
  ASSERT_EQ(r.rows.size(), 3u);
  EXPECT_EQ(r.columns, (std::vector<std::string>{"id", "name", "age"}));
  EXPECT_EQ(r.rows[0], (std::vector<std::string>{"1", "ada", "36"}));

  r = Must("SELECT name FROM users WHERE age >= 40 AND age < 50");
  ASSERT_EQ(r.rows.size(), 1u);
  EXPECT_EQ(r.rows[0][0], "grace");

  r = Must("SELECT age FROM users WHERE id = 3");
  ASSERT_EQ(r.rows.size(), 1u);
  EXPECT_EQ(r.rows[0][0], "72");

  r = Must("UPDATE users SET age = 37 WHERE name = 'ada'");
  EXPECT_EQ(r.affected, 1u);
  r = Must("SELECT age FROM users WHERE id = 1");
  EXPECT_EQ(r.rows[0][0], "37");

  r = Must("DELETE FROM users WHERE age > 70");
  EXPECT_EQ(r.affected, 1u);
  r = Must("SELECT * FROM users");
  EXPECT_EQ(r.rows.size(), 2u);
}

TEST_F(SqlClusterTest, ErrorsAndConstraints) {
  Must("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
  EXPECT_FALSE(Try("CREATE TABLE t (id INT PRIMARY KEY)").ok());  // exists
  EXPECT_FALSE(Try("SELECT * FROM missing").ok());
  EXPECT_FALSE(Try("SELECT nope FROM t").ok());
  EXPECT_FALSE(Try("INSERT INTO t VALUES ('text-in-int', 'v')").ok());

  Must("INSERT INTO t VALUES (1, 'a')");
  EXPECT_FALSE(Try("INSERT INTO t VALUES (1, 'dup')").ok());  // duplicate pk
  EXPECT_FALSE(Try("UPDATE t SET id = 9 WHERE id = 1").ok());  // pk immutable
  auto r = Must("SELECT v FROM t WHERE id = 1");
  EXPECT_EQ(r.rows[0][0], "a");  // failed insert changed nothing

  Must("DROP TABLE t");
  EXPECT_FALSE(Try("SELECT * FROM t").ok());
}

TEST_F(SqlClusterTest, RowsSurviveSplitAndMultiRowUpdateIsAtomic) {
  Must("CREATE TABLE inv (sku INT PRIMARY KEY, qty INT)");
  for (int i = 0; i < 20; i++) {
    Must("INSERT INTO inv VALUES (" + std::to_string(i) + ", 10)");
  }
  // Split the raw keyspace under the table's rows.
  client::Client raw(cluster_->ClientAddrs());
  ASSERT_TRUE(raw.Split(std::string("t")).ok());
  ASSERT_TRUE(cluster_->WaitForGroupCount(2));

  auto r = Must("UPDATE inv SET qty = 5");
  EXPECT_EQ(r.affected, 20u);
  r = Must("SELECT * FROM inv WHERE qty = 5");
  EXPECT_EQ(r.rows.size(), 20u);

  // Ordered by primary key.
  r = Must("SELECT sku FROM inv");
  for (size_t i = 0; i < r.rows.size(); i++) {
    EXPECT_EQ(r.rows[i][0], std::to_string(i));
  }
}

TEST_F(SqlClusterTest, NegativePksSortCorrectly) {
  Must("CREATE TABLE n (id INT PRIMARY KEY)");
  Must("INSERT INTO n VALUES (5), (-3), (0), (-100), (42)");
  auto r = Must("SELECT id FROM n");
  ASSERT_EQ(r.rows.size(), 5u);
  EXPECT_EQ(r.rows[0][0], "-100");
  EXPECT_EQ(r.rows[1][0], "-3");
  EXPECT_EQ(r.rows[2][0], "0");
  EXPECT_EQ(r.rows[4][0], "42");
}

TEST_F(SqlClusterTest, TextPrimaryKey) {
  Must("CREATE TABLE kv (k TEXT PRIMARY KEY, v INT)");
  Must("INSERT INTO kv VALUES ('banana', 2), ('apple', 1), ('cherry', 3)");
  auto r = Must("SELECT k FROM kv");
  EXPECT_EQ(r.rows[0][0], "apple");
  EXPECT_EQ(r.rows[2][0], "cherry");
  r = Must("SELECT v FROM kv WHERE k = 'banana'");
  EXPECT_EQ(r.rows[0][0], "2");
}

}  // namespace
}  // namespace flotilla::server
