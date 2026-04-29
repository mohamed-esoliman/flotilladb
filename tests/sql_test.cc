#include <gtest/gtest.h>

#include "sql/catalog.h"
#include "sql/parser.h"

namespace flotilla::sql {
namespace {

TEST(SqlParser, CreateTable) {
  Statement stmt;
  ASSERT_TRUE(
      Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)", &stmt).ok());
  const auto& create = std::get<CreateTableStmt>(stmt);
  EXPECT_EQ(create.table, "users");
  ASSERT_EQ(create.columns.size(), 3u);
  EXPECT_TRUE(create.columns[0].is_pk);
  EXPECT_EQ(create.columns[1].name, "name");
  EXPECT_EQ(create.columns[1].type, ValueType::kText);
}

TEST(SqlParser, CreateTableRequiresOnePk) {
  Statement stmt;
  EXPECT_FALSE(Parse("CREATE TABLE t (a INT, b TEXT)", &stmt).ok());
  EXPECT_FALSE(
      Parse("CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)", &stmt).ok());
  EXPECT_FALSE(Parse("CREATE TABLE t (a INT PRIMARY KEY, a TEXT)", &stmt).ok());
}

TEST(SqlParser, InsertForms) {
  Statement stmt;
  ASSERT_TRUE(Parse("INSERT INTO t VALUES (1, 'ada'), (2, 'grace')", &stmt).ok());
  const auto& insert = std::get<InsertStmt>(stmt);
  ASSERT_EQ(insert.rows.size(), 2u);
  EXPECT_EQ(insert.rows[0][1].s, "ada");

  ASSERT_TRUE(Parse("insert into t (id, name) values (3, 'it''s')", &stmt).ok());
  const auto& insert2 = std::get<InsertStmt>(stmt);
  EXPECT_EQ(insert2.columns.size(), 2u);
  EXPECT_EQ(insert2.rows[0][1].s, "it's");
}

TEST(SqlParser, SelectWithWhere) {
  Statement stmt;
  ASSERT_TRUE(
      Parse("SELECT name, age FROM t WHERE age >= 40 AND name != 'x';", &stmt).ok());
  const auto& select = std::get<SelectStmt>(stmt);
  EXPECT_EQ(select.columns.size(), 2u);
  ASSERT_EQ(select.where.size(), 2u);
  EXPECT_EQ(select.where[0].op, CompareOp::kGe);
  EXPECT_EQ(select.where[0].literal.i, 40);
  EXPECT_EQ(select.where[1].op, CompareOp::kNe);

  ASSERT_TRUE(Parse("SELECT * FROM t", &stmt).ok());
  EXPECT_TRUE(std::get<SelectStmt>(stmt).columns.empty());
}

TEST(SqlParser, UpdateAndDelete) {
  Statement stmt;
  ASSERT_TRUE(Parse("UPDATE t SET age = 37, name = 'b' WHERE id = 1", &stmt).ok());
  const auto& update = std::get<UpdateStmt>(stmt);
  EXPECT_EQ(update.sets.size(), 2u);
  EXPECT_EQ(update.where.size(), 1u);

  ASSERT_TRUE(Parse("DELETE FROM t WHERE age < 0", &stmt).ok());
  EXPECT_EQ(std::get<DeleteStmt>(stmt).where[0].op, CompareOp::kLt);

  ASSERT_TRUE(Parse("DROP TABLE t", &stmt).ok());
  EXPECT_EQ(std::get<DropTableStmt>(stmt).table, "t");
}

TEST(SqlParser, ErrorsCarryPosition) {
  Statement stmt;
  Status s = Parse("SELECT FROM", &stmt);
  EXPECT_FALSE(s.ok());
  EXPECT_NE(s.ToString().find("offset"), std::string::npos);
  EXPECT_FALSE(Parse("SELECT * FROM t WHERE", &stmt).ok());
  EXPECT_FALSE(Parse("INSERT INTO t VALUES (1", &stmt).ok());
  EXPECT_FALSE(Parse("SELECT * FROM t garbage", &stmt).ok());
  EXPECT_FALSE(Parse("", &stmt).ok());
}

TEST(SqlCatalog, SchemaRoundtrip) {
  TableSchema schema;
  schema.table_id = 7;
  schema.name = "users";
  schema.columns = {{"id", ValueType::kInt, true}, {"name", ValueType::kText, false}};
  TableSchema decoded;
  ASSERT_TRUE(DecodeSchema("users", EncodeSchema(schema), &decoded));
  EXPECT_EQ(decoded.table_id, 7u);
  EXPECT_EQ(decoded.PkIndex(), 0);
  EXPECT_EQ(decoded.columns[1].name, "name");
}

TEST(SqlCatalog, PkEncodingPreservesIntOrder) {
  std::vector<int64_t> values = {-1000, -1, 0, 1, 42, 1000000, INT64_MIN, INT64_MAX};
  std::vector<std::pair<std::string, int64_t>> encoded;
  for (int64_t v : values) encoded.emplace_back(EncodePkValue(Value::Int(v)), v);
  std::sort(encoded.begin(), encoded.end());
  for (size_t i = 1; i < encoded.size(); i++) {
    EXPECT_LT(encoded[i - 1].second, encoded[i].second);
  }
}

TEST(SqlCatalog, RowRoundtrip) {
  TableSchema schema;
  schema.table_id = 1;
  schema.columns = {{"id", ValueType::kInt, true}, {"name", ValueType::kText, false}};
  std::vector<Value> row = {Value::Int(-5), Value::Text("with 'quotes' and \0"
                                                        "nul")};
  std::vector<Value> decoded;
  ASSERT_TRUE(DecodeRow(EncodeRow(row), schema, &decoded));
  EXPECT_EQ(decoded[0].i, -5);
  EXPECT_EQ(decoded[1].s, row[1].s);
  // Wrong column count rejected.
  TableSchema wider = schema;
  wider.columns.push_back({"x", ValueType::kInt, false});
  EXPECT_FALSE(DecodeRow(EncodeRow(row), wider, &decoded));
}

}  // namespace
}  // namespace flotilla::sql
