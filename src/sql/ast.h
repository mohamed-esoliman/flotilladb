#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace flotilla::sql {

enum class ValueType : uint8_t { kInt = 1, kText = 2 };

struct Value {
  ValueType type = ValueType::kInt;
  int64_t i = 0;
  std::string s;

  static Value Int(int64_t v) {
    Value value;
    value.type = ValueType::kInt;
    value.i = v;
    return value;
  }
  static Value Text(std::string v) {
    Value value;
    value.type = ValueType::kText;
    value.s = std::move(v);
    return value;
  }

  bool operator==(const Value& other) const {
    if (type != other.type) return false;
    return type == ValueType::kInt ? i == other.i : s == other.s;
  }
  // Comparison across matching types only; callers type-check first.
  int Compare(const Value& other) const {
    if (type == ValueType::kInt) return i < other.i ? -1 : i > other.i ? 1 : 0;
    return s.compare(other.s) < 0 ? -1 : s > other.s ? 1 : 0;
  }
  std::string ToString() const { return type == ValueType::kInt ? std::to_string(i) : s; }
};

struct ColumnDef {
  std::string name;
  ValueType type;
  bool is_pk = false;
};

enum class CompareOp { kEq, kNe, kLt, kLe, kGt, kGe };

struct Predicate {
  std::string column;
  CompareOp op;
  Value literal;
};

struct CreateTableStmt {
  std::string table;
  std::vector<ColumnDef> columns;
};

struct DropTableStmt {
  std::string table;
};

struct InsertStmt {
  std::string table;
  std::vector<std::string> columns;  // empty = schema order
  std::vector<std::vector<Value>> rows;
};

struct SelectStmt {
  std::string table;
  std::vector<std::string> columns;  // empty = *
  std::vector<Predicate> where;      // ANDed
};

struct UpdateStmt {
  std::string table;
  std::vector<std::pair<std::string, Value>> sets;
  std::vector<Predicate> where;
};

struct DeleteStmt {
  std::string table;
  std::vector<Predicate> where;
};

using Statement = std::variant<CreateTableStmt, DropTableStmt, InsertStmt, SelectStmt,
                               UpdateStmt, DeleteStmt>;

}  // namespace flotilla::sql
