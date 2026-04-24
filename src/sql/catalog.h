#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "sql/ast.h"

namespace flotilla::sql {

struct TableSchema {
  uint32_t table_id = 0;
  std::string name;
  std::vector<ColumnDef> columns;

  int PkIndex() const {
    for (size_t i = 0; i < columns.size(); i++) {
      if (columns[i].is_pk) return static_cast<int>(i);
    }
    return -1;
  }
  int ColumnIndex(const std::string& name_in) const {
    for (size_t i = 0; i < columns.size(); i++) {
      if (columns[i].name == name_in) return static_cast<int>(i);
    }
    return -1;
  }
};

// Catalog keys (in the transactional keyspace):
std::string CatalogKey(const std::string& table_name);
inline constexpr char kNextTableIdKey[] = "__cat_next_id";

std::string EncodeSchema(const TableSchema& schema);
bool DecodeSchema(const std::string& name, std::string_view data, TableSchema* out);

// Row keys: "t" + table_id (BE) + "r" + order-preserving primary key.
std::string TablePrefix(uint32_t table_id);
std::string RowKey(uint32_t table_id, const Value& pk);
std::string EncodePkValue(const Value& pk);

std::string EncodeRow(const std::vector<Value>& values);
bool DecodeRow(std::string_view data, const TableSchema& schema,
               std::vector<Value>* out);

}  // namespace flotilla::sql
