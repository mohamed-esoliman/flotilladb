#include "sql/executor.h"

#include "client/txn_client.h"
#include "common/coding.h"
#include "sql/catalog.h"
#include "sql/parser.h"

namespace flotilla::sql {

namespace {

bool Matches(const Predicate& pred, const Value& value) {
  if (value.type != pred.literal.type) return false;
  int cmp = value.Compare(pred.literal);
  switch (pred.op) {
    case CompareOp::kEq: return cmp == 0;
    case CompareOp::kNe: return cmp != 0;
    case CompareOp::kLt: return cmp < 0;
    case CompareOp::kLe: return cmp <= 0;
    case CompareOp::kGt: return cmp > 0;
    case CompareOp::kGe: return cmp >= 0;
  }
  return false;
}

class Executor {
 public:
  explicit Executor(client::Txn* txn) : txn_(txn) {}

  Status Run(const Statement& stmt, ResultSet* out) {
    return std::visit([&](const auto& s) { return Execute(s, out); }, stmt);
  }

 private:
  Status LoadSchema(const std::string& table, TableSchema* schema) {
    std::string raw;
    Status s = txn_->Get(CatalogKey(table), &raw);
    if (s.IsNotFound()) return Status::InvalidArgument("unknown table " + table);
    if (!s.ok()) return s;
    if (!DecodeSchema(table, raw, schema)) {
      return Status::Corruption("bad schema for " + table);
    }
    return Status::OK();
  }

  Status CheckWhere(const TableSchema& schema, const std::vector<Predicate>& where) {
    for (const auto& pred : where) {
      int idx = schema.ColumnIndex(pred.column);
      if (idx < 0) return Status::InvalidArgument("unknown column " + pred.column);
      if (schema.columns[static_cast<size_t>(idx)].type != pred.literal.type) {
        return Status::InvalidArgument("type mismatch on " + pred.column);
      }
    }
    return Status::OK();
  }

  bool RowMatches(const TableSchema& schema, const std::vector<Predicate>& where,
                  const std::vector<Value>& row) {
    for (const auto& pred : where) {
      int idx = schema.ColumnIndex(pred.column);
      if (idx < 0 || !Matches(pred, row[static_cast<size_t>(idx)])) return false;
    }
    return true;
  }

  // The unique point-lookup pk value, if the WHERE clause pins it.
  const Value* PointLookup(const TableSchema& schema,
                           const std::vector<Predicate>& where) {
    int pk = schema.PkIndex();
    for (const auto& pred : where) {
      if (pred.op == CompareOp::kEq &&
          schema.ColumnIndex(pred.column) == pk) {
        return &pred.literal;
      }
    }
    return nullptr;
  }

  // Fetches matching rows as (row_key, values) pairs.
  Status FetchRows(const TableSchema& schema, const std::vector<Predicate>& where,
                   std::vector<std::pair<std::string, std::vector<Value>>>* rows) {
    Status s = CheckWhere(schema, where);
    if (!s.ok()) return s;

    if (const Value* pk = PointLookup(schema, where)) {
      std::string key = RowKey(schema.table_id, *pk);
      std::string raw;
      s = txn_->Get(key, &raw);
      if (s.IsNotFound()) return Status::OK();
      if (!s.ok()) return s;
      std::vector<Value> row;
      if (!DecodeRow(raw, schema, &row)) return Status::Corruption("bad row");
      if (RowMatches(schema, where, row)) rows->emplace_back(key, std::move(row));
      return Status::OK();
    }

    std::string prefix = TablePrefix(schema.table_id);
    std::string end = prefix;
    end.back() = static_cast<char>(end.back() + 1);  // "t<id>r" -> "t<id>s"
    std::vector<std::pair<std::string, std::string>> raw_rows;
    s = txn_->Scan(prefix, end, 1u << 20, &raw_rows);
    if (!s.ok()) return s;
    for (auto& [key, raw] : raw_rows) {
      std::vector<Value> row;
      if (!DecodeRow(raw, schema, &row)) return Status::Corruption("bad row");
      if (RowMatches(schema, where, row)) rows->emplace_back(key, std::move(row));
    }
    return Status::OK();
  }

  Status Execute(const CreateTableStmt& stmt, ResultSet* out) {
    std::string existing;
    Status s = txn_->Get(CatalogKey(stmt.table), &existing);
    if (s.ok()) return Status::InvalidArgument("table " + stmt.table + " exists");
    if (!s.IsNotFound()) return s;

    uint32_t table_id = 1;
    std::string raw;
    s = txn_->Get(kNextTableIdKey, &raw);
    if (s.ok() && raw.size() == 4) {
      table_id = DecodeFixed32(raw.data());
    } else if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    std::string next;
    PutFixed32(&next, table_id + 1);
    txn_->Put(kNextTableIdKey, next);

    TableSchema schema;
    schema.table_id = table_id;
    schema.name = stmt.table;
    schema.columns = stmt.columns;
    txn_->Put(CatalogKey(stmt.table), EncodeSchema(schema));
    out->message = "table " + stmt.table + " created";
    return Status::OK();
  }

  Status Execute(const DropTableStmt& stmt, ResultSet* out) {
    TableSchema schema;
    Status s = LoadSchema(stmt.table, &schema);
    if (!s.ok()) return s;
    std::vector<std::pair<std::string, std::vector<Value>>> rows;
    s = FetchRows(schema, {}, &rows);
    if (!s.ok()) return s;
    for (const auto& [key, row] : rows) txn_->Delete(key);
    txn_->Delete(CatalogKey(stmt.table));
    out->affected = rows.size();
    out->message = "table " + stmt.table + " dropped";
    return Status::OK();
  }

  Status Execute(const InsertStmt& stmt, ResultSet* out) {
    TableSchema schema;
    Status s = LoadSchema(stmt.table, &schema);
    if (!s.ok()) return s;

    // Map the provided column order onto the schema order.
    std::vector<int> mapping;
    if (stmt.columns.empty()) {
      for (size_t i = 0; i < schema.columns.size(); i++) {
        mapping.push_back(static_cast<int>(i));
      }
    } else {
      if (stmt.columns.size() != schema.columns.size()) {
        return Status::InvalidArgument("all columns must be provided");
      }
      for (const auto& name : stmt.columns) {
        int idx = schema.ColumnIndex(name);
        if (idx < 0) return Status::InvalidArgument("unknown column " + name);
        mapping.push_back(idx);
      }
    }

    for (const auto& literals : stmt.rows) {
      if (literals.size() != schema.columns.size()) {
        return Status::InvalidArgument("wrong value count");
      }
      std::vector<Value> row(schema.columns.size());
      for (size_t i = 0; i < literals.size(); i++) {
        size_t target = static_cast<size_t>(mapping[i]);
        if (literals[i].type != schema.columns[target].type) {
          return Status::InvalidArgument("type mismatch for column " +
                                         schema.columns[target].name);
        }
        row[target] = literals[i];
      }
      const Value& pk = row[static_cast<size_t>(schema.PkIndex())];
      std::string key = RowKey(schema.table_id, pk);
      std::string existing;
      s = txn_->Get(key, &existing);
      if (s.ok()) {
        return Status::InvalidArgument("duplicate primary key " + pk.ToString());
      }
      if (!s.IsNotFound()) return s;
      txn_->Put(key, EncodeRow(row));
      out->affected++;
    }
    return Status::OK();
  }

  Status Execute(const SelectStmt& stmt, ResultSet* out) {
    TableSchema schema;
    Status s = LoadSchema(stmt.table, &schema);
    if (!s.ok()) return s;

    std::vector<int> projection;
    if (stmt.columns.empty()) {
      for (size_t i = 0; i < schema.columns.size(); i++) {
        projection.push_back(static_cast<int>(i));
        out->columns.push_back(schema.columns[i].name);
      }
    } else {
      for (const auto& name : stmt.columns) {
        int idx = schema.ColumnIndex(name);
        if (idx < 0) return Status::InvalidArgument("unknown column " + name);
        projection.push_back(idx);
        out->columns.push_back(name);
      }
    }

    std::vector<std::pair<std::string, std::vector<Value>>> rows;
    s = FetchRows(schema, stmt.where, &rows);
    if (!s.ok()) return s;
    for (const auto& [key, row] : rows) {
      std::vector<std::string> rendered;
      for (int idx : projection) {
        rendered.push_back(row[static_cast<size_t>(idx)].ToString());
      }
      out->rows.push_back(std::move(rendered));
    }
    return Status::OK();
  }

  Status Execute(const UpdateStmt& stmt, ResultSet* out) {
    TableSchema schema;
    Status s = LoadSchema(stmt.table, &schema);
    if (!s.ok()) return s;
    for (const auto& [name, value] : stmt.sets) {
      int idx = schema.ColumnIndex(name);
      if (idx < 0) return Status::InvalidArgument("unknown column " + name);
      if (schema.columns[static_cast<size_t>(idx)].type != value.type) {
        return Status::InvalidArgument("type mismatch on " + name);
      }
      if (idx == schema.PkIndex()) {
        return Status::InvalidArgument("cannot update the primary key");
      }
    }

    std::vector<std::pair<std::string, std::vector<Value>>> rows;
    s = FetchRows(schema, stmt.where, &rows);
    if (!s.ok()) return s;
    for (auto& [key, row] : rows) {
      for (const auto& [name, value] : stmt.sets) {
        row[static_cast<size_t>(schema.ColumnIndex(name))] = value;
      }
      txn_->Put(key, EncodeRow(row));
      out->affected++;
    }
    return Status::OK();
  }

  Status Execute(const DeleteStmt& stmt, ResultSet* out) {
    TableSchema schema;
    Status s = LoadSchema(stmt.table, &schema);
    if (!s.ok()) return s;
    std::vector<std::pair<std::string, std::vector<Value>>> rows;
    s = FetchRows(schema, stmt.where, &rows);
    if (!s.ok()) return s;
    for (const auto& [key, row] : rows) {
      txn_->Delete(key);
      out->affected++;
    }
    return Status::OK();
  }

  client::Txn* txn_;
};

}  // namespace

Status Execute(client::Client* client, const std::string& statement, ResultSet* out) {
  Statement stmt;
  Status s = Parse(statement, &stmt);
  if (!s.ok()) return s;

  client::Txn txn(client);
  s = txn.Begin();
  if (!s.ok()) return s;
  Executor executor(&txn);
  *out = ResultSet{};
  s = executor.Run(stmt, out);
  if (!s.ok()) {
    txn.Rollback();
    return s;
  }
  return txn.Commit();
}

}  // namespace flotilla::sql
