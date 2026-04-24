#include "sql/catalog.h"

#include "common/coding.h"

namespace flotilla::sql {

std::string CatalogKey(const std::string& table_name) { return "__cat/" + table_name; }

std::string EncodeSchema(const TableSchema& schema) {
  std::string out;
  PutFixed32(&out, schema.table_id);
  PutFixed32(&out, static_cast<uint32_t>(schema.columns.size()));
  for (const auto& col : schema.columns) {
    PutLengthPrefixed(&out, col.name);
    PutFixed8(&out, static_cast<uint8_t>(col.type));
    PutFixed8(&out, col.is_pk ? 1 : 0);
  }
  return out;
}

bool DecodeSchema(const std::string& name, std::string_view data, TableSchema* out) {
  Decoder dec(data);
  out->name = name;
  out->table_id = dec.U32();
  uint32_t ncols = dec.U32();
  out->columns.clear();
  for (uint32_t i = 0; i < ncols && dec.ok(); i++) {
    ColumnDef col;
    col.name = dec.Str();
    col.type = static_cast<ValueType>(dec.U8());
    col.is_pk = dec.U8() != 0;
    out->columns.push_back(std::move(col));
  }
  return dec.ok() && dec.remaining() == 0 && out->PkIndex() >= 0;
}

std::string TablePrefix(uint32_t table_id) {
  std::string out = "t";
  for (int i = 3; i >= 0; i--) out.push_back(static_cast<char>(table_id >> (8 * i)));
  out += "r";
  return out;
}

std::string EncodePkValue(const Value& pk) {
  if (pk.type == ValueType::kInt) {
    // Sign-flipped big-endian preserves signed integer order bytewise.
    uint64_t bits = static_cast<uint64_t>(pk.i) ^ (1ull << 63);
    std::string out;
    for (int i = 7; i >= 0; i--) out.push_back(static_cast<char>(bits >> (8 * i)));
    return out;
  }
  return pk.s;
}

std::string RowKey(uint32_t table_id, const Value& pk) {
  return TablePrefix(table_id) + EncodePkValue(pk);
}

std::string EncodeRow(const std::vector<Value>& values) {
  std::string out;
  PutFixed32(&out, static_cast<uint32_t>(values.size()));
  for (const auto& value : values) {
    PutFixed8(&out, static_cast<uint8_t>(value.type));
    if (value.type == ValueType::kInt) {
      std::string bytes;
      PutFixed64(&bytes, static_cast<uint64_t>(value.i));
      PutLengthPrefixed(&out, bytes);
    } else {
      PutLengthPrefixed(&out, value.s);
    }
  }
  return out;
}

bool DecodeRow(std::string_view data, const TableSchema& schema,
               std::vector<Value>* out) {
  Decoder dec(data);
  uint32_t count = dec.U32();
  if (count != schema.columns.size()) return false;
  out->clear();
  for (uint32_t i = 0; i < count && dec.ok(); i++) {
    Value value;
    value.type = static_cast<ValueType>(dec.U8());
    std::string bytes = dec.Str();
    if (value.type == ValueType::kInt) {
      if (bytes.size() != 8) return false;
      value.i = static_cast<int64_t>(DecodeFixed64(bytes.data()));
    } else {
      value.s = std::move(bytes);
    }
    out->push_back(std::move(value));
  }
  return dec.ok() && dec.remaining() == 0;
}

}  // namespace flotilla::sql
