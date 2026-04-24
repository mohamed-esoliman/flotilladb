#include "sql/parser.h"

#include "sql/lexer.h"

namespace flotilla::sql {

namespace {

class Parser {
 public:
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  Status ParseStatement(Statement* out) {
    if (AcceptKeyword("CREATE")) return ParseCreate(out);
    if (AcceptKeyword("DROP")) return ParseDrop(out);
    if (AcceptKeyword("INSERT")) return ParseInsert(out);
    if (AcceptKeyword("SELECT")) return ParseSelect(out);
    if (AcceptKeyword("UPDATE")) return ParseUpdate(out);
    if (AcceptKeyword("DELETE")) return ParseDelete(out);
    return Error("expected CREATE, DROP, INSERT, SELECT, UPDATE, or DELETE");
  }

  Status Finish() {
    AcceptSymbol(";");
    if (Peek().type != TokenType::kEnd) return Error("unexpected trailing input");
    return Status::OK();
  }

 private:
  const Token& Peek() const { return tokens_[pos_]; }
  const Token& Advance() { return tokens_[pos_++]; }

  bool AcceptKeyword(const std::string& kw) {
    if (Peek().type == TokenType::kKeyword && Peek().text == kw) {
      pos_++;
      return true;
    }
    return false;
  }

  bool AcceptSymbol(const std::string& sym) {
    if (Peek().type == TokenType::kSymbol && Peek().text == sym) {
      pos_++;
      return true;
    }
    return false;
  }

  Status Error(const std::string& expected) const {
    const Token& token = Peek();
    std::string got = token.type == TokenType::kEnd ? "end of input"
                                                    : "'" + token.text + "'";
    return Status::InvalidArgument(expected + " at " + got + " (offset " +
                                   std::to_string(token.pos) + ")");
  }

  Status ExpectKeyword(const std::string& kw) {
    if (!AcceptKeyword(kw)) return Error("expected " + kw);
    return Status::OK();
  }

  Status ExpectSymbol(const std::string& sym) {
    if (!AcceptSymbol(sym)) return Error("expected '" + sym + "'");
    return Status::OK();
  }

  Status ExpectIdentifier(std::string* out) {
    if (Peek().type != TokenType::kIdentifier) return Error("expected identifier");
    *out = Advance().text;
    return Status::OK();
  }

  Status ExpectLiteral(Value* out) {
    if (Peek().type == TokenType::kInt) {
      *out = Value::Int(Advance().int_value);
      return Status::OK();
    }
    if (Peek().type == TokenType::kString) {
      *out = Value::Text(Advance().text);
      return Status::OK();
    }
    return Error("expected literal");
  }

  Status ParseCreate(Statement* out) {
    CreateTableStmt stmt;
    Status s = ExpectKeyword("TABLE");
    if (!s.ok()) return s;
    s = ExpectIdentifier(&stmt.table);
    if (!s.ok()) return s;
    s = ExpectSymbol("(");
    if (!s.ok()) return s;
    int pk_count = 0;
    do {
      ColumnDef col;
      s = ExpectIdentifier(&col.name);
      if (!s.ok()) return s;
      if (AcceptKeyword("INT")) {
        col.type = ValueType::kInt;
      } else if (AcceptKeyword("TEXT")) {
        col.type = ValueType::kText;
      } else {
        return Error("expected INT or TEXT");
      }
      if (AcceptKeyword("PRIMARY")) {
        s = ExpectKeyword("KEY");
        if (!s.ok()) return s;
        col.is_pk = true;
        pk_count++;
      }
      for (const auto& existing : stmt.columns) {
        if (existing.name == col.name) {
          return Status::InvalidArgument("duplicate column " + col.name);
        }
      }
      stmt.columns.push_back(std::move(col));
    } while (AcceptSymbol(","));
    s = ExpectSymbol(")");
    if (!s.ok()) return s;
    if (pk_count != 1) {
      return Status::InvalidArgument("exactly one PRIMARY KEY column required");
    }
    *out = std::move(stmt);
    return Status::OK();
  }

  Status ParseDrop(Statement* out) {
    DropTableStmt stmt;
    Status s = ExpectKeyword("TABLE");
    if (!s.ok()) return s;
    s = ExpectIdentifier(&stmt.table);
    if (!s.ok()) return s;
    *out = std::move(stmt);
    return Status::OK();
  }

  Status ParseInsert(Statement* out) {
    InsertStmt stmt;
    Status s = ExpectKeyword("INTO");
    if (!s.ok()) return s;
    s = ExpectIdentifier(&stmt.table);
    if (!s.ok()) return s;
    if (AcceptSymbol("(")) {
      do {
        std::string col;
        s = ExpectIdentifier(&col);
        if (!s.ok()) return s;
        stmt.columns.push_back(std::move(col));
      } while (AcceptSymbol(","));
      s = ExpectSymbol(")");
      if (!s.ok()) return s;
    }
    s = ExpectKeyword("VALUES");
    if (!s.ok()) return s;
    do {
      s = ExpectSymbol("(");
      if (!s.ok()) return s;
      std::vector<Value> row;
      do {
        Value value;
        s = ExpectLiteral(&value);
        if (!s.ok()) return s;
        row.push_back(std::move(value));
      } while (AcceptSymbol(","));
      s = ExpectSymbol(")");
      if (!s.ok()) return s;
      stmt.rows.push_back(std::move(row));
    } while (AcceptSymbol(","));
    *out = std::move(stmt);
    return Status::OK();
  }

  Status ParseWhere(std::vector<Predicate>* where) {
    if (!AcceptKeyword("WHERE")) return Status::OK();
    do {
      Predicate pred;
      Status s = ExpectIdentifier(&pred.column);
      if (!s.ok()) return s;
      if (Peek().type != TokenType::kSymbol) return Error("expected comparison");
      std::string op = Advance().text;
      if (op == "=") {
        pred.op = CompareOp::kEq;
      } else if (op == "!=") {
        pred.op = CompareOp::kNe;
      } else if (op == "<") {
        pred.op = CompareOp::kLt;
      } else if (op == "<=") {
        pred.op = CompareOp::kLe;
      } else if (op == ">") {
        pred.op = CompareOp::kGt;
      } else if (op == ">=") {
        pred.op = CompareOp::kGe;
      } else {
        return Error("expected comparison operator");
      }
      s = ExpectLiteral(&pred.literal);
      if (!s.ok()) return s;
      where->push_back(std::move(pred));
    } while (AcceptKeyword("AND"));
    return Status::OK();
  }

  Status ParseSelect(Statement* out) {
    SelectStmt stmt;
    if (!AcceptSymbol("*")) {
      do {
        std::string col;
        Status s = ExpectIdentifier(&col);
        if (!s.ok()) return s;
        stmt.columns.push_back(std::move(col));
      } while (AcceptSymbol(","));
    }
    Status s = ExpectKeyword("FROM");
    if (!s.ok()) return s;
    s = ExpectIdentifier(&stmt.table);
    if (!s.ok()) return s;
    s = ParseWhere(&stmt.where);
    if (!s.ok()) return s;
    *out = std::move(stmt);
    return Status::OK();
  }

  Status ParseUpdate(Statement* out) {
    UpdateStmt stmt;
    Status s = ExpectIdentifier(&stmt.table);
    if (!s.ok()) return s;
    s = ExpectKeyword("SET");
    if (!s.ok()) return s;
    do {
      std::string col;
      s = ExpectIdentifier(&col);
      if (!s.ok()) return s;
      s = ExpectSymbol("=");
      if (!s.ok()) return s;
      Value value;
      s = ExpectLiteral(&value);
      if (!s.ok()) return s;
      stmt.sets.emplace_back(std::move(col), std::move(value));
    } while (AcceptSymbol(","));
    s = ParseWhere(&stmt.where);
    if (!s.ok()) return s;
    *out = std::move(stmt);
    return Status::OK();
  }

  Status ParseDelete(Statement* out) {
    DeleteStmt stmt;
    Status s = ExpectKeyword("FROM");
    if (!s.ok()) return s;
    s = ExpectIdentifier(&stmt.table);
    if (!s.ok()) return s;
    s = ParseWhere(&stmt.where);
    if (!s.ok()) return s;
    *out = std::move(stmt);
    return Status::OK();
  }

  std::vector<Token> tokens_;
  size_t pos_ = 0;
};

}  // namespace

Status Parse(const std::string& input, Statement* out) {
  std::vector<Token> tokens;
  Status s = Lex(input, &tokens);
  if (!s.ok()) return s;
  Parser parser(std::move(tokens));
  s = parser.ParseStatement(out);
  if (!s.ok()) return s;
  return parser.Finish();
}

}  // namespace flotilla::sql
