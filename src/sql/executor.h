#pragma once

#include <string>
#include <vector>

#include "client/client.h"
#include "common/status.h"
#include "sql/ast.h"

namespace flotilla::sql {

struct ResultSet {
  std::vector<std::string> columns;             // empty for write statements
  std::vector<std::vector<std::string>> rows;   // rendered values
  size_t affected = 0;                          // write statements
  std::string message;                          // e.g. "table created"
};

// Parses and executes one statement inside its own snapshot-isolation
// transaction. Conflict aborts surface as Conflict (retryable by the caller).
Status Execute(client::Client* client, const std::string& statement, ResultSet* out);

}  // namespace flotilla::sql
