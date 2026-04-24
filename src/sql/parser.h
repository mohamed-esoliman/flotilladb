#pragma once

#include <string>

#include "common/status.h"
#include "sql/ast.h"

namespace flotilla::sql {

// Parses exactly one statement (an optional trailing ';' is allowed).
Status Parse(const std::string& input, Statement* out);

}  // namespace flotilla::sql
