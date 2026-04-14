//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_ai.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/shell_extension.hpp"

namespace duckdb_shell {

using duckdb::idx_t;
using duckdb::string;
using duckdb::unique_ptr;
using duckdb::vector;

//! A single message in the AI conversation
struct AIMessage {
	string role;         //! "user" or "assistant"
	string content_json; //! Raw JSON content (string literal or content block array)
};

//! Parsed content block from a Claude API response
struct AIContentBlock {
	string type;       //! "text" or "tool_use"
	string text;       //! Text content (for type=="text")
	string id;         //! Tool use ID (for type=="tool_use")
	string name;       //! Tool name (for type=="tool_use")
	string input_json; //! Raw JSON of tool input (for type=="tool_use")
};

//! Parsed Claude API response
struct AIResponse {
	string stop_reason;
	vector<AIContentBlock> content;
	int64_t input_tokens = 0;
	int64_t output_tokens = 0;
	string error_message;
};

//! Conversation state persists across .ask invocations within a shell session.
//! Inherits from ShellExtensionData so it can be stored via ShellContext.
struct AIConversationState : public duckdb::ShellExtensionData {
	vector<AIMessage> messages;
	//! SQL queries that ran successfully (added to SQL history on AI mode exit)
	vector<string> successful_sql;
	bool httpfs_loaded = false;
};

//! Entry point for the .ai metadata command
duckdb::ShellCommandResult RunAIMode(duckdb::ShellContext &context, const duckdb::vector<duckdb::string> &args);

//! Register the .ask shell command with the database config
void RegisterAIShellCommand(duckdb::DBConfig &config);

} // namespace duckdb_shell
