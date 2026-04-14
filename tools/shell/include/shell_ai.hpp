//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_ai.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_state.hpp"

namespace duckdb_shell {

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

//! Conversation state persists across .ai invocations within a shell session
struct AIConversationState {
	vector<AIMessage> messages;
	//! SQL queries that ran successfully (added to SQL history on AI mode exit)
	vector<string> successful_sql;
	bool httpfs_loaded = false;
};

//! Entry point for the .ai metadata command
MetadataResult RunAIMode(ShellState &state, const vector<string> &args);

} // namespace duckdb_shell
