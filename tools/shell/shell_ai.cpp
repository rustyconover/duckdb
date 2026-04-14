//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_ai.cpp
//
// AI conversation mode for the DuckDB CLI shell.
// Uses the Claude Messages API with tool calling.
//
//===----------------------------------------------------------------------===//

#include "shell_ai.hpp"
#include "shell_renderer.hpp"

#include <cerrno>
#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "yyjson.hpp"

#ifdef HAVE_LINENOISE
#include "linenoise.h"
#include "history.hpp"
#include "terminal.hpp"
#endif

using namespace duckdb_yyjson; // NOLINT

namespace duckdb_shell {

using duckdb::ClientContext;
using duckdb::Connection;
using duckdb::const_data_ptr_cast;
using duckdb::DatabaseInstance;
using duckdb::HTTPHeaders;
using duckdb::HTTPResponse;
using duckdb::HTTPUtil;
using duckdb::PostRequestInfo;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

static constexpr int MAX_TOOL_ROUNDS = 100;
static constexpr int MAX_RESULT_ROWS_FOR_MODEL = 50;
static constexpr int API_TIMEOUT_SECONDS = 120;
static constexpr const char *DEFAULT_MODEL = "claude-sonnet-4-20250514";
static constexpr const char *API_URL = "https://api.anthropic.com/v1/messages";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static string GetEnvVar(const char *name) {
	const char *val = getenv(name);
	if (val && strlen(val) > 0) {
		return string(val);
	}
	return string();
}

//! Escape a string for use as a JSON string value (without surrounding quotes)
static string JSONEscapeString(const string &input) {
	string result;
	result.reserve(input.size() + 16);
	for (char c : input) {
		switch (c) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
				result += buf;
			} else {
				result += c;
			}
			break;
		}
	}
	return result;
}

// ---------------------------------------------------------------------------
// ANSI escape codes
// ---------------------------------------------------------------------------

static constexpr const char *ANSI_BOLD = "\x1b[1m";
static constexpr const char *ANSI_DIM = "\x1b[2m";
static constexpr const char *ANSI_ITALIC = "\x1b[3m";
static constexpr const char *ANSI_BOLD_OFF = "\x1b[22m";
static constexpr const char *ANSI_ITALIC_OFF = "\x1b[23m";
static constexpr const char *ANSI_CYAN = "\x1b[36m";
static constexpr const char *ANSI_YELLOW = "\x1b[33m";
static constexpr const char *ANSI_GREEN = "\x1b[32m";
static constexpr const char *ANSI_RESET = "\x1b[0m";

// ---------------------------------------------------------------------------
// Markdown to ANSI renderer
// ---------------------------------------------------------------------------

//! Get visible length of a string (excluding ANSI escape sequences)
static idx_t VisibleLength(const string &s) {
	idx_t len = 0;
	bool in_escape = false;
	for (char c : s) {
		if (in_escape) {
			if (c == 'm') {
				in_escape = false;
			}
		} else if (c == '\x1b') {
			in_escape = true;
		} else {
			len++;
		}
	}
	return len;
}

//! Apply inline markdown formatting: **bold**, *italic*, `code`
static string FormatInlineMarkdown(const string &line) {
	string result;
	result.reserve(line.size() + 64);
	idx_t i = 0;
	idx_t len = line.size();

	while (i < len) {
		// Inline code `...`
		if (line[i] == '`' && (i + 1 < len) && line[i + 1] != '`') {
			auto end = line.find('`', i + 1);
			if (end != string::npos) {
				result += ANSI_CYAN;
				result += line.substr(i + 1, end - i - 1);
				result += ANSI_RESET;
				i = end + 1;
				continue;
			}
		}
		// Bold **...**
		if (i + 1 < len && line[i] == '*' && line[i + 1] == '*') {
			auto end = line.find("**", i + 2);
			if (end != string::npos) {
				result += ANSI_BOLD;
				result += line.substr(i + 2, end - i - 2);
				result += ANSI_BOLD_OFF;
				i = end + 2;
				continue;
			}
		}
		// Italic *...*
		if (line[i] == '*' && (i == 0 || line[i - 1] != '*') && (i + 1 < len) && line[i + 1] != '*') {
			auto end = line.find('*', i + 1);
			if (end != string::npos && (end + 1 >= len || line[end + 1] != '*')) {
				result += ANSI_ITALIC;
				result += line.substr(i + 1, end - i - 1);
				result += ANSI_ITALIC_OFF;
				i = end + 1;
				continue;
			}
		}
		result += line[i];
		i++;
	}
	return result;
}

//! Word-wrap a formatted line at the given visible width
static string WrapText(const string &text, idx_t width, const string &indent = "") {
	if (VisibleLength(text) <= width) {
		return text;
	}

	string result;
	string current;
	idx_t current_len = 0;

	// Split on spaces
	idx_t i = 0;
	idx_t len = text.size();
	while (i < len) {
		// Collect a word (including any leading spaces)
		string word;
		while (i < len && text[i] == ' ') {
			word += text[i++];
		}
		while (i < len && text[i] != ' ') {
			if (text[i] == '\x1b') {
				// Include full ANSI escape
				while (i < len && text[i] != 'm') {
					word += text[i++];
				}
				if (i < len) {
					word += text[i++]; // the 'm'
				}
			} else {
				word += text[i++];
			}
		}

		idx_t word_vis_len = VisibleLength(word);
		if (current_len + word_vis_len > width && current_len > 0) {
			// Wrap
			// Trim trailing spaces from current line
			while (!current.empty() && current.back() == ' ') {
				current.pop_back();
			}
			result += current + "\n";
			// Start new line with indent, skip leading spaces in word
			string trimmed_word;
			idx_t j = 0;
			while (j < word.size() && word[j] == ' ') {
				j++;
			}
			trimmed_word = word.substr(j);
			current = indent + trimmed_word;
			current_len = indent.size() + VisibleLength(trimmed_word);
		} else {
			current += word;
			current_len += word_vis_len;
		}
	}
	if (!current.empty()) {
		while (!current.empty() && current.back() == ' ') {
			current.pop_back();
		}
		result += current;
	}
	return result;
}

//! Render a full markdown text block to ANSI-formatted terminal output
static string RenderMarkdown(const string &text, idx_t terminal_width) {
	idx_t wrap_width = terminal_width > 4 ? terminal_width - 2 : terminal_width;
	if (wrap_width > 100) {
		wrap_width = 100;
	}

	string result;
	bool in_code_block = false;

	// Split into lines
	idx_t start = 0;
	while (start <= text.size()) {
		idx_t end = text.find('\n', start);
		if (end == string::npos) {
			end = text.size();
		}
		string line = text.substr(start, end - start);
		start = end + 1;

		// Code block fence
		if (line.find("```") == 0 || (line.size() >= 3 && line.substr(0, 3) == "```")) {
			if (!in_code_block) {
				in_code_block = true;
				result += string(ANSI_DIM);
			} else {
				in_code_block = false;
				result += string(ANSI_RESET);
			}
			result += "\n";
			continue;
		}

		// Inside code block — pass through dimmed, no wrapping
		if (in_code_block) {
			result += "  " + line + "\n";
			continue;
		}

		// Empty line
		if (line.empty()) {
			result += "\n";
			continue;
		}

		// Heading: # ## ### at line start
		if (line[0] == '#') {
			idx_t level = 0;
			while (level < line.size() && line[level] == '#') {
				level++;
			}
			if (level <= 4 && level < line.size() && line[level] == ' ') {
				string heading_text = line.substr(level + 1);
				result += string(ANSI_BOLD) + string(ANSI_GREEN) + heading_text + string(ANSI_RESET) + "\n";
				continue;
			}
		}

		// Table row: starts with |
		if (line[0] == '|') {
			// Check if it's a separator row (|---|---|)
			bool is_separator = true;
			for (char c : line) {
				if (c != '|' && c != '-' && c != ' ' && c != ':') {
					is_separator = false;
					break;
				}
			}
			if (is_separator) {
				result += string(ANSI_DIM) + line + string(ANSI_RESET) + "\n";
			} else {
				result += FormatInlineMarkdown(line) + "\n";
			}
			continue;
		}

		// Bullet: - or * at line start
		idx_t indent_len = 0;
		while (indent_len < line.size() && line[indent_len] == ' ') {
			indent_len++;
		}
		if (indent_len < line.size() && (line[indent_len] == '-' || line[indent_len] == '*') &&
		    indent_len + 1 < line.size() && line[indent_len + 1] == ' ') {
			string indent(indent_len, ' ');
			string content = FormatInlineMarkdown(line.substr(indent_len + 2));
			string prefix = indent + string(ANSI_YELLOW) + "\xe2\x80\xa2" + string(ANSI_RESET) + " ";
			result += WrapText(prefix + content, wrap_width, indent + "  ") + "\n";
			continue;
		}

		// Numbered list: 1. 2. etc
		if (indent_len < line.size() && line[indent_len] >= '0' && line[indent_len] <= '9') {
			auto dot_pos = line.find(". ", indent_len);
			if (dot_pos != string::npos && dot_pos - indent_len <= 3) {
				string indent(indent_len, ' ');
				string num = line.substr(indent_len, dot_pos - indent_len);
				string content = FormatInlineMarkdown(line.substr(dot_pos + 2));
				string prefix = indent + string(ANSI_YELLOW) + num + "." + string(ANSI_RESET) + " ";
				result += WrapText(prefix + content, wrap_width, indent + "   ") + "\n";
				continue;
			}
		}

		// Regular line — apply inline formatting and wrap
		result += WrapText(FormatInlineMarkdown(line), wrap_width) + "\n";
	}

	// Close unclosed code block
	if (in_code_block) {
		result += string(ANSI_RESET);
	}

	return result;
}

// ---------------------------------------------------------------------------
// Animated spinner for blocking operations
// ---------------------------------------------------------------------------

static const char *const SPINNER_FRAMES[] = {"\xe2\xa0\x8b", "\xe2\xa0\x99", "\xe2\xa0\xb9", "\xe2\xa0\xb8",
                                              "\xe2\xa0\xbc", "\xe2\xa0\xb4", "\xe2\xa0\xa6", "\xe2\xa0\xa7",
                                              "\xe2\xa0\x87", "\xe2\xa0\x8f"};
static constexpr int SPINNER_FRAME_COUNT = 10;

struct Spinner {
	duckdb::atomic<bool> running {false};
	unique_ptr<duckdb::thread> spin_thread;
	string label;
	string color;
	//! Optional pointer to ShellState for setting seenInterrupt on Escape
	ShellState *interrupt_state = nullptr;

	void Start(const string &new_label, const string &new_color = "\x1b[1;35m") {
		Stop();
		label = new_label;
		color = new_color;
		running = true;
#if defined(HAVE_LINENOISE) && !defined(_WIN32)
		// Enable raw mode so we can catch Escape/Ctrl+C directly from stdin
		// (otherwise Ctrl+C goes to the signal handler and we can't interrupt immediately)
		if (interrupt_state) {
			duckdb::Terminal::EnableRawMode();
		}
#endif
		spin_thread = make_uniq<duckdb::thread>([this]() {
			int frame = 0;
			while (running) {
				fprintf(stdout, "\r%s%s %s\x1b[0m\x1b[K", color.c_str(),
				        SPINNER_FRAMES[frame % SPINNER_FRAME_COUNT], label.c_str());
				fflush(stdout);
				frame++;
#if defined(HAVE_LINENOISE) && !defined(_WIN32)
				// Poll for Escape or Ctrl+C to allow interrupting during API calls
				if (interrupt_state && duckdb::Terminal::HasMoreData(STDIN_FILENO, 0)) {
					char c;
					if (read(STDIN_FILENO, &c, 1) == 1) {
						if (c == '\x1b' || c == '\x03') {
							interrupt_state->seenInterrupt = 1;
							running = false;
							return;
						}
					}
				}
#endif
				duckdb::ThreadUtil::SleepMs(100);
			}
		});
	}

	void Stop() {
		running = false;
		if (spin_thread) {
			if (spin_thread->joinable()) {
				spin_thread->join();
			}
			spin_thread.reset();
#if defined(HAVE_LINENOISE) && !defined(_WIN32)
			if (interrupt_state) {
				duckdb::Terminal::DisableRawMode();
			}
#endif
			fprintf(stdout, "\r\x1b[K");
			fflush(stdout);
		}
	}

	~Spinner() {
		Stop();
	}
};

// ---------------------------------------------------------------------------
// RAII wrapper for yyjson_doc (immutable read document)
// ---------------------------------------------------------------------------

struct YYJsonDocGuard {
	yyjson_doc *doc = nullptr;
	explicit YYJsonDocGuard(yyjson_doc *d) : doc(d) {
	}
	~YYJsonDocGuard() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
	YYJsonDocGuard(const YYJsonDocGuard &) = delete;
	YYJsonDocGuard &operator=(const YYJsonDocGuard &) = delete;
};

struct YYJsonMutDocGuard {
	yyjson_mut_doc *doc = nullptr;
	explicit YYJsonMutDocGuard(yyjson_mut_doc *d) : doc(d) {
	}
	~YYJsonMutDocGuard() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}
	YYJsonMutDocGuard(const YYJsonMutDocGuard &) = delete;
	YYJsonMutDocGuard &operator=(const YYJsonMutDocGuard &) = delete;
};

// ---------------------------------------------------------------------------
// yyjson helper: get string from object key safely
// ---------------------------------------------------------------------------

static const char *YYGetStr(yyjson_val *obj, const char *key) {
	auto *val = yyjson_obj_get(obj, key);
	if (val && yyjson_is_str(val)) {
		return yyjson_get_str(val);
	}
	return nullptr;
}

static int64_t YYGetInt(yyjson_val *obj, const char *key) {
	auto *val = yyjson_obj_get(obj, key);
	if (val && yyjson_is_int(val)) {
		return yyjson_get_int(val);
	}
	return 0;
}

// ---------------------------------------------------------------------------
// Ensure httpfs is loaded
// ---------------------------------------------------------------------------

static bool EnsureHTTPFS(ShellState &state, AIConversationState &conv) {
	if (conv.httpfs_loaded) {
		return true;
	}
	// Try loading httpfs (may already be loaded)
	auto result = state.conn->Query("LOAD httpfs");
	if (result->HasError()) {
		// Try installing first
		auto install_result = state.conn->Query("INSTALL httpfs");
		if (install_result->HasError()) {
			state.PrintF(PrintOutput::STDERR, "Error: httpfs extension required for .ask but could not be loaded: %s\n",
			             install_result->GetError().c_str());
			return false;
		}
		result = state.conn->Query("LOAD httpfs");
		if (result->HasError()) {
			state.PrintF(PrintOutput::STDERR, "Error: failed to load httpfs extension: %s\n",
			             result->GetError().c_str());
			return false;
		}
	}
	conv.httpfs_loaded = true;
	return true;
}

// ---------------------------------------------------------------------------
// System prompt builder
// ---------------------------------------------------------------------------

static string BuildSystemPrompt(ShellState &state) {
	string prompt;
	prompt.reserve(4096);

	// Query database metadata
	string catalog_info;
	{
		auto result = state.conn->Query(
		    "SELECT database_name, schema_name, table_name, table_type, "
		    "COALESCE(comment, '') as comment "
		    "FROM information_schema.tables "
		    "ORDER BY database_name, schema_name, table_name");
		if (!result->HasError()) {
			auto &mat = result->Cast<duckdb::MaterializedQueryResult>();
			for (idx_t r = 0; r < mat.RowCount(); r++) {
				auto db_name = mat.GetValue(0, r).ToString();
				auto schema_name = mat.GetValue(1, r).ToString();
				auto table_name = mat.GetValue(2, r).ToString();
				auto table_type = mat.GetValue(3, r).ToString();
				auto comment = mat.GetValue(4, r).ToString();
				string type_label = (table_type == "VIEW") ? "view" : "table";
				catalog_info += "* `" + db_name + "." + schema_name + "." + table_name + "` (" + type_label + ")";
				if (!comment.empty()) {
					catalog_info += " -- " + comment;
				}
				catalog_info += "\n";
			}
		}
	}

	// Query loaded extensions
	string extensions_info;
	{
		auto result = state.conn->Query(
		    "SELECT extension_name FROM duckdb_extensions() "
		    "WHERE installed AND loaded ORDER BY extension_name");
		if (!result->HasError()) {
			auto &mat = result->Cast<duckdb::MaterializedQueryResult>();
			for (idx_t r = 0; r < mat.RowCount(); r++) {
				if (r > 0) {
					extensions_info += ", ";
				}
				extensions_info += mat.GetValue(0, r).ToString();
			}
		}
	}

	// Get first catalog/schema/table for examples
	string example_catalog = "memory";
	string example_schema = "main";
	string example_table = "my_table";
	{
		auto result = state.conn->Query(
		    "SELECT database_name, schema_name, table_name "
		    "FROM information_schema.tables LIMIT 1");
		if (!result->HasError()) {
			auto &mat = result->Cast<duckdb::MaterializedQueryResult>();
			if (mat.RowCount() > 0) {
				example_catalog = mat.GetValue(0, 0).ToString();
				example_schema = mat.GetValue(1, 0).ToString();
				example_table = mat.GetValue(2, 0).ToString();
			}
		}
	}
	string ex_full = example_catalog + "." + example_schema + "." + example_table;

	prompt += "You are a data analyst assistant connected to a DuckDB database.\n\n";

	prompt += "## Tools\n";
	prompt += "* **describe_table** -- Get column names, types, and descriptions for a table.\n";
	prompt += "* **run_sql** -- Execute a DuckDB SQL query.\n";
	prompt += "* **list_tables** -- List all schemas, tables, and views in the database.\n";
	prompt += "* **ask_user** -- Ask the user to choose between specific options.\n";
	prompt += "* **read_file** -- Read a file's contents. No confirmation needed.\n";
	prompt += "* **write_file** -- Write content to a file on disk (user confirms before write).\n";
	prompt += "* **edit_file** -- Replace a specific string in a file (user confirms). Use read_file first.\n";
	prompt += "* **bash** -- Execute a bash command (user confirms before execution).\n";
	prompt += "* **glob** -- Find files matching a pattern. No confirmation needed.\n";
	prompt += "* **grep** -- Search file contents for a pattern. No confirmation needed.\n\n";

	prompt += "## Rules\n\n";

	prompt += "### Before writing any query\n";
	prompt += "You MUST call describe_table for every table you plan to reference. ";
	prompt += "Do not guess or infer column names from the table description -- they are not predictable.\n\n";

	prompt += "### Query planning\n";
	prompt += "For multi-step or ambiguous questions, outline your analysis plan first: ";
	prompt += "which tables, what joins, what aggregations. Then execute step by step using CTEs, ";
	prompt += "views, or temporary tables to break complex work into stages.\n\n";

	prompt += "### SQL style\n";
	prompt += "* Always use fully qualified three-part table references: `catalog.schema.table` ";
	prompt += "(e.g., `" + ex_full + "`). Never use bare table names or two-part names.\n";
	prompt += "* Use short aliases to keep queries readable: `FROM " + ex_full + " t`.\n";
	prompt += "* Always JOIN tables in SQL rather than combining results from separate queries in prose.\n";
	prompt += "* All arithmetic, aggregation, and numeric comparison MUST happen in SQL via run_sql. ";
	prompt += "Never do math in your head.\n";
	prompt += "* For final results, select only the columns relevant to the user's question -- avoid `SELECT *`.\n";
	prompt += "* Prefer CTEs (`WITH` clauses) for intermediate steps.\n\n";

	prompt += "### Disambiguation\n";
	prompt += "Use ask_user when the user's question is ambiguous -- e.g., which item, which metric, ";
	prompt += "which time period. Don't assume.\n\n";

	prompt += "### File editing\n";
	prompt += "* When modifying an existing file, ALWAYS use edit_file instead of rewriting the entire file with write_file.\n";
	prompt += "* Use read_file first to see the current content, then use edit_file to make targeted changes.\n";
	prompt += "* Only use write_file for creating new files that don't exist yet.\n";
	prompt += "* For multiple changes to the same file, make multiple edit_file calls.\n\n";

	prompt += "### Error recovery\n";
	prompt += "If a query fails, look up every function used: ";
	prompt += "`SELECT function_name, parameters, description FROM duckdb_functions() WHERE function_name = 'name'`. ";
	prompt += "If the same error occurs twice, explain the issue to the user and ask for guidance.\n\n";

	prompt += "### Output\n";
	prompt += "* The run_sql tool automatically displays query results to the user in a formatted table. ";
	prompt += "You do not need to reproduce or reformat the data in your response -- just explain the findings.\n";
	prompt += "* For wide results (> 6 columns): select only the relevant columns.\n";
	prompt += "* Always explain your findings in plain language after presenting data.\n\n";

	prompt += "### Never do this\n";
	prompt += "* Never use `SELECT *` in result queries.\n";
	prompt += "* Never perform arithmetic outside SQL.\n";
	prompt += "* Never combine results from separate queries in prose -- use JOINs or CTEs.\n";
	prompt += "* Never use two-part or bare table names -- always use `catalog.schema.table`.\n";
	prompt += "* Never attempt to LOAD or INSTALL extensions.\n\n";

	prompt += "## Object naming: catalog -> schema -> table\n\n";
	prompt += "DuckDB uses a three-level namespace: `catalog.schema.table`.\n\n";
	prompt += "| Level | What it is | Example |\n";
	prompt += "|-------|-----------|--------|\n";
	prompt += "| Catalog | A database or attached data source | `" + example_catalog + "` |\n";
	prompt += "| Schema | A grouping of related tables within a catalog | `" + example_catalog + "." + example_schema +
	          "` |\n";
	prompt += "| Table | A single table or view | `" + ex_full + "` |\n\n";
	prompt += "Always use fully qualified three-part names.\n\n";

	if (!extensions_info.empty()) {
		prompt += "## Loaded extensions\n";
		prompt += extensions_info + "\n\n";
	}

	prompt += "## Available tables and views\n\n";
	prompt += "This list is current as of the start of this conversation. You do not need to call list_tables ";
	prompt += "to verify it -- only call list_tables if you have created or dropped tables during the conversation.\n\n";
	if (!catalog_info.empty()) {
		prompt += catalog_info;
		prompt += "\n";
	} else {
		prompt += "No tables or views exist yet.\n\n";
	}

	return prompt;
}

// ---------------------------------------------------------------------------
// Tool definitions (built into JSON request)
// ---------------------------------------------------------------------------

static void AddToolDefinitions(yyjson_mut_doc *doc, yyjson_mut_val *tools_arr) {
	// run_sql
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "run_sql");
		yyjson_mut_obj_add_str(doc, tool, "description",
		                       "Execute a single DuckDB SQL statement against the connected database. Returns results "
		                       "as JSON with columns, types, rows, and total row count. Only one statement per call "
		                       "— use multiple tool calls for multiple statements.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);
		auto *sql_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, sql_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, sql_prop, "description", "The SQL query to execute");
		yyjson_mut_obj_add_val(doc, props, "sql", sql_prop);
		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "sql");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// list_tables
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "list_tables");
		yyjson_mut_obj_add_str(doc, tool, "description",
		                       "List all schemas, tables, and views in the database with their comments. "
		                       "This is the authoritative source for what exists in the database. "
		                       "If it returns an empty list, there are no tables -- do not try alternative queries.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// describe_table
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "describe_table");
		yyjson_mut_obj_add_str(
		    doc, tool, "description",
		    "Get detailed information for a table or view: columns (name, type, nullable, default, comment).");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *catalog_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, catalog_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, catalog_prop, "description",
		                       "Catalog name. Defaults to the current catalog if omitted.");
		yyjson_mut_obj_add_val(doc, props, "catalog", catalog_prop);

		auto *schema_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, schema_prop, "description", "Schema name (e.g., 'main')");
		yyjson_mut_obj_add_val(doc, props, "schema", schema_prop);

		auto *table_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, table_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, table_prop, "description", "Table or view name");
		yyjson_mut_obj_add_val(doc, props, "table", table_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "schema");
		yyjson_mut_arr_add_str(doc, required, "table");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// ask_user
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "ask_user");
		yyjson_mut_obj_add_str(
		    doc, tool, "description",
		    "Present a question with numbered options to the user and wait for their selection.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *question_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, question_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, question_prop, "description", "The question to ask");
		yyjson_mut_obj_add_val(doc, props, "question", question_prop);

		auto *options_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, options_prop, "type", "array");
		auto *items = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, items, "type", "string");
		yyjson_mut_obj_add_val(doc, options_prop, "items", items);
		yyjson_mut_obj_add_str(doc, options_prop, "description", "List of options for the user to choose from");
		yyjson_mut_obj_add_val(doc, props, "options", options_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "question");
		yyjson_mut_arr_add_str(doc, required, "options");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// write_file
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "write_file");
		yyjson_mut_obj_add_str(doc, tool, "description",
		                       "Write content to a file on disk. The user will be asked to confirm before the file "
		                       "is written. Use this to create reports, scripts, data exports, or any other files.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *path_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, path_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, path_prop, "description", "File path to write to (absolute or relative)");
		yyjson_mut_obj_add_val(doc, props, "path", path_prop);

		auto *content_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, content_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, content_prop, "description", "Content to write to the file");
		yyjson_mut_obj_add_val(doc, props, "content", content_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "path");
		yyjson_mut_arr_add_str(doc, required, "content");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// bash
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "bash");
		yyjson_mut_obj_add_str(
		    doc, tool, "description",
		    "Execute a bash command and return stdout+stderr. The user confirms before execution. "
		    "Use for: compiling files (typst, latex), running scripts, file manipulation (sed, awk, grep), "
		    "installing tools, opening files, or any system task.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *cmd_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, cmd_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, cmd_prop, "description", "The shell command to execute");
		yyjson_mut_obj_add_val(doc, props, "command", cmd_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "command");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// read_file
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "read_file");
		yyjson_mut_obj_add_str(doc, tool, "description",
		                       "Read the contents of a file from disk. No confirmation required. "
		                       "Returns the file content as text. Use this to inspect files before editing.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);
		auto *path_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, path_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, path_prop, "description", "File path to read (absolute or relative)");
		yyjson_mut_obj_add_val(doc, props, "path", path_prop);
		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "path");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// edit_file
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "edit_file");
		yyjson_mut_obj_add_str(
		    doc, tool, "description",
		    "Edit a file by replacing an exact string match with new content. The user confirms before writing. "
		    "Use read_file first to see the current content, then use this to make targeted changes.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *path_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, path_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, path_prop, "description", "File path to edit");
		yyjson_mut_obj_add_val(doc, props, "path", path_prop);

		auto *old_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, old_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, old_prop, "description", "Exact string to find and replace (must match exactly)");
		yyjson_mut_obj_add_val(doc, props, "old_string", old_prop);

		auto *new_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, new_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, new_prop, "description", "Replacement string");
		yyjson_mut_obj_add_val(doc, props, "new_string", new_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "path");
		yyjson_mut_arr_add_str(doc, required, "old_string");
		yyjson_mut_arr_add_str(doc, required, "new_string");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// glob
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "glob");
		yyjson_mut_obj_add_str(doc, tool, "description",
		                       "Find files matching a glob pattern. No confirmation required. "
		                       "Returns a list of matching file paths. Use ** for recursive matching.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *pattern_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, pattern_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, pattern_prop, "description",
		                       "Glob pattern (e.g. '*.csv', 'data/**/*.parquet', 'src/*.typ')");
		yyjson_mut_obj_add_val(doc, props, "pattern", pattern_prop);

		auto *path_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, path_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, path_prop, "description", "Directory to search in (default: current directory)");
		yyjson_mut_obj_add_val(doc, props, "path", path_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "pattern");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}

	// grep
	{
		auto *tool = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, tool, "name", "grep");
		yyjson_mut_obj_add_str(doc, tool, "description",
		                       "Search file contents for a pattern. No confirmation required. "
		                       "Returns matching lines with file names and line numbers.");
		auto *schema = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, schema, "type", "object");
		auto *props = yyjson_mut_obj(doc);

		auto *pattern_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, pattern_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, pattern_prop, "description", "Search pattern (regular expression)");
		yyjson_mut_obj_add_val(doc, props, "pattern", pattern_prop);

		auto *path_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, path_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, path_prop, "description",
		                       "File or directory to search (default: current directory)");
		yyjson_mut_obj_add_val(doc, props, "path", path_prop);

		auto *include_prop = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, include_prop, "type", "string");
		yyjson_mut_obj_add_str(doc, include_prop, "description",
		                       "File glob to filter (e.g. '*.typ', '*.sql'). Optional.");
		yyjson_mut_obj_add_val(doc, props, "include", include_prop);

		yyjson_mut_obj_add_val(doc, schema, "properties", props);
		auto *required = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_str(doc, required, "pattern");
		yyjson_mut_obj_add_val(doc, schema, "required", required);
		yyjson_mut_obj_add_val(doc, tool, "input_schema", schema);
		yyjson_mut_arr_add_val(tools_arr, tool);
	}
}

// ---------------------------------------------------------------------------
// Build JSON request body
// ---------------------------------------------------------------------------

static string BuildRequestJSON(const vector<AIMessage> &messages, const string &system_prompt, const string &model) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard doc_guard(doc);

	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// model
	yyjson_mut_obj_add_str(doc, root, "model", model.c_str());
	// max_tokens
	yyjson_mut_obj_add_int(doc, root, "max_tokens", 16384);
	// stream: false
	yyjson_mut_obj_add_bool(doc, root, "stream", false);

	// system prompt (with cache_control for prompt caching)
	auto *system_arr = yyjson_mut_arr(doc);
	auto *system_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_str(doc, system_obj, "type", "text");
	yyjson_mut_obj_add_str(doc, system_obj, "text", system_prompt.c_str());
	auto *sys_cache = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_str(doc, sys_cache, "type", "ephemeral");
	yyjson_mut_obj_add_val(doc, system_obj, "cache_control", sys_cache);
	yyjson_mut_arr_add_val(system_arr, system_obj);
	yyjson_mut_obj_add_val(doc, root, "system", system_arr);

	// tools
	auto *tools_arr = yyjson_mut_arr(doc);
	AddToolDefinitions(doc, tools_arr);
	yyjson_mut_obj_add_val(doc, root, "tools", tools_arr);

	// messages — use strcpy variants to copy all strings into the yyjson document
	auto *msgs_arr = yyjson_mut_arr(doc);
	for (auto &msg : messages) {
		auto *msg_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, msg_obj, "role", msg.role.c_str());

		// Parse the stored JSON content and copy it into this document
		auto *content_doc = yyjson_read(msg.content_json.c_str(), msg.content_json.size(), 0);
		if (content_doc) {
			YYJsonDocGuard content_guard(content_doc);
			auto *content_root = yyjson_doc_get_root(content_doc);
			if (content_root) {
				auto *content_copy = yyjson_val_mut_copy(doc, content_root);
				if (content_copy) {
					yyjson_mut_obj_add_val(doc, msg_obj, "content", content_copy);
				} else {
					// Copy failed (OOM?) — use empty string
					yyjson_mut_obj_add_str(doc, msg_obj, "content", "");
				}
			}
		} else {
			// Fallback: treat as plain string
			yyjson_mut_obj_add_strcpy(doc, msg_obj, "content", msg.content_json.c_str());
		}
		yyjson_mut_arr_add_val(msgs_arr, msg_obj);
	}
	yyjson_mut_obj_add_val(doc, root, "messages", msgs_arr);

	// Serialize
	size_t len = 0;
	auto *json_str = yyjson_mut_write(doc, 0, &len);
	if (!json_str) {
		return "{}";
	}
	string result(json_str, len);
	free(json_str);
	return result;
}

// ---------------------------------------------------------------------------
// Parse Claude API response
// ---------------------------------------------------------------------------

static AIResponse ParseResponse(const string &response_body) {
	AIResponse result;

	auto *doc = yyjson_read(response_body.c_str(), response_body.size(), 0);
	if (!doc) {
		result.error_message = "Failed to parse API response JSON";
		return result;
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	if (!root) {
		result.error_message = "Empty API response";
		return result;
	}

	// Check for API error
	auto *error_obj = yyjson_obj_get(root, "error");
	if (error_obj) {
		auto *msg = yyjson_obj_get(error_obj, "message");
		if (msg && yyjson_is_str(msg)) {
			result.error_message = yyjson_get_str(msg);
		} else {
			result.error_message = "Unknown API error";
		}
		return result;
	}

	// stop_reason
	auto *stop_reason = YYGetStr(root, "stop_reason");
	if (stop_reason) {
		result.stop_reason = stop_reason;
	}

	// usage
	auto *usage = yyjson_obj_get(root, "usage");
	if (usage) {
		result.input_tokens = YYGetInt(usage, "input_tokens");
		result.output_tokens = YYGetInt(usage, "output_tokens");
	}

	// content array
	auto *content = yyjson_obj_get(root, "content");
	if (content && yyjson_is_arr(content)) {
		size_t idx2, max2;
		yyjson_val *block;
		yyjson_arr_foreach(content, idx2, max2, block) {
			AIContentBlock cb;
			auto *type_val = YYGetStr(block, "type");
			if (!type_val) {
				continue;
			}
			cb.type = type_val;

			if (cb.type == "text") {
				auto *text_val = YYGetStr(block, "text");
				if (text_val) {
					cb.text = text_val;
				}
			} else if (cb.type == "tool_use") {
				auto *id_val = YYGetStr(block, "id");
				if (id_val) {
					cb.id = id_val;
				}
				auto *name_val = YYGetStr(block, "name");
				if (name_val) {
					cb.name = name_val;
				}
				// Serialize the input object back to JSON string
				auto *input_val = yyjson_obj_get(block, "input");
				if (input_val) {
					size_t input_len = 0;
					auto *input_str = yyjson_val_write(input_val, 0, &input_len);
					if (input_str) {
						cb.input_json = string(input_str, input_len);
						free(input_str);
					}
				}
			}
			result.content.push_back(std::move(cb));
		}
	}

	return result;
}

// ---------------------------------------------------------------------------
// Serialize content blocks back to JSON for message storage
// ---------------------------------------------------------------------------

static string SerializeContentBlocks(const vector<AIContentBlock> &blocks) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard doc_guard(doc);

	auto *arr = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, arr);

	for (auto &block : blocks) {
		auto *obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, obj, "type", block.type.c_str());

		if (block.type == "text") {
			yyjson_mut_obj_add_strcpy(doc, obj, "text", block.text.c_str());
		} else if (block.type == "tool_use") {
			yyjson_mut_obj_add_strcpy(doc, obj, "id", block.id.c_str());
			yyjson_mut_obj_add_strcpy(doc, obj, "name", block.name.c_str());
			// Parse input_json back into object
			auto *input_doc = yyjson_read(block.input_json.c_str(), block.input_json.size(), 0);
			if (input_doc) {
				YYJsonDocGuard input_guard(input_doc);
				auto *input_root = yyjson_doc_get_root(input_doc);
				auto *input_copy = yyjson_val_mut_copy(doc, input_root);
				yyjson_mut_obj_add_val(doc, obj, "input", input_copy);
			} else {
				auto *empty_obj = yyjson_mut_obj(doc);
				yyjson_mut_obj_add_val(doc, obj, "input", empty_obj);
			}
		}
		yyjson_mut_arr_add_val(arr, obj);
	}

	size_t len = 0;
	auto *json_str = yyjson_mut_write(doc, 0, &len);
	if (!json_str) {
		return "[]";
	}
	string result(json_str, len);
	free(json_str);
	return result;
}

// ---------------------------------------------------------------------------
// Build tool results JSON for sending back as user message
// ---------------------------------------------------------------------------

struct ToolResult {
	string tool_use_id;
	string content;
	bool is_error = false;
};

static string SerializeToolResults(const vector<ToolResult> &results) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard doc_guard(doc);

	auto *arr = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, arr);

	for (auto &tr : results) {
		auto *obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, obj, "type", "tool_result");
		yyjson_mut_obj_add_strcpy(doc, obj, "tool_use_id", tr.tool_use_id.c_str());
		yyjson_mut_obj_add_strcpy(doc, obj, "content", tr.content.c_str());
		if (tr.is_error) {
			yyjson_mut_obj_add_bool(doc, obj, "is_error", true);
		}
		yyjson_mut_arr_add_val(arr, obj);
	}

	size_t len = 0;
	auto *json_str = yyjson_mut_write(doc, 0, &len);
	if (!json_str) {
		return "[]";
	}
	string result(json_str, len);
	free(json_str);
	return result;
}

// ---------------------------------------------------------------------------
// Format a MaterializedQueryResult as JSON for the model
// ---------------------------------------------------------------------------

static string FormatResultAsJSON(duckdb::MaterializedQueryResult &result, idx_t max_rows = MAX_RESULT_ROWS_FOR_MODEL) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard doc_guard(doc);

	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	auto &types = result.types;
	auto &names = result.names;

	// columns (use strcpy variants — yyjson_mut_*_add_str only stores pointers, doesn't copy)
	auto *cols_arr = yyjson_mut_arr(doc);
	for (auto &name : names) {
		yyjson_mut_arr_add_strcpy(doc, cols_arr, name.c_str());
	}
	yyjson_mut_obj_add_val(doc, root, "columns", cols_arr);

	// types
	auto *types_arr = yyjson_mut_arr(doc);
	for (auto &type : types) {
		auto type_str = type.ToString();
		yyjson_mut_arr_add_strcpy(doc, types_arr, type_str.c_str());
	}
	yyjson_mut_obj_add_val(doc, root, "types", types_arr);

	// row_count
	auto total_rows = result.RowCount();
	yyjson_mut_obj_add_int(doc, root, "row_count", static_cast<int64_t>(total_rows));

	// rows (capped)
	auto show_rows = total_rows < max_rows ? total_rows : max_rows;
	auto *rows_arr = yyjson_mut_arr(doc);
	for (idx_t r = 0; r < show_rows; r++) {
		auto *row_obj = yyjson_mut_obj(doc);
		for (idx_t c = 0; c < names.size(); c++) {
			auto val = result.GetValue(c, r);
			if (val.IsNull()) {
				yyjson_mut_obj_add_null(doc, row_obj, names[c].c_str());
			} else {
				auto str_val = val.ToString();
				yyjson_mut_obj_add_strncpy(doc, row_obj, names[c].c_str(), str_val.c_str(), str_val.size());
			}
		}
		yyjson_mut_arr_add_val(rows_arr, row_obj);
	}
	yyjson_mut_obj_add_val(doc, root, "rows", rows_arr);
	yyjson_mut_obj_add_int(doc, root, "showing", static_cast<int64_t>(show_rows));

	size_t len = 0;
	auto *json_str = yyjson_mut_write(doc, 0, &len);
	if (!json_str) {
		return "{\"error\": \"Failed to serialize result\"}";
	}
	string json_result(json_str, len);
	free(json_str);
	return json_result;
}

// ---------------------------------------------------------------------------
// Tool executors
// ---------------------------------------------------------------------------

static string ExecuteRunSQLInternal(ShellState &state, AIConversationState &conv, const string &input_json) {
	// Parse SQL from input
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *sql_val = YYGetStr(root, "sql");
	if (!sql_val) {
		return "{\"error\": \"Missing 'sql' parameter\"}";
	}
	string sql(sql_val);

	// Print the SQL to the user with syntax highlighting
	// (temporarily re-enable highlighting since AI mode disables it for input)
	string highlighted_sql = sql;
	bool was_enabled = state.highlighting_enabled;
	state.highlighting_enabled = true;
	state.HighlightSQL(highlighted_sql);
	state.highlighting_enabled = was_enabled;
	state.Print("\n\x1b[1;36mRunning SQL:\x1b[0m\n");
	state.PrintF("%s\n\n", highlighted_sql.c_str());

	// Execute using the shell's full execution path (supports progress bar + rendering)
	auto statements = state.conn->ExtractStatements(sql);
	if (statements.empty()) {
		return "{\"error\": \"Empty SQL statement\"}";
	}

	// Execute the first statement via SendQuery with materialization
	duckdb::QueryParameters params;
	params.output_type = duckdb::QueryResultOutputType::FORCE_MATERIALIZED;
	auto result = state.conn->SendQuery(std::move(statements[0]), params);

	if (result->HasError()) {
		string err = result->GetError();
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}

	// Record successful SQL for later addition to SQL history
	// Strip SQL line comments (-- ...) before adding to history,
	// because history collapses newlines and a leading -- would comment out the entire statement
	string history_sql;
	idx_t pos = 0;
	while (pos < sql.size()) {
		// Check for -- line comment
		if (pos + 1 < sql.size() && sql[pos] == '-' && sql[pos + 1] == '-') {
			// Skip to end of line
			while (pos < sql.size() && sql[pos] != '\n') {
				pos++;
			}
			if (pos < sql.size()) {
				pos++; // skip the newline
			}
			continue;
		}
		// Check for string literals (don't strip -- inside strings)
		if (sql[pos] == '\'') {
			history_sql += sql[pos++];
			while (pos < sql.size() && sql[pos] != '\'') {
				history_sql += sql[pos++];
			}
			if (pos < sql.size()) {
				history_sql += sql[pos++]; // closing quote
			}
			continue;
		}
		history_sql += sql[pos++];
	}
	StringUtil::Trim(history_sql);
	if (!history_sql.empty()) {
		conv.successful_sql.push_back(history_sql);
	}

	// Render to terminal for the user
	if (result->properties.return_type == duckdb::StatementReturnType::QUERY_RESULT) {
		auto renderer = state.GetRenderer();
		state.RenderQueryResult(*renderer, *result);

		// Format as JSON for the model
		auto &mat_result = result->Cast<duckdb::MaterializedQueryResult>();
		return FormatResultAsJSON(mat_result);
	}

	// Non-result query (DDL, etc.)
	return "{\"ok\": true, \"message\": \"Query executed successfully\"}";
}

static string ExecuteRunSQL(ShellState &state, AIConversationState &conv, const string &input_json) {
	try {
		return ExecuteRunSQLInternal(state, conv, input_json);
	} catch (std::exception &e) {
		string err = e.what();
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}
}

static string ExecuteListTables(ShellState &state) {
	auto result = state.conn->Query(
	    "SELECT database_name, schema_name, table_name, table_type, "
	    "COALESCE(comment, '') as comment "
	    "FROM information_schema.tables "
	    "ORDER BY database_name, schema_name, table_name");

	if (result->HasError()) {
		return "{\"error\": \"" + JSONEscapeString(result->GetError()) + "\"}";
	}

	auto &mat_result = result->Cast<duckdb::MaterializedQueryResult>();
	if (mat_result.RowCount() == 0) {
		return "{\"tables\": [], \"message\": \"No tables exist in the database. "
		       "The user can create tables with CREATE TABLE or by importing data.\"}";
	}
	return FormatResultAsJSON(mat_result, 500); // Allow more rows for catalog listing
}

static string ExecuteDescribeTable(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *schema_val = YYGetStr(root, "schema");
	auto *table_val = YYGetStr(root, "table");
	if (!schema_val || !table_val) {
		return "{\"error\": \"Missing 'schema' or 'table' parameter\"}";
	}

	string catalog_name;
	auto *catalog_val = YYGetStr(root, "catalog");
	if (catalog_val) {
		catalog_name = catalog_val;
	}

	// Use prepared statement to prevent SQL injection
	string query;
	duckdb::vector<duckdb::Value> params;
	if (!catalog_name.empty()) {
		query = "SELECT column_name, data_type, is_nullable, "
		        "column_default, COALESCE(comment, '') as comment "
		        "FROM information_schema.columns "
		        "WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3 "
		        "ORDER BY ordinal_position";
		params.push_back(duckdb::Value(catalog_name));
		params.push_back(duckdb::Value(string(schema_val)));
		params.push_back(duckdb::Value(string(table_val)));
	} else {
		query = "SELECT column_name, data_type, is_nullable, "
		        "column_default, COALESCE(comment, '') as comment "
		        "FROM information_schema.columns "
		        "WHERE table_schema = $1 AND table_name = $2 "
		        "ORDER BY ordinal_position";
		params.push_back(duckdb::Value(string(schema_val)));
		params.push_back(duckdb::Value(string(table_val)));
	}
	auto prepared = state.conn->Prepare(query);
	if (prepared->HasError()) {
		return "{\"error\": \"" + JSONEscapeString(prepared->GetError()) + "\"}";
	}
	auto result = prepared->Execute(params, false);
	if (result->HasError()) {
		return "{\"error\": \"" + JSONEscapeString(result->GetError()) + "\"}";
	}

	auto &mat_result = result->Cast<duckdb::MaterializedQueryResult>();
	return FormatResultAsJSON(mat_result, 500);
}

static string ExecuteAskUser(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "Error: Invalid tool input";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *question = YYGetStr(root, "question");
	if (!question) {
		return "Error: Missing 'question' parameter";
	}

	// Print question with styling
	state.PrintF("\n%s%s%s\n\n", ANSI_BOLD, question, ANSI_BOLD_OFF);

	// Print options
	auto *options = yyjson_obj_get(root, "options");
	vector<string> option_strings;
	if (options && yyjson_is_arr(options)) {
		size_t idx2, max2;
		yyjson_val *opt;
		yyjson_arr_foreach(options, idx2, max2, opt) {
			if (yyjson_is_str(opt)) {
				option_strings.push_back(yyjson_get_str(opt));
			}
		}
		for (size_t i = 0; i < option_strings.size(); i++) {
			state.PrintF("  %s%zu.%s %s\n", ANSI_YELLOW, i + 1, ANSI_RESET, option_strings[i].c_str());
		}
	}
	state.Print("\n");

	// Read user input
	string user_input;
#ifdef HAVE_LINENOISE
	char *line = linenoise("\x1b[1;36mSelect\x1b[0m > ");
	if (line) {
		user_input = line;
		free(line);
	}
#else
	state.Print("Select > ");
	fflush(stdout);
	char buf[256];
	if (fgets(buf, sizeof(buf), stdin)) {
		user_input = buf;
	}
#endif

	// Parse selection
	StringUtil::Trim(user_input);
	if (!option_strings.empty()) {
		try {
			int idx = std::stoi(user_input) - 1;
			if (idx >= 0 && idx < static_cast<int>(option_strings.size())) {
				return "User selected: " + option_strings[static_cast<size_t>(idx)];
			}
		} catch (...) {
		}
	}
	return "User responded: " + user_input;
}

//! Ask the user to confirm an action. Returns true if confirmed.
//! Ask user to confirm an action. Returns empty string if approved,
//! or user's feedback/reason if denied.
static string ConfirmAction(ShellState &state, const string &action_description) {
	state.PrintF("\n%s%s%s\n", ANSI_BOLD, action_description.c_str(), ANSI_BOLD_OFF);

	string user_input;
#ifdef HAVE_LINENOISE
	char *line = linenoise("\x1b[1;33mAllow?\x1b[0m [Y/n/feedback] > ");
	if (line) {
		user_input = line;
		free(line);
	}
#else
	state.Print("Allow? [Y/n/feedback] > ");
	fflush(stdout);
	char buf[1024];
	if (fgets(buf, sizeof(buf), stdin)) {
		user_input = buf;
	}
#endif
	StringUtil::Trim(user_input);
	// Default to yes on empty input
	if (user_input.empty() || user_input == "y" || user_input == "Y" || user_input == "yes" || user_input == "Yes") {
		return ""; // approved
	}
	// "n" or "no" with no further feedback
	if (user_input == "n" || user_input == "N" || user_input == "no" || user_input == "No") {
		return "User denied this action.";
	}
	// Anything else is feedback
	return "User denied this action with feedback: " + user_input;
}

static string ExecuteWriteFile(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *path_val = YYGetStr(root, "path");
	auto *content_val = YYGetStr(root, "content");
	if (!path_val) {
		return "{\"error\": \"Missing 'path' parameter\"}";
	}
	if (!content_val) {
		return "{\"error\": \"Missing 'content' parameter\"}";
	}
	string path(path_val);
	string content(content_val);

	// Show file path and contents with line numbers
	state.PrintF("\n\x1b[1mWrite file:\x1b[0m %s\n", path.c_str());
	state.Print("\x1b[2m");

	// Display content with line numbers
	idx_t line_num = 1;
	idx_t pos = 0;
	while (pos <= content.size()) {
		idx_t eol = content.find('\n', pos);
		if (eol == string::npos) {
			eol = content.size();
		}
		string line = content.substr(pos, eol - pos);
		state.PrintF("  %4d │ %s\n", static_cast<int>(line_num), line.c_str());
		line_num++;
		pos = eol + 1;
		if (eol == content.size()) {
			break;
		}
	}
	state.Print("\x1b[0m");

	auto write_denied = ConfirmAction(state, "Write this file?");
	if (!write_denied.empty()) {
		return "{\"error\": \"" + JSONEscapeString(write_denied) + "\"}";
	}

	// Write the file
	FILE *f = fopen(path.c_str(), "w");
	if (!f) {
		string err = "Failed to open file: " + path + " (" + strerror(errno) + ")";
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}
	size_t written = fwrite(content.c_str(), 1, content.size(), f);
	int write_errno = errno;
	fclose(f);

	if (written != content.size()) {
		string err = "Failed to write all bytes to file (" + string(strerror(write_errno)) + ")";
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}

	state.PrintF("\x1b[32mWrote %zu bytes to %s\x1b[0m\n", written, path.c_str());
	return "{\"ok\": true, \"message\": \"File written successfully\", \"path\": \"" + JSONEscapeString(path) +
	       "\", \"bytes\": " + to_string(written) + "}";
}

static string ExecuteRunCommand(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *cmd_val = YYGetStr(root, "command");
	if (!cmd_val) {
		return "{\"error\": \"Missing 'command' parameter\"}";
	}
	string command(cmd_val);

	// Show command and ask for confirmation
	state.PrintF("\n\x1b[2mRun command:\x1b[0m\n\x1b[33m%s\x1b[0m\n", command.c_str());
	auto cmd_denied = ConfirmAction(state, "Run this command?");
	if (!cmd_denied.empty()) {
		return "{\"error\": \"" + JSONEscapeString(cmd_denied) + "\"}";
	}

	// Execute the command and capture output
	string output;
	string redirect_cmd = command + " 2>&1";
	FILE *pipe = popen(redirect_cmd.c_str(), "r");
	if (!pipe) {
		string err = "Failed to execute command";
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}

	char buf[4096];
	while (fgets(buf, sizeof(buf), pipe)) {
		output += buf;
		// Also print to terminal so user can see
		state.Print(buf);
	}
	int exit_code = pclose(pipe);
	// pclose returns the exit status in the format of waitpid
#ifndef _WIN32
	exit_code = WEXITSTATUS(exit_code);
#endif

	// Ensure we're on a new line before printing status
	if (!output.empty() && output.back() != '\n') {
		state.Print("\n");
	}
	if (exit_code != 0) {
		state.PrintF("\x1b[31mCommand exited with code %d\x1b[0m\n", exit_code);
	} else {
		state.PrintF("\x1b[32mCommand completed successfully\x1b[0m\n");
	}

	// Truncate output if too long — keep head and tail so the model sees
	// both the beginning context and the end (where errors typically appear)
	static constexpr size_t MAX_OUTPUT_BYTES = 100000;
	static constexpr size_t HEAD_BYTES = 50000;
	static constexpr size_t TAIL_BYTES = 45000;
	string output_for_model = output;
	bool was_truncated = false;
	if (output.size() > MAX_OUTPUT_BYTES) {
		was_truncated = true;
		string head = output.substr(0, HEAD_BYTES);
		string tail = output.substr(output.size() - TAIL_BYTES);
		output_for_model = head + "\n\n... [" + to_string(output.size() - HEAD_BYTES - TAIL_BYTES) +
		                   " bytes truncated] ...\n\n" + tail;
	}

	// Build result JSON
	auto *rdoc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard rdoc_guard(rdoc);
	auto *rroot = yyjson_mut_obj(rdoc);
	yyjson_mut_doc_set_root(rdoc, rroot);
	yyjson_mut_obj_add_int(rdoc, rroot, "exit_code", exit_code);
	yyjson_mut_obj_add_strcpy(rdoc, rroot, "output", output_for_model.c_str());
	if (was_truncated) {
		yyjson_mut_obj_add_int(rdoc, rroot, "total_output_bytes", static_cast<int64_t>(output.size()));
		yyjson_mut_obj_add_bool(rdoc, rroot, "truncated", true);
	}
	if (exit_code == 0) {
		yyjson_mut_obj_add_bool(rdoc, rroot, "ok", true);
	}

	size_t len = 0;
	auto *json_str = yyjson_mut_write(rdoc, 0, &len);
	if (!json_str) {
		return "{\"exit_code\": " + to_string(exit_code) + "}";
	}
	string json_result(json_str, len);
	free(json_str);
	return json_result;
}

static string ExecuteReadFile(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *path_val = YYGetStr(root, "path");
	if (!path_val) {
		return "{\"error\": \"Missing 'path' parameter\"}";
	}
	string path(path_val);

	state.PrintF("\n\x1b[2mReading %s\x1b[0m\n", path.c_str());

	FILE *f = fopen(path.c_str(), "r");
	if (!f) {
		string err = "Failed to open file: " + path + " (" + strerror(errno) + ")";
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}

	string content;
	char buf[4096];
	while (size_t n = fread(buf, 1, sizeof(buf), f)) {
		content.append(buf, n);
	}
	fclose(f);

	// Truncate if too large
	static constexpr size_t MAX_FILE_READ = 100000;
	bool truncated = false;
	if (content.size() > MAX_FILE_READ) {
		truncated = true;
		content.resize(MAX_FILE_READ);
		content += "\n\n... [file truncated at " + to_string(MAX_FILE_READ) + " bytes]";
	}

	state.PrintF("\x1b[2mRead %zu bytes\x1b[0m\n", content.size());

	auto *rdoc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard rdoc_guard(rdoc);
	auto *rroot = yyjson_mut_obj(rdoc);
	yyjson_mut_doc_set_root(rdoc, rroot);
	yyjson_mut_obj_add_strcpy(rdoc, rroot, "content", content.c_str());
	yyjson_mut_obj_add_strcpy(rdoc, rroot, "path", path.c_str());
	yyjson_mut_obj_add_int(rdoc, rroot, "size", static_cast<int64_t>(content.size()));
	if (truncated) {
		yyjson_mut_obj_add_bool(rdoc, rroot, "truncated", true);
	}

	size_t len = 0;
	auto *json_str = yyjson_mut_write(rdoc, 0, &len);
	if (!json_str) {
		return "{\"error\": \"Failed to serialize result\"}";
	}
	string json_result(json_str, len);
	free(json_str);
	return json_result;
}

static string ExecuteEditFile(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *path_val = YYGetStr(root, "path");
	auto *old_str_val = YYGetStr(root, "old_string");
	auto *new_str_val = YYGetStr(root, "new_string");
	if (!path_val || !old_str_val || !new_str_val) {
		return "{\"error\": \"Missing required parameter (path, old_string, new_string)\"}";
	}
	string path(path_val);
	string old_str(old_str_val);
	string new_str(new_str_val);

	// Read the file
	FILE *f = fopen(path.c_str(), "r");
	if (!f) {
		string err = "Failed to open file: " + path + " (" + strerror(errno) + ")";
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}
	string content;
	char buf[4096];
	while (size_t n = fread(buf, 1, sizeof(buf), f)) {
		content.append(buf, n);
	}
	fclose(f);

	// Find the old string
	auto pos = content.find(old_str);
	if (pos == string::npos) {
		return "{\"error\": \"old_string not found in file\"}";
	}

	// Check for uniqueness
	auto second = content.find(old_str, pos + old_str.size());
	if (second != string::npos) {
		return "{\"error\": \"old_string found multiple times — provide a larger, unique string to match\"}";
	}

	// Show the edit
	state.PrintF("\n\x1b[1mEdit file:\x1b[0m %s\n", path.c_str());
	state.PrintF("\x1b[31m- %zu chars removed\x1b[0m\n", old_str.size());
	state.PrintF("\x1b[32m+ %zu chars added\x1b[0m\n", new_str.size());

	auto edit_denied = ConfirmAction(state, "Apply this edit?");
	if (!edit_denied.empty()) {
		return "{\"error\": \"" + JSONEscapeString(edit_denied) + "\"}";
	}

	// Apply the replacement
	content.replace(pos, old_str.size(), new_str);

	// Write back
	f = fopen(path.c_str(), "w");
	if (!f) {
		string err = "Failed to write file: " + path + " (" + strerror(errno) + ")";
		state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", err.c_str());
		return "{\"error\": \"" + JSONEscapeString(err) + "\"}";
	}
	fwrite(content.c_str(), 1, content.size(), f);
	fclose(f);

	state.PrintF("\x1b[32mEdited %s\x1b[0m\n", path.c_str());
	return "{\"ok\": true, \"message\": \"Edit applied successfully\"}";
}

static string ExecuteGlob(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *pattern_val = YYGetStr(root, "pattern");
	if (!pattern_val) {
		return "{\"error\": \"Missing 'pattern' parameter\"}";
	}
	string pattern(pattern_val);

	string search_path = ".";
	auto *path_val = YYGetStr(root, "path");
	if (path_val) {
		search_path = path_val;
	}

	state.PrintF("\n\x1b[2mSearching for %s in %s\x1b[0m\n", pattern.c_str(), search_path.c_str());

	// Use find + shell globbing via bash
	string cmd = "find " + search_path + " -name '" + pattern + "' -type f 2>/dev/null | sort | head -500";
	FILE *pipe = popen(cmd.c_str(), "r");
	if (!pipe) {
		return "{\"error\": \"Failed to execute find\"}";
	}

	vector<string> files;
	char buf[4096];
	while (fgets(buf, sizeof(buf), pipe)) {
		string file(buf);
		StringUtil::Trim(file);
		if (!file.empty()) {
			files.push_back(file);
		}
	}
	pclose(pipe);

	state.PrintF("\x1b[2mFound %zu files\x1b[0m\n", files.size());

	auto *rdoc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard rdoc_guard(rdoc);
	auto *rroot = yyjson_mut_obj(rdoc);
	yyjson_mut_doc_set_root(rdoc, rroot);
	auto *files_arr = yyjson_mut_arr(rdoc);
	for (auto &file : files) {
		yyjson_mut_arr_add_strcpy(rdoc, files_arr, file.c_str());
	}
	yyjson_mut_obj_add_val(rdoc, rroot, "files", files_arr);
	yyjson_mut_obj_add_int(rdoc, rroot, "count", static_cast<int64_t>(files.size()));

	size_t len = 0;
	auto *json_str = yyjson_mut_write(rdoc, 0, &len);
	if (!json_str) {
		return "{\"error\": \"Failed to serialize result\"}";
	}
	string json_result(json_str, len);
	free(json_str);
	return json_result;
}

static string ExecuteGrep(ShellState &state, const string &input_json) {
	auto *doc = yyjson_read(input_json.c_str(), input_json.size(), 0);
	if (!doc) {
		return "{\"error\": \"Invalid tool input\"}";
	}
	YYJsonDocGuard doc_guard(doc);

	auto *root = yyjson_doc_get_root(doc);
	auto *pattern_val = YYGetStr(root, "pattern");
	if (!pattern_val) {
		return "{\"error\": \"Missing 'pattern' parameter\"}";
	}
	string pattern(pattern_val);

	string search_path = ".";
	auto *path_val = YYGetStr(root, "path");
	if (path_val) {
		search_path = path_val;
	}

	string include_glob;
	auto *include_val = YYGetStr(root, "include");
	if (include_val) {
		include_glob = include_val;
	}

	state.PrintF("\n\x1b[2mSearching for '%s' in %s\x1b[0m\n", pattern.c_str(), search_path.c_str());

	// Use grep -rn (recursive, line numbers)
	string cmd = "grep -rn";
	if (!include_glob.empty()) {
		cmd += " --include='" + include_glob + "'";
	}
	cmd += " -- '" + pattern + "' " + search_path + " 2>/dev/null | head -200";
	FILE *pipe = popen(cmd.c_str(), "r");
	if (!pipe) {
		return "{\"error\": \"Failed to execute grep\"}";
	}

	string output;
	int match_count = 0;
	char buf[4096];
	while (fgets(buf, sizeof(buf), pipe)) {
		output += buf;
		match_count++;
	}
	pclose(pipe);

	state.PrintF("\x1b[2mFound %d matches\x1b[0m\n", match_count);

	// Truncate if too large
	static constexpr size_t MAX_GREP_OUTPUT = 50000;
	bool truncated = false;
	if (output.size() > MAX_GREP_OUTPUT) {
		truncated = true;
		output.resize(MAX_GREP_OUTPUT);
		output += "\n... [output truncated]";
	}

	auto *rdoc = yyjson_mut_doc_new(nullptr);
	YYJsonMutDocGuard rdoc_guard(rdoc);
	auto *rroot = yyjson_mut_obj(rdoc);
	yyjson_mut_doc_set_root(rdoc, rroot);
	yyjson_mut_obj_add_strcpy(rdoc, rroot, "output", output.c_str());
	yyjson_mut_obj_add_int(rdoc, rroot, "match_count", match_count);
	if (truncated) {
		yyjson_mut_obj_add_bool(rdoc, rroot, "truncated", true);
	}

	size_t len = 0;
	auto *json_str = yyjson_mut_write(rdoc, 0, &len);
	if (!json_str) {
		return "{\"error\": \"Failed to serialize result\"}";
	}
	string json_result(json_str, len);
	free(json_str);
	return json_result;
}

static string ExecuteTool(ShellState &state, AIConversationState &conv, const string &tool_name,
                          const string &input_json) {
	if (tool_name == "run_sql") {
		return ExecuteRunSQL(state, conv, input_json);
	}
	if (tool_name == "list_tables") {
		return ExecuteListTables(state);
	}
	if (tool_name == "describe_table") {
		return ExecuteDescribeTable(state, input_json);
	}
	if (tool_name == "ask_user") {
		return ExecuteAskUser(state, input_json);
	}
	if (tool_name == "write_file") {
		return ExecuteWriteFile(state, input_json);
	}
	if (tool_name == "bash") {
		return ExecuteRunCommand(state, input_json);
	}
	if (tool_name == "read_file") {
		return ExecuteReadFile(state, input_json);
	}
	if (tool_name == "edit_file") {
		return ExecuteEditFile(state, input_json);
	}
	if (tool_name == "glob") {
		return ExecuteGlob(state, input_json);
	}
	if (tool_name == "grep") {
		return ExecuteGrep(state, input_json);
	}
	return "{\"error\": \"Unknown tool: " + JSONEscapeString(tool_name) + "\"}";
}

// ---------------------------------------------------------------------------
// HTTP POST to Claude API
// ---------------------------------------------------------------------------

static duckdb::unique_ptr<HTTPResponse> CallClaudeAPI(ShellState &state, const string &request_body,
                                                      const string &api_key) {
	auto &db = *state.db->instance;
	auto &http_util = HTTPUtil::Get(db);

	HTTPHeaders headers;
	headers.Insert("x-api-key", api_key);
	headers.Insert("anthropic-version", "2023-06-01");
	headers.Insert("anthropic-beta", "prompt-caching-2024-07-31");
	headers.Insert("Content-Type", "application/json");

	// API_URL must be stored as a string that outlives PostRequestInfo,
	// because BaseRequest::url is a reference member
	string url(API_URL);
	auto params = http_util.InitializeParameters(db, url);
	params->timeout = API_TIMEOUT_SECONDS;
	params->retries = 2;
	params->retry_wait_ms = 1000;

	PostRequestInfo info(url, headers, *params, const_data_ptr_cast(request_body.c_str()), request_body.size());
	info.try_request = true;

	return http_util.Request(info);
}

// ---------------------------------------------------------------------------
// Agent loop — runs one full turn (may loop for tool calls)
// ---------------------------------------------------------------------------

static void RunAgentLoop(ShellState &state, AIConversationState &conv, const string &api_key, const string &model,
                         const string &system_prompt) {
	Spinner spinner;
	spinner.interrupt_state = &state;
	int64_t total_input_tokens = 0;
	int64_t total_output_tokens = 0;

	// Repair broken conversation state: validate message sequence.
	// Rules: 1) Messages must alternate user/assistant (with first being user)
	//        2) Every assistant tool_use must be followed by user tool_result with matching IDs
	//        3) Conversation must end with a user message (so we can send the next request)
	{
		idx_t valid_end = 0;
		for (idx_t i = 0; i < conv.messages.size(); i++) {
			auto &msg = conv.messages[i];

			// Check alternation: even indices should be user, odd should be assistant
			// (first message is always user)
			if (i % 2 == 0 && msg.role != "user") {
				break;
			}
			if (i % 2 == 1 && msg.role != "assistant") {
				break;
			}

			if (msg.role == "assistant") {
				// Parse to check for tool_use blocks
				auto *adoc = yyjson_read(msg.content_json.c_str(), msg.content_json.size(), 0);
				if (!adoc) {
					break;
				}
				YYJsonDocGuard ag(adoc);
				auto *aroot = yyjson_doc_get_root(adoc);

				// Collect tool_use IDs
				vector<string> tool_use_ids;
				if (yyjson_is_arr(aroot)) {
					size_t aidx, amax;
					yyjson_val *ablock;
					yyjson_arr_foreach(aroot, aidx, amax, ablock) {
						auto *type = YYGetStr(ablock, "type");
						auto *id = YYGetStr(ablock, "id");
						if (type && string(type) == "tool_use" && id) {
							tool_use_ids.push_back(string(id));
						}
					}
				}

				if (!tool_use_ids.empty()) {
					// Must have a following user message with matching tool_results
					if (i + 1 >= conv.messages.size() || conv.messages[i + 1].role != "user") {
						break;
					}

					auto &next_msg = conv.messages[i + 1];
					auto *udoc = yyjson_read(next_msg.content_json.c_str(), next_msg.content_json.size(), 0);
					if (!udoc) {
						break;
					}
					YYJsonDocGuard ug(udoc);
					auto *uroot = yyjson_doc_get_root(udoc);
					if (!yyjson_is_arr(uroot)) {
						break;
					}

					duckdb::unordered_set<string> result_ids;
					size_t uidx, umax;
					yyjson_val *ublock;
					yyjson_arr_foreach(uroot, uidx, umax, ublock) {
						auto *type = YYGetStr(ublock, "tool_use_id");
						if (type) {
							result_ids.insert(string(type));
						}
					}

					bool all_matched = true;
					for (auto &tool_id : tool_use_ids) {
						if (result_ids.find(tool_id) == result_ids.end()) {
							all_matched = false;
							break;
						}
					}
					if (!all_matched) {
						break;
					}

					// This assistant + tool_result pair is valid, skip the tool_result message
					valid_end = i + 2;
					i++; // skip the tool_result user message in the loop
					continue;
				}
			}

			valid_end = i + 1;
		}

		if (valid_end < conv.messages.size()) {
			conv.messages.resize(valid_end);
		}

		// Conversation must end with a user message for the API
		while (!conv.messages.empty() && conv.messages.back().role != "user") {
			conv.messages.pop_back();
		}
	}

	for (int round = 0; round < MAX_TOOL_ROUNDS; round++) {
		if (state.seenInterrupt) {
			state.ClearInterrupt();
			state.Print("\nCancelled.\n\n");
			return;
		}

		// Build request
		auto request_body = BuildRequestJSON(conv.messages, system_prompt, model);

		// Show animated spinner and call API on background thread
		// so Escape/Ctrl+C can interrupt immediately.
		// The API call's data is heap-allocated so the thread can be safely detached on cancel.
		spinner.Start("Thinking...");

		struct APICallState {
			duckdb::atomic<bool> done {false};
			duckdb::unique_ptr<HTTPResponse> response;
			// Own copies of data the thread needs (so it doesn't reference stack locals)
			string request_body_copy;
			string api_key_copy;
		};
		auto api_state = duckdb::make_shared_ptr<APICallState>();
		api_state->request_body_copy = request_body;
		api_state->api_key_copy = api_key;

		// Capture shared_ptr by value so the thread owns a reference
		auto api_thread = make_uniq<duckdb::thread>([&state, api_state]() {
			api_state->response = CallClaudeAPI(state, api_state->request_body_copy, api_state->api_key_copy);
			api_state->done = true;
		});

		// Wait for API to complete, checking for interrupts
		bool interrupted = false;
		while (!api_state->done) {
			if (state.seenInterrupt) {
				interrupted = true;
				break;
			}
			duckdb::ThreadUtil::SleepMs(50);
		}

		spinner.Stop();

		if (interrupted) {
			state.ClearInterrupt();
			// Detach the thread — it holds its own copies of the data via shared_ptr
			api_thread->detach();
			state.Print("\nCancelled.\n\n");
			return;
		}

		api_thread->join();

		// Move response out of the shared state
		auto response = std::move(api_state->response);

		if (!response) {
			state.Print(PrintOutput::STDERR, "Error: No response from API\n\n");
			return;
		}

		if (response->HasRequestError()) {
			state.PrintF(PrintOutput::STDERR, "Error: %s\n\n", response->GetRequestError().c_str());
			return;
		}

		// Check HTTP status
		if (!response->Success()) {
			auto status_int = static_cast<int>(response->status);

			if (status_int == 401 || status_int == 403) {
				state.Print(PrintOutput::STDERR, "Error: Invalid API key. Check ANTHROPIC_API_KEY.\n\n");
				return;
			}

			if (status_int == 529) {
				for (int countdown = 5; countdown > 0; countdown--) {
					string msg = "Claude is busy, retrying in " + to_string(countdown) + "s...";
					spinner.Start(msg, "\x1b[1;33m");
					ShellState::Sleep(1000);
				}
				spinner.Stop();
				round--; // Don't count 529 retries against tool round limit
				continue;
			}

			// Try to parse error message from response body
			if (!response->body.empty()) {
				auto api_resp = ParseResponse(response->body);
				if (!api_resp.error_message.empty()) {
					state.PrintF(PrintOutput::STDERR, "API error (%d): %s\n\n", status_int,
					             api_resp.error_message.c_str());
					return;
				}
			}

			state.PrintF(PrintOutput::STDERR, "API error: HTTP %d\n\n", status_int);
			return;
		}

		// Parse response
		auto api_response = ParseResponse(response->body);
		if (!api_response.error_message.empty()) {
			state.PrintF(PrintOutput::STDERR, "API error: %s\n\n", api_response.error_message.c_str());
			return;
		}

		total_input_tokens += api_response.input_tokens;
		total_output_tokens += api_response.output_tokens;

		// Serialize assistant response (but don't store yet — wait for tool results)
		string assistant_content_json = SerializeContentBlocks(api_response.content);

		// Print text blocks to terminal with markdown formatting
		for (auto &block : api_response.content) {
			if (block.type == "text" && !block.text.empty()) {
				idx_t term_width = 100;
#ifdef HAVE_LINENOISE
				auto term_size = duckdb::Terminal::GetTerminalSize();
				if (term_size.ws_col > 0) {
					term_width = static_cast<idx_t>(term_size.ws_col);
				}
#endif
				state.Print("\n");
				state.Print(RenderMarkdown(block.text, term_width));
			}
		}

		// If response was truncated, warn the user
		if (api_response.stop_reason == "max_tokens") {
			state.Print("\n\x1b[33m(Response was truncated due to length)\x1b[0m\n");
		}

		// If not tool_use, store the assistant message and we're done
		if (api_response.stop_reason != "tool_use") {
			AIMessage assistant_msg;
			assistant_msg.role = "assistant";
			assistant_msg.content_json = std::move(assistant_content_json);
			conv.messages.push_back(std::move(assistant_msg));

			if (!GetEnvVar("ANTHROPIC_SHOW_TOKENS").empty()) {
				state.PrintF("\n\x1b[2m  tokens: %d in, %d out\x1b[0m\n", static_cast<int>(total_input_tokens),
				             static_cast<int>(total_output_tokens));
			}
			state.Print("\n");
			return;
		}

		// Execute tool calls and collect results
		vector<ToolResult> tool_results;
		for (auto &block : api_response.content) {
			if (block.type == "tool_use" && !block.id.empty()) {
				// Tool-specific display with colors
				if (block.name == "run_sql") {
					// SQL is printed inside ExecuteRunSQL
				} else if (block.name == "list_tables") {
					state.Print("\n\x1b[2mListing tables...\x1b[0m\n");
				} else if (block.name == "describe_table") {
					// Parse table name for display
					auto *input_doc = yyjson_read(block.input_json.c_str(), block.input_json.size(), 0);
					if (input_doc) {
						YYJsonDocGuard ig(input_doc);
						auto *input_root = yyjson_doc_get_root(input_doc);
						auto *schema_v = YYGetStr(input_root, "schema");
						auto *table_v = YYGetStr(input_root, "table");
						if (schema_v && table_v) {
							state.PrintF("\n\x1b[2mDescribing %s.%s...\x1b[0m\n", schema_v, table_v);
						} else {
							state.Print("\n\x1b[2mDescribing table...\x1b[0m\n");
						}
					} else {
						state.Print("\n\x1b[2mDescribing table...\x1b[0m\n");
					}
				} else if (block.name != "write_file" && block.name != "bash" && block.name != "read_file" &&
				           block.name != "edit_file" && block.name != "glob" && block.name != "grep") {
					// These tools handle their own display
					state.PrintF("\n\x1b[2m[%s]\x1b[0m\n", block.name.c_str());
				}

				string result;
				bool is_error = false;
				try {
					result = ExecuteTool(state, conv, block.name, block.input_json);
				} catch (std::exception &e) {
					result = string("Error: ") + e.what();
					is_error = true;
					state.PrintF(PrintOutput::STDERR, "\x1b[31mError: %s\x1b[0m\n", e.what());
				}

				ToolResult tr;
				tr.tool_use_id = block.id;
				tr.content = result;
				tr.is_error = is_error;
				tool_results.push_back(std::move(tr));
			}
		}

		// Check for interrupt during tool execution
		if (state.seenInterrupt) {
			state.ClearInterrupt();
			state.Print("\nCancelled.\n\n");
			return;
		}

		// Store assistant message and tool results atomically
		// (prevents broken conversation state if interrupted between the two)
		AIMessage assistant_msg;
		assistant_msg.role = "assistant";
		assistant_msg.content_json = std::move(assistant_content_json);
		conv.messages.push_back(std::move(assistant_msg));

		AIMessage tool_msg;
		tool_msg.role = "user";
		tool_msg.content_json = SerializeToolResults(tool_results);
		conv.messages.push_back(std::move(tool_msg));
	}

	state.Print(PrintOutput::STDERR, "Error: Too many tool rounds. Try a simpler question.\n\n");
}

// ---------------------------------------------------------------------------
// Main entry point: .ai command
// ---------------------------------------------------------------------------

MetadataResult RunAIMode(ShellState &state, const vector<string> &args) {
	// Safe mode check
	if (state.safe_mode) {
		state.Print(PrintOutput::STDERR, "Error: .ask is not available in safe mode\n");
		return MetadataResult::FAIL;
	}

	// Initialize conversation state if needed
	if (!state.ai_conversation) {
		state.ai_conversation = make_uniq<AIConversationState>();
	}
	auto &conv = *state.ai_conversation;

	// Handle subcommands that don't need API access
	if (args.size() > 1) {
		const auto &subcmd = args[1];
		if (subcmd == "new" || subcmd == "clear") {
			conv.messages.clear();
			state.Print("Conversation cleared.\n");
			return MetadataResult::SUCCESS;
		}
	}

	// Resolve API key
	string api_key = GetEnvVar("ANTHROPIC_API_KEY");
	if (api_key.empty()) {
		state.Print(PrintOutput::STDERR,
		            "Error: No API key found. Set the ANTHROPIC_API_KEY environment variable.\n");
		return MetadataResult::FAIL;
	}

	// Ensure httpfs is loaded for HTTP POST support
	if (!EnsureHTTPFS(state, conv)) {
		return MetadataResult::FAIL;
	}

	// Get model
	string model = GetEnvVar("ANTHROPIC_MODEL");
	if (model.empty()) {
		model = DEFAULT_MODEL;
	}

	// Handle subcommands that need API access
	if (args.size() > 1) {

		// Single-shot mode: .ai <question>
		string question;
		for (size_t i = 1; i < args.size(); i++) {
			if (i > 1) {
				question += " ";
			}
			question += args[i];
		}

		// Store as JSON string
		AIMessage user_msg;
		user_msg.role = "user";
		user_msg.content_json = "\"" + JSONEscapeString(question) + "\"";
		conv.messages.push_back(std::move(user_msg));
		string system_prompt = BuildSystemPrompt(state);
		RunAgentLoop(state, conv, api_key, model, system_prompt);
		return MetadataResult::SUCCESS;
	}

	// Interactive mode
	if (conv.messages.empty()) {
		state.Print("Entering ask mode. Type /exit to return to SQL.\n\n");
	} else {
		state.PrintF("Resuming conversation (%zu messages). Type /new for a fresh start.\n\n",
		             conv.messages.size());
	}

	string system_prompt = BuildSystemPrompt(state);

#ifdef HAVE_LINENOISE
	// Save SQL history and load AI history
	duckdb::LocalFileSystem lfs;
	string home = lfs.GetHomeDirectory();
	string sql_history_path = home + "/.duckdb_history";
	string ai_history_path = home + "/.duckdb_ai_history";

	// Save current SQL history, clear, then load AI history
	state.ShellSaveHistory(sql_history_path.c_str());
	duckdb::History::Clear();
	duckdb::History::LoadRaw(ai_history_path.c_str());

	// Save and disable SQL-specific linenoise settings for AI input
	// Keep multiline ON for line wrapping, but force submit on Enter (skip SQLIsComplete check)
	auto *saved_completion_cb = linenoiseGetCompletionCallback();
	auto *saved_format_cb = linenoiseGetFormatCallback();
	duckdb::Terminal::SetForceSubmitOnEnter(true);
	linenoiseSetCompletionCallback(nullptr);
	linenoiseSetFormatCallback(nullptr);
	linenoiseSetErrorRendering(0);
	linenoiseSetCompletionRendering(0);
#endif
	// Disable SQL syntax highlighting during AI mode
	bool saved_highlighting = state.highlighting_enabled;
	state.highlighting_enabled = false;

	while (true) {
		if (state.seenInterrupt) {
			state.ClearInterrupt();
			// Don't exit ask mode on Ctrl+C, just return to prompt
		}

		// Read user input
		char *line = nullptr;
#ifdef HAVE_LINENOISE
		line = linenoise("\x1b[1;36mASK\x1b[0m > ");
#else
		state.Print("ASK > ");
		fflush(stdout);
		char buf[4096];
		if (fgets(buf, sizeof(buf), stdin)) {
			line = strdup(buf);
		}
#endif
		if (!line) {
			// Ctrl+D
			state.Print("\nExiting ask mode.\n\n");
			break;
		}

		string input(line);
		free(line);

		StringUtil::Trim(input);
		if (input.empty() || input[0] == '\3') {
			// Empty input or Ctrl+C — skip
			continue;
		}

		// Sub-commands
		if (input == "/exit" || input == ".exit") {
			state.Print("Exiting ask mode.\n\n");
			break;
		}
		if (input == "/new" || input == "/clear") {
			conv.messages.clear();
			state.Print("Conversation cleared.\n\n");
			continue;
		}
		if (input == "/help") {
			state.Print("/new      Start new conversation\n");
			state.Print("/model    Switch AI model\n");
			state.Print("/prompt   Show system prompt\n");
			state.Print("/exit     Return to SQL mode\n");
			state.Print("/help     Show this help\n\n");
			continue;
		}
		if (input == "/prompt") {
			state.PrintF("\n%sSystem prompt (%zu chars):%s\n\n", ANSI_BOLD, system_prompt.size(), ANSI_BOLD_OFF);
			state.Print(ANSI_DIM);
			state.Print(system_prompt);
			state.Print(ANSI_RESET);
			state.PrintF("\n%sConversation: %zu messages%s\n\n", ANSI_DIM, conv.messages.size(), ANSI_RESET);
			continue;
		}
		if (input == "/model") {
			state.PrintF("\nCurrent model: %s%s%s\n\n", ANSI_BOLD, model.c_str(), ANSI_BOLD_OFF);
			state.PrintF("  %s1.%s claude-sonnet-4-20250514 (balanced)\n", ANSI_YELLOW, ANSI_RESET);
			state.PrintF("  %s2.%s claude-opus-4-20250514 (most capable)\n", ANSI_YELLOW, ANSI_RESET);
			state.PrintF("  %s3.%s claude-haiku-4-5-20251001 (fastest)\n\n", ANSI_YELLOW, ANSI_RESET);
			string choice;
#ifdef HAVE_LINENOISE
			char *mline = linenoise("\x1b[1;36mSelect\x1b[0m > ");
			if (mline) {
				choice = mline;
				free(mline);
			}
#else
			state.Print("Select > ");
			fflush(stdout);
			char mbuf[256];
			if (fgets(mbuf, sizeof(mbuf), stdin)) {
				choice = mbuf;
			}
#endif
			StringUtil::Trim(choice);
			if (choice == "1" || choice == "sonnet") {
				model = "claude-sonnet-4-20250514";
			} else if (choice == "2" || choice == "opus") {
				model = "claude-opus-4-20250514";
			} else if (choice == "3" || choice == "haiku") {
				model = "claude-haiku-4-5-20251001";
			} else if (!choice.empty()) {
				// Allow raw model ID
				model = choice;
			}
			state.PrintF("Model set to: %s%s%s\n\n", ANSI_BOLD, model.c_str(), ANSI_BOLD_OFF);
			continue;
		}

		// Add to AI history
#ifdef HAVE_LINENOISE
		linenoiseHistoryAdd(input.c_str());
#endif

		// Add user message and run agent.
		// If the last message is already a user message (from a cancelled turn),
		// replace it so we don't get consecutive user messages.
		string user_content = "\"" + JSONEscapeString(input) + "\"";
		if (!conv.messages.empty() && conv.messages.back().role == "user") {
			conv.messages.back().content_json = std::move(user_content);
		} else {
			AIMessage user_msg;
			user_msg.role = "user";
			user_msg.content_json = std::move(user_content);
			conv.messages.push_back(std::move(user_msg));
		}
		RunAgentLoop(state, conv, api_key, model, system_prompt);
	}

#ifdef HAVE_LINENOISE
	// Save AI history, clear, then restore SQL history
	linenoiseHistorySave(ai_history_path.c_str());
	duckdb::History::Clear();
	state.ShellLoadHistory(sql_history_path.c_str());

	// Add successful AI-executed SQL queries to the SQL history
	for (auto &sql : conv.successful_sql) {
		linenoiseHistoryAdd(sql.c_str());
	}
	conv.successful_sql.clear();

	// Restore all linenoise settings for SQL input
	duckdb::Terminal::SetForceSubmitOnEnter(false);
	linenoiseSetCompletionCallback(saved_completion_cb);
	linenoiseSetFormatCallback(saved_format_cb);
	linenoiseSetErrorRendering(1);
	linenoiseSetCompletionRendering(1);
#endif
	state.highlighting_enabled = saved_highlighting;

	return MetadataResult::SUCCESS;
}

} // namespace duckdb_shell
