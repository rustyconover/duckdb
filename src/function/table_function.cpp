#include "duckdb/function/table_function.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/common/vector/union_vector.hpp"

namespace duckdb {

GlobalTableFunctionState::~GlobalTableFunctionState() {
}

LocalTableFunctionState::~LocalTableFunctionState() {
}

PartitionStatistics::PartitionStatistics() : row_start(0), count(0), count_type(CountType::COUNT_APPROXIMATE) {
}

TableFunctionInfo::~TableFunctionInfo() {
}

static idx_t CountTableArguments(const TableFunction &function) {
	idx_t count = 0;
	for (auto &argument : function.GetArguments()) {
		if (argument == LogicalType::TABLE) {
			count++;
		}
	}
	return count;
}

static bool IsValidMultiTableInput(const TableFunctionBindInput &input) {
	auto table_count = CountTableArguments(input.table_function);
	if (table_count < 2 || input.input_table_types.size() != 1 ||
	    input.input_table_types[0].id() != LogicalTypeId::UNION ||
	    UnionType::GetMemberCount(input.input_table_types[0]) != table_count) {
		return false;
	}
	idx_t table_idx = 0;
	for (idx_t argument_idx = 0; argument_idx < input.table_function.GetArguments().size(); argument_idx++) {
		if (input.table_function.GetArguments()[argument_idx] != LogicalType::TABLE) {
			continue;
		}
		if (UnionType::GetMemberType(input.input_table_types[0], table_idx).id() != LogicalTypeId::STRUCT) {
			return false;
		}
		if (UnionType::GetMemberName(input.input_table_types[0], table_idx) !=
		    Identifier("arg_" + to_string(argument_idx))) {
			return false;
		}
		table_idx++;
	}
	return true;
}

static const LogicalType &GetMultiTableInputType(const TableFunctionBindInput &input) {
	if (!IsValidMultiTableInput(input)) {
		throw InvalidInputException("Table function bind input is not a valid multi-TABLE input");
	}
	return input.input_table_types[0];
}

bool TableFunctionBindInput::IsMultiTableInput() const {
	return IsValidMultiTableInput(*this);
}

idx_t TableFunctionBindInput::GetTableInputCount() const {
	return UnionType::GetMemberCount(GetMultiTableInputType(*this));
}

idx_t TableFunctionBindInput::GetTableArgumentIndex(idx_t table_index) const {
	GetMultiTableInputType(*this);
	idx_t table_ordinal = 0;
	for (idx_t argument_idx = 0; argument_idx < table_function.GetArguments().size(); argument_idx++) {
		if (table_function.GetArguments()[argument_idx] != LogicalType::TABLE) {
			continue;
		}
		if (table_ordinal == table_index) {
			return argument_idx;
		}
		table_ordinal++;
	}
	throw InvalidInputException("TABLE input index %llu is out of range", table_index);
}

const Identifier &TableFunctionBindInput::GetTableInputMemberName(idx_t table_index) const {
	auto &input_type = GetMultiTableInputType(*this);
	if (table_index >= UnionType::GetMemberCount(input_type)) {
		throw InvalidInputException("TABLE input index %llu is out of range", table_index);
	}
	return UnionType::GetMemberName(input_type, table_index);
}

const LogicalType &TableFunctionBindInput::GetTableInputType(idx_t table_index) const {
	auto &input_type = GetMultiTableInputType(*this);
	if (table_index >= UnionType::GetMemberCount(input_type)) {
		throw InvalidInputException("TABLE input index %llu is out of range", table_index);
	}
	return UnionType::GetMemberType(input_type, table_index);
}

static const Vector &GetMultiTableVector(const DataChunk &input) {
	if (input.ColumnCount() != 1 || input.data[0].GetType().id() != LogicalTypeId::UNION) {
		throw InvalidInputException("Multi-TABLE function input must contain one UNION column");
	}
	for (idx_t table_idx = 0; table_idx < UnionType::GetMemberCount(input.data[0].GetType()); table_idx++) {
		if (UnionType::GetMemberType(input.data[0].GetType(), table_idx).id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("Multi-TABLE function input UNION members must be STRUCTs");
		}
	}
	return input.data[0];
}

bool MultiTableFunctionInput::TryGetTableIndex(const DataChunk &input, idx_t row_index, idx_t &table_index) {
	if (row_index >= input.size()) {
		throw InvalidInputException("Multi-TABLE input row index %llu is out of range", row_index);
	}
	auto &input_vector = GetMultiTableVector(input);
	union_tag_t tag;
	if (!UnionVector::TryGetTag(input_vector, row_index, tag)) {
		return false;
	}
	table_index = tag;
	return true;
}

const Vector &MultiTableFunctionInput::GetTableRows(const DataChunk &input, idx_t table_index) {
	auto &input_vector = GetMultiTableVector(input);
	if (table_index >= UnionType::GetMemberCount(input_vector.GetType())) {
		throw InvalidInputException("TABLE input index %llu is out of range", table_index);
	}
	return UnionVector::GetMember(input_vector, table_index);
}

TableFunction::TableFunction(Identifier name, const vector<LogicalType> &arguments, table_function_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), arguments), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(function_),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), statistics_extended(nullptr),
      dependency(nullptr), cardinality(nullptr), get_metrics(nullptr), pushdown_complex_filter(nullptr),
      pushdown_expression(nullptr), combine_schema(nullptr), claim_batch(nullptr), finish_batch(nullptr),
      to_string(nullptr), table_scan_progress(nullptr), get_partition_data(nullptr), get_bind_info(nullptr),
      projection_expression_pushdown(nullptr), get_multi_file_reader(nullptr), supports_pushdown_type(nullptr),
      supports_pushdown_extract(nullptr), is_repeatable(nullptr), get_partition_info(nullptr),
      get_partition_stats(nullptr), get_virtual_columns(nullptr), get_row_id_columns(nullptr), set_scan_order(nullptr),
      serialize(nullptr), deserialize(nullptr), projection_pushdown(false), filter_pushdown(false), filter_prune(false),
      sampling_pushdown(false), late_materialization(false),
      return_type(TableFunctionReturnType::TABLE_RETURNING_FUNCTION) {
}

TableFunction::TableFunction(Identifier name, const vector<LogicalType> &arguments, std::nullptr_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), arguments), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(nullptr),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), statistics_extended(nullptr),
      dependency(nullptr), cardinality(nullptr), get_metrics(nullptr), pushdown_complex_filter(nullptr),
      pushdown_expression(nullptr), combine_schema(nullptr), claim_batch(nullptr), finish_batch(nullptr),
      to_string(nullptr), table_scan_progress(nullptr), get_partition_data(nullptr), get_bind_info(nullptr),
      projection_expression_pushdown(nullptr), get_multi_file_reader(nullptr), supports_pushdown_type(nullptr),
      supports_pushdown_extract(nullptr), is_repeatable(nullptr), get_partition_info(nullptr),
      get_partition_stats(nullptr), get_virtual_columns(nullptr), get_row_id_columns(nullptr), set_scan_order(nullptr),
      serialize(nullptr), deserialize(nullptr), projection_pushdown(false), filter_pushdown(false), filter_prune(false),
      sampling_pushdown(false), late_materialization(false),
      return_type(TableFunctionReturnType::TABLE_RETURNING_FUNCTION) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : TableFunction("", arguments, function_, bind, init_global, init_local) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, std::nullptr_t function_, table_function_bind_t bind,
                             table_function_init_global_t init_global, table_function_init_local_t init_local)
    : TableFunction("", arguments, function_, bind, init_global, init_local) {
}

TableFunction::TableFunction() : TableFunction("", {}, nullptr, nullptr, nullptr, nullptr) {
}

bool TableFunction::operator==(const TableFunction &rhs) const {
	return name == rhs.name && arguments == rhs.GetArguments() && varargs == rhs.GetVarArgs() && bind == rhs.bind &&
	       bind_replace == rhs.bind_replace && bind_operator == rhs.bind_operator && init_global == rhs.init_global &&
	       init_local == rhs.init_local && function == rhs.function && in_out_function == rhs.in_out_function &&
	       in_out_function_final == rhs.in_out_function_final && statistics == rhs.statistics &&
	       dependency == rhs.dependency && cardinality == rhs.cardinality &&
	       pushdown_complex_filter == rhs.pushdown_complex_filter && pushdown_expression == rhs.pushdown_expression &&
	       to_string == rhs.to_string && table_scan_progress == rhs.table_scan_progress &&
	       get_partition_data == rhs.get_partition_data && get_bind_info == rhs.get_bind_info &&
	       projection_expression_pushdown == rhs.projection_expression_pushdown &&
	       get_multi_file_reader == rhs.get_multi_file_reader && supports_pushdown_type == rhs.supports_pushdown_type &&
	       is_repeatable == rhs.is_repeatable && get_partition_info == rhs.get_partition_info &&
	       get_partition_stats == rhs.get_partition_stats && get_virtual_columns == rhs.get_virtual_columns &&
	       get_row_id_columns == rhs.get_row_id_columns && serialize == rhs.serialize &&
	       deserialize == rhs.deserialize && verify_serialization == rhs.verify_serialization &&
	       projection_pushdown == rhs.projection_pushdown && filter_pushdown == rhs.filter_pushdown &&
	       filter_prune == rhs.filter_prune && sampling_pushdown == rhs.sampling_pushdown &&
	       late_materialization == rhs.late_materialization && return_type == rhs.return_type &&
	       global_initialization == rhs.global_initialization;
}

bool TableFunction::operator!=(const TableFunction &rhs) const {
	return !(*this == rhs);
}

bool TableFunction::Equal(const TableFunction &rhs) const {
	// number of types
	if (this->GetArguments().size() != rhs.GetArguments().size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->GetArguments().size(); ++i) {
		if (this->GetArguments()[i] != rhs.GetArguments()[i]) {
			return false;
		}
	}
	// varargs
	if (this->GetVarArgs() != rhs.GetVarArgs()) {
		return false;
	}

	return true; // they are equal
}

bool TableFunctionInput::HandleBlocked(AsyncResult &blocked_result) {
	D_ASSERT(blocked_result.GetResultType() == AsyncResultType::BLOCKED);
	switch (results_execution_mode) {
	case AsyncResultsExecutionMode::TASK_EXECUTOR:
		async_result = std::move(blocked_result);
		return true;
	case AsyncResultsExecutionMode::SYNCHRONOUS:
		// run the I/O synchronously, then loop again to resume
		blocked_result.ExecuteTasksSynchronously();
		if (blocked_result.GetResultType() != AsyncResultType::HAVE_MORE_OUTPUT) {
			throw InternalException("Unexpected behaviour from ExecuteTasksSynchronously");
		}
		return false;
	default:
		throw InternalException("Unexpected AsyncResultsExecutionMode in HandleBlocked");
	}
}

} // namespace duckdb
