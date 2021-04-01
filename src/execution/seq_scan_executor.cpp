//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      predicate_(plan_->GetPredicate()),
      table_metadata_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_iter_(table_metadata_->table_->Begin(exec_ctx_->GetTransaction())),
      table_iter_end_(table_metadata_->table_->End()) {}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  while (table_iter_ != table_iter_end_) {
    *rid = table_iter_->GetRid();
    cur_tuple = *table_iter_;
    ++table_iter_;
    if (predicate_ == nullptr || predicate_->Evaluate(&cur_tuple, &table_metadata_->schema_).GetAs<bool>()) {
      const Schema *schema = plan_->OutputSchema();
      const auto &columns = schema->GetColumns();
      uint32_t column_count = schema->GetColumnCount();
      std::vector<Value> values(column_count);
      for (uint32_t ii = 0; ii < column_count; ++ii) {
        values[ii] = columns[ii].GetExpr()->Evaluate(&cur_tuple, &table_metadata_->schema_);
      }
      *tuple = Tuple(values, schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
