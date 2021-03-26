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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
  AbstractExecutor(exec_ctx),
  plan_(plan),
  predicate_(plan_->GetPredicate()),
  schema_(plan_->OutputSchema()) {
  Catalog *catalog = exec_ctx->GetCatalog();
  table_oid_t table_oid = plan->GetTableOid();
  TableMetadata *table_metadata = catalog->GetTable(table_oid);
  table_heap_ = table_metadata->table_.get();
  Transaction *transaction = exec_ctx->GetTransaction();
  table_iter_ = std::make_unique<TableIterator>(table_heap_->Begin(transaction));
}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  do {
    if (*table_iter_ == table_heap_->End()) {
      return false;
    }

    // Get tuple and assign to tuple.
    Tuple cur_tuple = **table_iter_;
    if (schema_ == nullptr) {
      *tuple = cur_tuple;
    } else {
      std::vector<Value> res;
      const std::vector<Column>& columns = schema_->GetColumns();
      for (auto& col : columns) {
        Value value = col.GetExpr()->Evaluate(&cur_tuple, schema_);
        res.emplace_back(value);
      }
      *tuple = Tuple(res, schema_);
    }
    ++(*table_iter_);

    if (predicate_ == nullptr) {
      return true;
    }
  } while (!predicate_->Evaluate(tuple, schema_).GetAs<bool>());  // iterate until gets the correct result
  return true;
}

}  // namespace bustub
