//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  assert(child_ != nullptr);
  child_->Init();
}

// A query may include multiple aggregates, like Min, Max, Count;
// Next() method returns one aggrate result at a time.
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (!aggregate_results_.empty()) {
    *tuple = aggregate_results_.back();
    aggregate_results_.pop_back();
    return true;
  }

  // Child query will be execute only once, all aggregate results will be stored at aggregate_results_.
  bool child_execution_suc = false;
  while (child_->Next(tuple, rid)) {
    child_execution_suc = true;
    aht_.InsertCombine(MakeKey(tuple), MakeVal(tuple));
  }
  if (!child_execution_suc) {
    return false;
  }

  const Schema *schema = plan_->OutputSchema();
  const AbstractExpression *having = plan_->GetHaving();
  const auto &columns = schema->GetColumns();
  uint32_t column_number = schema->GetColumnCount();
  std::vector<Value> values(column_number);
  for (aht_iterator_ = aht_.Begin(); aht_iterator_ != aht_.End(); ++aht_iterator_) {
    const std::vector<Value> &group_bys = aht_iterator_.Key().group_bys_;
    const std::vector<Value> &aggregates = aht_iterator_.Val().aggregates_;
    // "HAVING" is "WHERE" used for aggregates.
    if (having == nullptr || having->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      for (uint32_t ii = 0; ii < column_number; ++ii) {
        values[ii] = columns[ii].GetExpr()->EvaluateAggregate(group_bys, aggregates);
      }
      aggregate_results_.emplace_back(values, schema);
    }
  }

  if (!aggregate_results_.empty()) {
    *tuple = aggregate_results_.back();
    aggregate_results_.pop_back();
    return true;
  }
  return false;
}

}  // namespace bustub
