//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

using std::vector;

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      predicate_(plan_->Predicate()),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  assert(left_executor_ != nullptr);
  assert(right_executor_ != nullptr);
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const auto *left_schema = plan_->GetLeftPlan()->OutputSchema();
  const auto *right_schema = plan_->GetRightPlan()->OutputSchema();
  const auto *out_schema = plan_->OutputSchema();
  uint32_t column_number = out_schema->GetColumnCount();
  std::vector<Value> values(column_number);
  Tuple left_tuple;
  Tuple right_tuple;
  if (left_executor_->Next(&left_tuple, rid) && right_executor_->Next(&right_tuple, rid)) {
    if (predicate_ == nullptr ||
        predicate_->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
      const auto &columns = out_schema->GetColumns();
      for (uint32_t ii = 0; ii < column_number; ++ii) {
        values[ii] = columns[ii].GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      }
      *tuple = Tuple(values, out_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
