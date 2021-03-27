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
                                               std::unique_ptr<AbstractExecutor> &&right_executor) :
  AbstractExecutor(exec_ctx),
  plan_(plan),
  left_executor_(std::move(left_executor)),
  right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  assert(left_executor_ != nullptr);
  assert(right_executor_ != nullptr);
  left_executor_->Init();
  right_executor_->Init();
}

vector<Tuple> NestedLoopJoinExecutor::SubQueryExecution(std::unique_ptr<AbstractExecutor> *executor,
                                                              Tuple *tuple, RID *rid) {
  vector<Tuple> subquery_res;
  while ((*executor)->Next(tuple, rid)) {
    subquery_res.push_back(*tuple);
  }
  return subquery_res;
}

// Store all joined tuples into out_result_.
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!out_result_.empty()) {
    *tuple = out_result_.back();
    out_result_.pop_back();
    return true;
  }

  const auto *left_schema = plan_->GetLeftPlan()->OutputSchema();
  const auto *right_schema = plan_->GetRightPlan()->OutputSchema();
  const auto *out_schema = plan_->OutputSchema();
  vector<Tuple> left_result = SubQueryExecution(&left_executor_, tuple, rid);
  vector<Tuple> right_result = SubQueryExecution(&right_executor_, tuple, rid);
  uint32_t column_number = out_schema->GetColumnCount();
  std::vector<Value> values(column_number);
  for (Tuple& left_tuple : left_result) {
    for (Tuple& right_tuple : right_result) {
      if (plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        const auto& columns = out_schema->GetColumns();
        for (uint32_t ii = 0; ii < column_number; ++ii) {
          values[ii] = columns[ii].GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        }
        out_result_.emplace_back(values, out_schema);
      }
    }
  }
  if (out_result_.empty()) {
    return false;
  }
  *tuple = out_result_.back();
  out_result_.pop_back();
  return true;
}

}  // namespace bustub
