//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  // Note:
  // (1) Insert does not make use of the tuple pointer being passed in.
  // (2) Insert doesn't conform with Iterator Model, return false after execution.
  bool Next([[maybe_unused]] Tuple *tuple, RID *rid) override;

 private:
  void InsertTupleAndIndex(Tuple *tuple, RID *rid, Transaction *txn);

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  TableMetadata *table_metadata_;
  std::vector<IndexInfo *> table_indexes_;
  const std::unique_ptr<AbstractExecutor> child_executor_;
};
}  // namespace bustub
