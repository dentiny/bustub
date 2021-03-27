//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  const AbstractExpression *predicate_;

  // IndexInfo and TableMetadata defined at catalog.h could get access to any member needed.
  IndexInfo *index_info_;
  TableMetadata *table_meta_;

  // Workarounbd: explicit instantiation here doesn't work for all types of index. All instantiations see below:
  // template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
  // template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
  // template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
  // template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
  // template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> it_;
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> end_it_;
};
}  // namespace bustub
