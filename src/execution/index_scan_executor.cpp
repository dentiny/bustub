//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      predicate_(plan_->GetPredicate()),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_meta_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      // Note:
      // (1) dynamic_cast<> only works for base class pointer/reference to derived class's, otherwise return nullptr.
      // (2) reinterpret_cast<>, widely used at b plus tree to cast page->GetData()(char*) to BPlusTreePage*,
      // is for casting between irrelative types.
      it_(dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())
              ->GetBeginIterator()),
      end_it_(dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())
                  ->GetEndIterator()) {}

void IndexScanExecutor::Init() {}

// Note:
// (1) Index scan is completed by BPlusTreeIndex.
// (2) (*it) get MappingType, for here is std::pair<GenericKey<8>, RID>.
// (3) Next supports both point query and range scan, which is completed by predicate.
// (4) Return value:
//      - bool: whether needs to call Next(keep iteration) again.
//      - tuple, rid: tuple content and record, which could be used as child_executor in InsertPlan and DeletePlan.
bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  while (it_ != end_it_) {
    *rid = (*it_).second;
    ++it_;
    table_meta_->table_->GetTuple(*rid, &cur_tuple, exec_ctx_->GetTransaction());
    if (predicate_ == nullptr || predicate_->Evaluate(&cur_tuple, &table_meta_->schema_).GetAs<bool>()) {
      const Schema *schema = plan_->OutputSchema();
      const auto &columns = schema->GetColumns();
      uint32_t column_count = schema->GetColumnCount();
      std::vector<Value> values(column_count);
      for (uint32_t ii = 0; ii < column_count; ++ii) {
        values[ii] = columns[ii].GetExpr()->Evaluate(&cur_tuple, &table_meta_->schema_);
      }
      *tuple = Tuple(values, schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
