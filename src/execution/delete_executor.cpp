//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// Note: insert could be RawInsert, while delete has to be coupled with quries.
void DeleteExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_meta_ = catalog->GetTable(plan_->TableOid());
  table_indexes_ = catalog->GetTableIndexes(table_meta_->name_);
  assert(child_executor_ != nullptr);
  child_executor_->Init();
}

// (1) Delete tuple via MarkDelete(record id), which will be applied when transaction commits.
// (2) Delete index related to this record id.
bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    if (table_meta_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      for (const auto &index_info : table_indexes_) {
        Index *index = index_info->index_.get();
        Tuple index_key(tuple->KeyFromTuple(table_meta_->schema_, index_info->key_schema_, index->GetKeyAttrs()));
        index->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
    throw;
  }
  return false;
}
}  // namespace bustub
