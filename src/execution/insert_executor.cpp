//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_metadata_ = catalog->GetTable(plan_->TableOid());
  table_indexes_ = catalog->GetTableIndexes(table_metadata_->name_);
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

void InsertExecutor::InsertTupleAndIndex(Tuple *tuple, RID *rid, Transaction *txn) {
  bool insert_tuple_suc = table_metadata_->table_->InsertTuple(*tuple, rid, txn);
  if (!insert_tuple_suc) {
    throw;
  }
  for (const auto &index_info : table_indexes_) {
    Index *index = index_info->index_.get();
    Tuple index_key(tuple->KeyFromTuple(table_metadata_->schema_, index_info->key_schema_, index->GetKeyAttrs()));
    index->InsertEntry(index_key, *rid, txn);
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    const auto &values = plan_->RawValues();
    for (auto &row : values) {
      *tuple = Tuple(row, &table_metadata_->schema_);
      InsertTupleAndIndex(tuple, rid, exec_ctx_->GetTransaction());
    }
  } else {
    while (child_executor_->Next(tuple, rid)) {
      InsertTupleAndIndex(tuple, rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
