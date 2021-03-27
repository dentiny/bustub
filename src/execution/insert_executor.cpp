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
                               std::unique_ptr<AbstractExecutor> &&child_executor) :
  AbstractExecutor(exec_ctx),
  plan_(plan),
  transaction_(exec_ctx->GetTransaction()),
  table_metadata_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
  schema_(table_metadata_->schema_),
  table_heap_(table_metadata_->table_.get()),
  child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {}

// Insert doesn't conform with Iterator Model, return false after execution.
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool insert_suc = true;
  Tuple tuple_to_insert;
  if (plan_->IsRawInsert()) {
    auto& values = plan_->RawValues();
    for (auto& row : values) {
      tuple_to_insert = Tuple(row, &schema_);
      insert_suc = table_heap_->InsertTuple(tuple_to_insert, rid, transaction_);
      if (!insert_suc) {
        throw;
      }
    }
  } else {
    while (child_executor_->Next(&tuple_to_insert, rid)) {
      insert_suc = table_heap_->InsertTuple(tuple_to_insert, rid, transaction_);
      if (!insert_suc) {
        throw;
      }
    }
  }
  return false;
}

}  // namespace bustub
