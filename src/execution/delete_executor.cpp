//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_info_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  if (child_executor_->Next(tuple, rid)) {
    auto *txn = exec_ctx_->GetTransaction();
    auto *lock_manager = exec_ctx_->GetLockManager();    
    if (txn->IsSharedLocked(*rid)) {
      if (!lock_manager->LockUpgrade(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    } else {
      if (!lock_manager->LockExclusive(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    }
    if (table_info_->table_->MarkDelete(*rid, txn)) {
      for (auto index_info : index_info_) {
        auto key =
            tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key, *rid, txn);
        txn->AppendIndexWriteRecord(IndexWriteRecord{*rid, table_info_->oid_, WType::DELETE, *tuple, Tuple{},
                                                     index_info->index_oid_, exec_ctx_->GetCatalog()});
      }
      return true;
    }
    throw Exception("delete err");
  }
  return false;
}

}  // namespace bustub
