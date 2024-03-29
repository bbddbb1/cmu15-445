//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      iter_(table_info_->table_->Begin(exec_ctx_->GetTransaction())),
      table_end_(table_info_->table_->End()) {}

void SeqScanExecutor::Init() { iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != table_end_) {
    auto *txn = exec_ctx_->GetTransaction();
    auto isolation_level = txn->GetIsolationLevel();
    auto *lock_manager = exec_ctx_->GetLockManager();
    if (isolation_level != IsolationLevel::READ_UNCOMMITTED && !lock_manager->LockShared(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
    auto temp = iter_++;
    if (plan_->GetPredicate() != nullptr &&
        !plan_->GetPredicate()->Evaluate(&(*temp), &table_info_->schema_).GetAs<bool>()) {
      continue;
    }
    std::vector<Value> values;
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      values.emplace_back(column.GetExpr()->Evaluate(&(*temp), &table_info_->schema_));
    }
    *tuple = Tuple(values, GetOutputSchema());
    *rid = temp->GetRid();
    if (isolation_level == IsolationLevel::READ_COMMITTED && !lock_manager->Unlock(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
    return true;
  }
  return false;
}

}  // namespace bustub
