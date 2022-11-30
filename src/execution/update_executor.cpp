//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)),
      index_info_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    Tuple updated_tuple = GenerateUpdatedTuple(*tuple);
    if (table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction())) {
      for (auto index_info : index_info_) {
        auto new_key = updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                  index_info->index_->GetKeyAttrs());
        auto old_key =
            tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
        index_info->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());

        // txn->AppendIndexWriteRecord({IndexWriteRecord{*rid, table_info_->oid_, WType::UPDATE, updated_tuple, *tuple,
        //                                               index_info->index_oid_, exec_ctx_->GetCatalog()}});
        auto undo_index_rec = IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, updated_tuple,
                                               index_info->index_oid_, exec_ctx_->GetCatalog());
        undo_index_rec.old_tuple_ = *tuple;
        txn->GetIndexWriteSet()->emplace_back(undo_index_rec);
        // txn->GetIndexWriteSet()->emplace_back(*rid, table_info_->oid_, WType::UPDATE, updated_tuple, *tuple,
        //                                       index_info->index_oid_, exec_ctx_->GetCatalog());
      }
      return true;
    }
    throw Exception("update err");
  }
  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
