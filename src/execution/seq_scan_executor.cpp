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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
}

void SeqScanExecutor::Init() {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    auto table_end = table_info_->table_->End();
    for(; iter_ != table_end; iter_++){
        *tuple = *iter_;
        *rid = tuple->GetRid();
        if(plan_->GetPredicate() != nullptr){
            if (plan_->GetPredicate()->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()){
                iter_++;
                return true;
            }
        }else{
            iter_++;
            return true;
        }
    }
    return false;
}

}  // namespace bustub
