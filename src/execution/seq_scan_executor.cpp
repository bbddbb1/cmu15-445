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
: AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
    iter_(table_info_->table_->Begin(exec_ctx_->GetTransaction())), table_end_(table_info_->table_->End()) {}

void SeqScanExecutor::Init() {
    for(auto column : plan_->OutputSchema()->GetColumns()){
        ColIdx_.emplace_back(plan_->OutputSchema()->GetColIdx(column.GetName()));
    }
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    for(; iter_ != table_end_; iter_++){
        auto temp = iter_++;
        if(plan_->GetPredicate() != nullptr){
            if (plan_->GetPredicate()->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()){
                std::vector<Value> values;
                for(auto colidx : ColIdx_){
                    values.emplace_back(temp->GetValue(&table_info_->schema_, colidx));
                }
                *tuple = Tuple(values, GetOutputSchema());
                *rid = tuple->GetRid();
                return true;
            }
        }else{
            throw Exception("predicate not exist");
        }
    }
    return false;
}

}  // namespace bustub
