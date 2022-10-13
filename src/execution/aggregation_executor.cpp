//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (aht_iterator_ != aht_.End()) {
    auto temp = aht_iterator_;
    ++aht_iterator_;
    if (plan_->GetHaving() != nullptr &&
        !plan_->GetHaving()->EvaluateAggregate(temp.Key().group_bys_, temp.Val().aggregates_).GetAs<bool>()) {
      continue;
    }

    std::vector<Value> values;
    for (auto &column : plan_->OutputSchema()->GetColumns()) {
      values.emplace_back(column.GetExpr()->EvaluateAggregate(temp.Key().group_bys_, temp.Val().aggregates_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    *rid = tuple->GetRid();
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
