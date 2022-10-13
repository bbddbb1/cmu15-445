//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  Tuple left_tuple;
  RID left_id;
  left_child_->Init();
  right_child_->Init();
  while (left_child_->Next(&left_tuple, &left_id)) {
    auto value = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    JoinKey key{value};
    // std::vector<Value> values;
    // for (const auto &column : plan_->OutputSchema()->GetColumns()) {
    //   values.emplace_back(column.GetExpr()->Evaluate(&left_tuple_, plan_->GetLeftPlan()->OutputSchema()));
    // }
    if (hash_.count(key) > 0) {
      hash_[key].emplace_back(std::move(left_tuple));
    } else {
      std::vector<Tuple> temp = {left_tuple};
      hash_.emplace(key, std::move(temp));
    }
  }
  left_tuple_buffer_.clear();
  bucket_cur_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  if (bucket_cur_ >= left_tuple_buffer_.size()) {
    bool find = false;
    while (right_child_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema());
      JoinKey key{value};
      auto key_value = hash_.find(key);
      if (key_value != hash_.end()) {
        bucket_cur_ = 0;
        left_tuple_buffer_ = std::move(key_value->second);
        find = true;
        break;
      }
    }
    if (!find) {
      return false;
    }
  }
  std::vector<Value> values;
  //   Tuple temp(key_value->second, GetOutputSchema());
  for (const auto &column : plan_->OutputSchema()->GetColumns()) {
    values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple_buffer_[bucket_cur_],
                                                       plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                                       plan_->GetRightPlan()->OutputSchema()));
  }
  bucket_cur_++;
  *tuple = Tuple(values, GetOutputSchema());
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
