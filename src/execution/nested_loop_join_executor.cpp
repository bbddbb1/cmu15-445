//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&outer_tuple_, &outer_id_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple inner_tuple;
  RID inner_id;
	for(;;){
		if (!right_executor_->Next(&inner_tuple, &inner_id)) {
			if (!left_executor_->Next(&outer_tuple_, &outer_id_)) return false;
			right_executor_->Init();
			assert(right_executor_->Next(&inner_tuple, &inner_id));
		}
		if (plan_->Predicate() != nullptr && !plan_->Predicate()
																							->EvaluateJoin(&outer_tuple_, plan_->GetLeftPlan()->OutputSchema(),
																														&inner_tuple, plan_->GetRightPlan()->OutputSchema())
																							.GetAs<bool>()) {
			continue;
		}
		std::vector<Value> values;
		for (const auto &column : plan_->OutputSchema()->GetColumns()) {
			values.emplace_back(column.GetExpr()->EvaluateJoin(&outer_tuple_, plan_->GetLeftPlan()->OutputSchema(),
																												&inner_tuple, plan_->GetRightPlan()->OutputSchema()));
		}
		*tuple = Tuple(values, GetOutputSchema());
		*rid = tuple->GetRid();
		return true;		
	}
}

}  // namespace bustub
