//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
    table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())), 
    index_info_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)){}

void InsertExecutor::Init() {
	if(!plan_->IsRawInsert()){
		child_executor_->Init();
  }else{
		iter_ = plan_->RawValues().begin();
	}
}
void InsertExecutor::Insert(Tuple *tuple, RID *rid){
	TableHeap *table_heap = table_info_->table_.get();
	table_heap->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
	for (auto &index_info: index_info_) {
		// HASH_TABLE_INDEX_TYPE *hash_index = reinterpret_cast<HASH_TABLE_INDEX_TYPE*>(index_info->index_.get());
		index_info->index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
			*rid, exec_ctx_->GetTransaction());
	}
}
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
	if (!plan_->IsRawInsert()){
		if (child_executor_->Next(tuple, rid)){
			Insert(tuple, rid);
			return true;
		}
		return false;
	}
	if (iter_ != plan_->RawValues().end()){
		*tuple = Tuple(*iter_++, &table_info_->schema_);
		Insert(tuple, rid);
		return true;
	}
	return false;
}

}  // namespace bustub
