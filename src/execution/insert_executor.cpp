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
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto table_id = plan_->TableOid();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(table_id);
  auto table_name = table_info->name_;
  index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  table_heap_ = &table_info->table_;
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ASSERT(rid != nullptr, "Null pointer exception");

  if (!state_) {
    return false;
  }
  int count = 0;

  while (child_executor_->Next(tuple, rid)) {
    ++count;
    // insert tuple
    (*table_heap_)->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    // update index
    for (auto index_info : index_info_vec_) {
      auto index_key_attrs = index_info->index_->GetKeyAttrs();
      auto schema = child_executor_->GetOutputSchema();
      auto key_schema = index_info->index_->GetKeySchema();
      auto key_tulpe = tuple->KeyFromTuple(schema, *key_schema, index_key_attrs);
      index_info->index_->InsertEntry(key_tulpe, *rid, exec_ctx_->GetTransaction());
    }
  }

  Tuple res(std::vector<Value>{{INTEGER, count}}, &plan_->OutputSchema());
  *tuple = res;
  state_ = false;
  return true;
}

}  // namespace bustub
