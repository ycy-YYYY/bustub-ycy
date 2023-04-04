//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  inner_table_ = &exec_ctx_->GetCatalog()->GetTable(plan->GetInnerTableOid())->table_;
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }
auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // For each tuple in the outer table, we need to find the matching tuple in the inner table with the index.
  Tuple left_tuple;
  RID left_rid;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    auto index_key = plan_->KeyPredicate()->Evaluate(&left_tuple, plan_->GetChildPlan()->OutputSchema());
    if (index_key.IsNull()) {
      continue;
    }
    // construct the index probe key by using key_predicate
    std::vector<Value> index_probe_key;
    index_probe_key.emplace_back(index_key);
    Tuple key = Tuple(index_probe_key, &index_info_->key_schema_);
    // Scan through the index to find the matching tuple
    std::vector<RID> rids;
    index_info_->index_->ScanKey(key, &rids, exec_ctx_->GetTransaction());

    // For each rid, find the tuple from table_heap and then construct the output tuple
    // Since there are no duplicates in the index, rids.size() should be 0 or 1
    if (!rids.empty()) {
      BUSTUB_ASSERT(rids.size() == 1, "There should be no duplicates in the index");
      Tuple right_tuple;
      bool found = inner_table_->get()->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
      BUSTUB_ASSERT(found, "The tuple should be found in the table");
      std::vector<Value> values;
      // construct the output tuple by concatenating the left and right tuple
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;
    }

    // If there is no matching tuple in the inner table, we need to check the join type
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      // construct the output tuple by concatenating the left tuple and null values
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      return true;
    }
  }

  return false;
}

}  // namespace bustub
