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
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      predicate_(plan->Predicate()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  // buffer right tuples
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }
  right_tuple_idx_ = right_tuples_.size();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // if idx = size, init left tuple
  if (right_tuple_idx_ == right_tuples_.size()) {
    if (!left_executor_->Next(&left_tuple_, rid)) {
      return false;
    }
    
    left_join_match_ = false;
    right_tuple_idx_ = 0;
  }

  while (true) {
    auto left_schema = left_executor_->GetOutputSchema();
    auto right_schema = right_executor_->GetOutputSchema();
    // if idx < size, then we can use the buffered right tuples
    for (; right_tuple_idx_ < right_tuples_.size(); right_tuple_idx_++) {
      auto right_tuple = right_tuples_[right_tuple_idx_];
      auto predicate_result = predicate_.EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
      if (!predicate_result.IsNull() && predicate_result.GetAs<bool>()) {
        std::vector<Value> output_values;
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          output_values.push_back(left_tuple_.GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          output_values.push_back(right_tuple.GetValue(&right_schema, i));
        }
        *tuple = Tuple(output_values, &GetOutputSchema());
        left_join_match_ = true;
        right_tuple_idx_++;
        return true;
      }
    }

    // If no match in right table, consider left join
    if (plan_->GetJoinType() == JoinType::LEFT && !left_join_match_ ) {
      std::vector<Value> output_values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        output_values.emplace_back(left_tuple_.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        output_values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      *tuple = Tuple(output_values, &GetOutputSchema());
      return true;
    }

    // if idx == size, then we need to fetch a new left tuple

    if (!left_executor_->Next(&left_tuple_, rid)) {
      return false;
    }
    left_join_match_ = false;
    right_tuple_idx_ = 0;
  }
}

}  // namespace bustub
