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
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"

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
  Tuple tuple{};
  RID rid{};
  while (child_->Next(&tuple, &rid)) {
    is_empty_ = false;
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // if table is empty, initial aggregrate value
  if (is_empty_) {
    if (plan_->GetGroupBys().empty()) {
      auto value = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(value.aggregates_, &plan_->OutputSchema());
      is_empty_ = false;
      return true;
    }
    return false;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  auto key = aht_iterator_.Key();
  auto value = aht_iterator_.Val();
  key.group_bys_.insert(key.group_bys_.end(), value.aggregates_.begin(), value.aggregates_.end());
  *tuple = Tuple(key.group_bys_, &plan_->OutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
