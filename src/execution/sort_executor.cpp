#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "binder/bound_order_by.h"
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  // define sort rules
  const auto &order_by = plan_->GetOrderBy();
  auto cmp = [order_by, this](const Tuple &tuple1, const Tuple &tuple2) {
    for (auto &order : order_by) {
      if (order.first == OrderByType::INVALID) {
        BUSTUB_ASSERT(false, "Invalid OrderByType");
      }
      if (order.first == OrderByType::ASC || order.first == OrderByType::DEFAULT) {
        const Value value1 = order.second->Evaluate(&tuple1, GetOutputSchema());
        const Value value2 = order.second->Evaluate(&tuple2, GetOutputSchema());
        if (value1.CompareLessThan(value2) == CmpBool::CmpTrue) {
          return true;
        }
        if (value1.CompareEquals(value2) == CmpBool::CmpTrue) {
          continue;
        }
        return false;
      }
      if (order.first == OrderByType::DESC) {
        const Value value1 = order.second->Evaluate(&tuple1, GetOutputSchema());
        const Value value2 = order.second->Evaluate(&tuple2, GetOutputSchema());
        if (value1.CompareGreaterThan(value2) == CmpBool::CmpTrue) {
          return true;
        }
        if (value1.CompareEquals(value2) == CmpBool::CmpTrue) {
          continue;
        }
        return false;
      }
    }
    return true;
  };
  std::sort(tuples_.begin(), tuples_.end(), cmp);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Emit the next tuple
  if (tuples_.empty()) {
    return false;
  }
  *tuple = tuples_.front();
  tuples_.erase(tuples_.begin());
  return true;
}

}  // namespace bustub
