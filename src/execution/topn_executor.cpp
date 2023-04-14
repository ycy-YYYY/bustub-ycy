#include "execution/executors/topn_executor.h"
#include <algorithm>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // Get the tuples from the child executor
  Tuple tuple;
  RID rid;
  child_executor_->Init();

  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  // define sort rules
  const auto &order_by = plan_->GetOrderBy();
  cmp_func_ = [order_by, this](const Tuple &tuple1, const Tuple &tuple2) {
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

  // Heapfy the tuples, make min heap if defalut ASC, max heap if DESC
  std::make_heap(tuples_.begin(), tuples_.end(),
                 [this](const Tuple &tuple1, const Tuple &tuple2) { return !cmp_func_(tuple1, tuple2); });
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Emit the next tuple
  if (tuples_.empty()) {
    return false;
  }
  // Pop the top tuple when Emitted tuple number < N
  if (num_tuples_yielded_ < plan_->GetN()) {
    std::pop_heap(tuples_.begin(), tuples_.end(),
                  [this](const Tuple &tuple1, const Tuple &tuple2) { return !cmp_func_(tuple1, tuple2); });
    *tuple = tuples_.back();
    tuples_.pop_back();
    num_tuples_yielded_++;
    return true;
  }

  return false;
}

}  // namespace bustub
