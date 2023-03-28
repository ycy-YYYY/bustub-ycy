//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  //   auto table_id = plan_->GetTableOid();
  //   auto table_info = exec_ctx_->GetCatalog()->GetTable(table_id);
  //   table_heap_ = &table_info->table_;
  //   cur_it_ = std::make_unique<TableIterator>((*table_heap_)->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //   if (*cur_it_ == (*table_heap_)->End()) {
  //     return false;
  //   }
  //   BUSTUB_ASSERT(tuple != nullptr && rid != nullptr, "Parameter can not be null");
  //   *tuple = *(*cur_it_);
  //   ++(*cur_it_);
  //   *rid = tuple->GetRid();
  //   return true;
  return true;
}

}  // namespace bustub
