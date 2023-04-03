//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // Fetch index object from catlog
  auto index_oid = plan_->GetIndexOid();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(index_oid);  // Get table_heap from catlog from index->info
  auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  table_heap_ = &table_info->table_;
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_.IsEnd()) {
    return false;
  }
  *rid = (*iterator_).second;
  ++iterator_;
  // Retrive tuple according to rid
  bool res = (*table_heap_)->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  BUSTUB_ASSERT(res, "There is no such tuple for given index");
  return true;
}

}  // namespace bustub
