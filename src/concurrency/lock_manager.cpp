//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <memory>
#include <mutex>
#include "common/logger.h"
#include "common/rid.h"
#include "stack"

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  CheckCommonLockMode(txn, lock_mode);

  LockMode cur_lock_mode;
  bool is_locked = IsLockedByTransaction(txn, cur_lock_mode, oid);

  if (!is_locked || cur_lock_mode == lock_mode) {
    return true;
  }

  if (is_locked && IsIncompatibleUpgrade(lock_mode, cur_lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }
  bool need_upgrade = is_locked;

  auto lock_request_que = GetLockRequestQueue(oid);
  std::unique_lock<std::mutex> lock(lock_request_que->latch_);

  // Check upgrade conflict
  if (need_upgrade && lock_request_que->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);

    lock.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // If lock request queue is empty, create a new request and grant the lock
  if (lock_request_que->request_queue_.empty()) {
    auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request_que->request_queue_.emplace_back(new_request);
    new_request->granted_ = true;
    InsertToTransactionLockSet(txn, new_request, true);
    LOG_INFO("Grant the first lock to txn %d", txn->GetTransactionId());

    return true;
  }

  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  // If need upgrade, set upgrading_ to txn_id and add upgrade request to the first non granted request position
  if (need_upgrade) {
    // If multiple transactions are upgrading, abort
    if (lock_request_que->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    lock_request_que->upgrading_ = txn->GetTransactionId();

    auto it = std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                           [](const auto &request) { return !request->granted_; });
    lock_request_que->request_queue_.emplace(it, new_request);

  } else {
    // If not need upgrade, add new request to the end of the queue
    lock_request_que->request_queue_.emplace_back(new_request);
  }

  // Check compatibility in Fifo order
  while (!CheckCompatibility(new_request, lock_request_que)) {
    // Check if the transaction is aborted
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_que->request_queue_.remove(new_request);
      return false;
    }
    lock_request_que->cv_.wait(lock);
  }

  // now we can grant the lock
  LOG_INFO("Grant lock to txn %d", txn->GetTransactionId());
  new_request->granted_ = true;
  InsertToTransactionLockSet(txn, new_request, true);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // First check if the txn holds the lock
  LockMode cur_lock_mode;
  bool is_locked = IsLockedByTransaction(txn, cur_lock_mode, oid);
  if (!is_locked) {
    // Abort the txn
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  CheckLockOnRow(txn, oid);

  auto lock_request_que = GetLockRequestQueue(oid);

  std::unique_lock<std::mutex> lock(lock_request_que->latch_);

  // Remove the lock from que and txn
  auto it = std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                         [txn](const auto &request) { return request->txn_id_ == txn->GetTransactionId(); });

  RemoveFromTransactionLockSet(txn, *it, true);
  lock_request_que->request_queue_.erase(it);
  lock.unlock();

  UpdateTranctionState(txn, cur_lock_mode);

  lock_request_que->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // If aquiring intention lock, abort
  if (IntentionLockRow(lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  /*While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table*/
  CheckTableLockWithRow(txn, lock_mode, oid);

  CheckCommonLockMode(txn, lock_mode);

  // Check if the transaction already holds a lock on the row
  LockMode cur_lock_mode;

  // If the current transaction already holds a lock on the row, upgrade the lock to the specified lock_mode(if
  // possible).
  bool is_locked = IsLockedByTransaction(txn, cur_lock_mode, oid, rid);
  // If cur_lock_mode is the same with lock_mode
  if (is_locked && cur_lock_mode == lock_mode) {
    return true;
  }

  // If IncompatibleUpgrade
  if (is_locked && IsIncompatibleUpgrade(lock_mode, cur_lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  bool need_upgrade = is_locked;

  auto lock_request_que = GetLockRequestQueue(rid);
  std::unique_lock<std::mutex> lock(lock_request_que->latch_);

  // Check upgrade conflict
  if (need_upgrade && lock_request_que->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);

    lock.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // If lock request queue is empty, create a new request and grant the lock
  if (lock_request_que->request_queue_.empty()) {
    auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request_que->request_queue_.emplace_back(new_request);
    new_request->granted_ = true;
    InsertToTransactionLockSet(txn, new_request, false);
    return true;
  }

  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  // If need upgrade, set upgrading_ to txn_id and add upgrade request to the first non granted request position
  if (need_upgrade) {
    // If multiple transactions are upgrading, abort
    if (lock_request_que->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    lock_request_que->upgrading_ = txn->GetTransactionId();

    auto it = std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                           [](const auto &request) { return !request->granted_; });
    lock_request_que->request_queue_.emplace(it, new_request);

  } else {
    // If not need upgrade, add new request to the end of the queue
    lock_request_que->request_queue_.emplace_back(new_request);
  }

  // Check compatibility in Fifo order
  while (!CheckCompatibility(new_request, lock_request_que)) {
    // Check if the transaction is aborted
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_que->request_queue_.remove(new_request);
      return false;
    }
    lock_request_que->cv_.wait(lock);
  }

  // now we can grant the lock
  new_request->granted_ = true;
  InsertToTransactionLockSet(txn, new_request, false);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // First check if the txn holds the lock
  LockMode cur_lock_mode;
  bool is_locked = IsLockedByTransaction(txn, cur_lock_mode, oid, rid);
  if (!is_locked) {
    // Abort the txn
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_que = GetLockRequestQueue(rid);

  std::unique_lock<std::mutex> lock(lock_request_que->latch_);

  // Remove the lock from que and txn
  auto it = std::find_if(lock_request_que->request_queue_.begin(), lock_request_que->request_queue_.end(),
                         [txn](const auto &request) { return request->txn_id_ == txn->GetTransactionId(); });

  RemoveFromTransactionLockSet(txn, *it, false);
  lock_request_que->request_queue_.erase(it);
  lock.unlock();

  UpdateTranctionState(txn, cur_lock_mode);

  lock_request_que->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock<std::mutex> lock(waits_for_latch_);
  // Add t1 -> t2 edge into the graph
  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock<std::mutex> lock(waits_for_latch_);
  // Assert that t1 -> t2 edge exists in the graph
  BUSTUB_ASSERT(std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2) != waits_for_[t1].end(),
                "t1 -> t2 edge does not exist in the graph.");

  // Remove t1 -> t2 edge from the graph
  waits_for_[t1].erase(std::remove(waits_for_[t1].begin(), waits_for_[t1].end(), t2), waits_for_[t1].end());

  if (waits_for_[t1].empty()) {
    waits_for_.erase(t1);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::scoped_lock<std::mutex> lock(waits_for_latch_);
  // If empty graph, no cycle
  if (waits_for_.empty()) {
    return false;
  }

  // Using DFS to detect cycle
  std::unordered_set<txn_id_t> visited;
  std::stack<txn_id_t> rec_stack;
  // Add first node to the stack
  rec_stack.push(waits_for_.begin()->first);
  while (!rec_stack.empty()) {
    txn_id_t curr = rec_stack.top();
    rec_stack.pop();
    // If the node is not visited, mark it as visited
    if (visited.find(curr) == visited.end()) {
      visited.insert(curr);
    }
    // If the node is visited, there is a cycle, return the newest txn_id
    else {
      // Find the newest txn_id in the cycle
      *txn_id = *std::max_element(visited.begin(), visited.end());
      return true;
    }
    // Add all the neighbour of the node to the stack
    for (auto neighbour : waits_for_[curr]) {
      rec_stack.push(neighbour);
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::scoped_lock<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &wait : waits_for_) {
    for (const auto &edge : wait.second) {
      edges.emplace_back(wait.first, edge);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

auto LockManager::IntentionLockRow(LockMode lock_mode) -> bool {
  // If aquiring intent lock, abort
  return lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
}

auto LockManager::LockOnSharedOnReadUnCommitted(Transaction *txn, LockMode lock_mode) -> bool {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           lock_mode == LockMode::SHARED;
  }
  return false;
}

auto LockManager::LockOnShrinking(Transaction *txn, LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      return true;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      return lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetLockRequestQueue(RID rid) -> std::shared_ptr<LockRequestQueue> {
  std::lock_guard<std::mutex> lock_guard(row_lock_map_latch_);
  return row_lock_map_[rid];
}

auto LockManager::GetLockRequestQueue(table_oid_t oid) -> std::shared_ptr<LockRequestQueue> {
  std::lock_guard<std::mutex> lock(table_lock_map_latch_);
  return table_lock_map_[oid];
}

auto LockManager::IsLockedByTransaction(Transaction *txn, LockMode &lock_mode, table_oid_t oid) -> bool {
  if (txn->IsTableExclusiveLocked(oid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->IsTableSharedLocked(oid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    lock_mode = LockMode::INTENTION_SHARED;
    return true;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::IsLockedByTransaction(Transaction *txn, LockMode &lock_mode, table_oid_t oid, RID rid) -> bool {
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->IsRowSharedLocked(oid, rid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }

  return false;
}

auto LockManager::IsIncompatibleUpgrade(LockMode lock_mode, LockMode current_lock_mode) -> bool {
  if (current_lock_mode == LockMode::INTENTION_SHARED) {
    return lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
           lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (current_lock_mode == LockMode::SHARED) {
    return lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE;
  }

  if (current_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return lock_mode != LockMode::EXCLUSIVE;
  }

  return true;
}

auto LockManager::InsertToTransactionLockSet(Transaction *txn, std::shared_ptr<LockRequest> lock_request, bool is_table)
    -> void {
  if (is_table) {
    switch (lock_request->lock_mode_) {
      case LockMode::INTENTION_SHARED: {
        auto intention_shared = txn->GetIntentionSharedTableLockSet();
        intention_shared->insert(lock_request->oid_);
        break;
      }

      case LockMode::SHARED: {
        auto shared = txn->GetSharedTableLockSet();
        shared->insert(lock_request->oid_);
        break;
      }

      case LockMode::EXCLUSIVE: {
        auto exclusive = txn->GetExclusiveTableLockSet();
        exclusive->insert(lock_request->oid_);
        break;
      }

      case LockMode::INTENTION_EXCLUSIVE: {
        auto intention_exclusive = txn->GetIntentionExclusiveTableLockSet();
        intention_exclusive->insert(lock_request->oid_);
        break;
      }

      case LockMode::SHARED_INTENTION_EXCLUSIVE: {
        auto shared_intention_exclusive = txn->GetSharedIntentionExclusiveTableLockSet();
        shared_intention_exclusive->insert(lock_request->oid_);
        break;
      }
      default:
        BUSTUB_ASSERT(false, "Unknown lock mode.");
        break;
    }
  } else {
    switch (lock_request->lock_mode_) {
      case LockMode::SHARED: {
        auto shared_ptr = txn->GetSharedRowLockSet();
        (*shared_ptr)[lock_request->oid_].insert(lock_request->rid_);
        break;
      }
      case LockMode::EXCLUSIVE: {
        auto exclusive_ptr = txn->GetExclusiveRowLockSet();
        (*exclusive_ptr)[lock_request->oid_].insert(lock_request->rid_);
        break;
      }
      default:
        BUSTUB_ASSERT(false, "Intention lock mode on rows");
        break;
    }
  }
}

auto LockManager::RemoveFromTransactionLockSet(Transaction *txn, std::shared_ptr<LockRequest> lock_request,
                                               bool is_table) -> void {
  if (is_table) {
    switch (lock_request->lock_mode_) {
      case LockMode::INTENTION_SHARED: {
        auto intention_shared = txn->GetIntentionSharedTableLockSet();
        intention_shared->erase(lock_request->oid_);
        break;
      }

      case LockMode::SHARED: {
        auto shared = txn->GetSharedTableLockSet();
        shared->erase(lock_request->oid_);
        break;
      }

      case LockMode::EXCLUSIVE: {
        auto exclusive = txn->GetExclusiveTableLockSet();
        exclusive->erase(lock_request->oid_);
        break;
      }

      case LockMode::INTENTION_EXCLUSIVE: {
        auto intention_exclusive = txn->GetIntentionExclusiveTableLockSet();
        intention_exclusive->erase(lock_request->oid_);
        break;
      }

      case LockMode::SHARED_INTENTION_EXCLUSIVE: {
        auto shared_intention_exclusive = txn->GetSharedIntentionExclusiveTableLockSet();
        shared_intention_exclusive->erase(lock_request->oid_);
        break;
      }
      default:
        BUSTUB_ASSERT(false, "Unknown lock mode.");
        break;
    }
  } else {
    switch (lock_request->lock_mode_) {
      case LockMode::SHARED: {
        auto shared_ptr = txn->GetSharedRowLockSet();
        (*shared_ptr)[lock_request->oid_].erase(lock_request->rid_);
        break;
      }
      case LockMode::EXCLUSIVE: {
        auto exclusive_ptr = txn->GetExclusiveRowLockSet();
        (*exclusive_ptr)[lock_request->oid_].erase(lock_request->rid_);
        break;
      }
      default:
        BUSTUB_ASSERT(false, "Intention lock mode on rows");
        break;
    }
  }
}

auto LockManager::CheckCompatibility(std::shared_ptr<LockRequest> lock_request,
                                     std::shared_ptr<LockRequestQueue> lock_request_queue) -> bool {
  BUSTUB_ASSERT(!lock_request_queue->request_queue_.empty(), "Lock request queue is empty.");
  for (auto &request : lock_request_queue->request_queue_) {
    // If the first ungranted lock request is the same as the lock request we are checking, then it is compatible.
    if (!request->granted_) {
      return request == lock_request;
    }
    if (!IsCompatible(lock_request->lock_mode_, request->lock_mode_)) {
      break;
    }
  }
  return false;
}

auto LockManager::IsCompatible(LockMode lock_mode, LockMode current_lock_mode) -> bool {
  if (lock_mode == LockMode::INTENTION_SHARED) {
    return current_lock_mode == LockMode::INTENTION_SHARED || current_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || current_lock_mode == LockMode::SHARED;
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return current_lock_mode == LockMode::INTENTION_EXCLUSIVE || current_lock_mode == LockMode::INTENTION_SHARED;
  }
  if (lock_mode == LockMode::SHARED) {
    return current_lock_mode == LockMode::INTENTION_SHARED || current_lock_mode == LockMode::SHARED;
  }
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return current_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  return false;
}

auto LockManager::CheckCommonLockMode(Transaction *txn, LockMode lock_mode) -> void {
  /* First we check if we can grant the lock*/
  IsolationLevel level = txn->GetIsolationLevel();
  switch (level) {
    case IsolationLevel::REPEATABLE_READ:
      // No locks allowed in shrink phase
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;

    case IsolationLevel::READ_COMMITTED:
      // Only IS,S locks are allowed in shrink phase
      if (txn->GetState() == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;

    case IsolationLevel::READ_UNCOMMITTED:
      // The transaction is required to take only IX, X locks.
      // X, IX locks are allowed in the GROWING state.S, IS, SIX locks are never allowed
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      } else {
        if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
      }
      break;
  }
}

void LockManager::CheckTableLockWithRow(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (txn->IsTableExclusiveLocked(oid)) {
      return;
    }
    if (txn->IsTableIntentionExclusiveLocked(oid)) {
      return;
    }
    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return;
    }
    // throw exception
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  if (lock_mode == LockMode::SHARED) {
    if (txn->IsTableExclusiveLocked(oid)) {
      return;
    }
    if (txn->IsTableIntentionExclusiveLocked(oid)) {
      return;
    }
    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return;
    }
    if (txn->IsTableSharedLocked(oid)) {
      return;
    }
    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return;
    }
    // throw exception
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // code should not reach here
  assert(false);
}

auto LockManager::IsHodingLock(Transaction *txn, table_oid_t oid, LockMode &lock_mode) -> bool {
  if (txn->IsTableExclusiveLocked(oid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->IsTableSharedLocked(oid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::IsHodingLock(Transaction *txn, table_oid_t oid, RID rid, LockMode &lock_mode) -> bool {
  if (txn->IsTableExclusiveLocked(oid)) {
    lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::INTENTION_EXCLUSIVE;
    assert(false);
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    assert(false);
  }
  if (txn->IsTableSharedLocked(oid)) {
    lock_mode = LockMode::SHARED;
    return true;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    assert(false);
  }
  return false;
}
void LockManager::UpdateTranctionState(Transaction *txn, LockMode lock_mode) {
  IsolationLevel level = txn->GetIsolationLevel();
  switch (level) {
    case IsolationLevel::REPEATABLE_READ:
      // No locks allowed in shrink phase
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::READ_UNCOMMITTED:
      // Unlock X locks should set the state to shrink
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }

      break;
  }
}
}  // namespace bustub