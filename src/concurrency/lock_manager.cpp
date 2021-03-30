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

#include "concurrency/lock_manager.h"

#include <iostream>
#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  return LockImpl(txn, rid, LockMode::SHARED, false /* is_upgrading */);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  return LockImpl(txn, rid, LockMode::EXCLUSIVE, false /* is_upgrading */);
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  return LockImpl(txn, rid, LockMode::EXCLUSIVE, true /* is_upgrading */);
}

bool LockManager::LockImpl(Transaction *txn, const RID &rid, LockMode lock_mode, bool is_upgrading) {
  // Check whether to abort the transaction.
  // (1) transaction has to be at GROWING state
  // (2) if lock_mode is upgrading
  // 1/ LockRequestQueue should have no other upgrading requests
  // 2/ the txn should be holding shared lock
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // Acquire lock on RID granularity.
  std::unique_lock<std::mutex> lock_table_latch(latch_);
  LockRequestQueue& lock_request_queue = lock_table_[rid];
  std::unique_lock<std::mutex> request_queue_lock(lock_request_queue.latch_);
  lock_table_latch.unlock();

  if (is_upgrading) {
    if (!lock_request_queue.CheckUpgradeAndRemoveLock(txn)) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    txn->RemoveSharedLock(rid);
  }

  // Currently the txn is guarenteed to hold no lock on the RID.
  lock_request_queue.GrantLock(txn, rid, lock_mode, is_upgrading, &request_queue_lock);
  return true;
}

// For 2PL, locks can be acquired at GRWOING state, and released at SHRINKING/COMMITED/ABORTED state.
// For strict 2PL, exclusive locks can only be released at COMMITED/ABORTED state to avoud cascading aborts.
// Note: TransactionManager will call ReleaseLocks() at Commit() and Abort() method.
// (1) Check whether transaction could unlock on RID, update its state.
// (2) Remove lock on LockRequestQueue and Transaction.
// (3) Check whether the released lock can be granted to other transactions.
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // Acquire lock on RID granularity.
  std::unique_lock<std::mutex> lock_table_latch(latch_);
  LockRequestQueue& lock_request_queue = lock_table_[rid];
  std::list<LockRequest>& request_queue = lock_request_queue.request_queue_;
  std::unique_lock<std::mutex> request_queue_lock(lock_request_queue.latch_);

  auto it = std::find_if(request_queue.begin(), request_queue.end(),
    [txn](const LockRequest& lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  if (it == request_queue.end()) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // Check and update transaction state.
  LockMode lock_mode = it->lock_mode_;
  TransactionState txn_state = txn->GetState();
  if (lock_mode == LockMode::EXCLUSIVE) {  // strict 2PL can only unlock at COMMIT or ABORTED state
    if (txn_state != TransactionState::COMMITTED && txn_state != TransactionState::ABORTED) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  } else if (lock_mode == LockMode::SHARED) {
    if (txn_state == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  // Remove lock on Transaction.
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->RemoveExclusiveLock(rid);
    --lock_request_queue.exclusive_count;
  } else if (lock_mode == LockMode::SHARED) {
    txn->RemoveSharedLock(rid);
    --lock_request_queue.shared_count;
  }

  // Remove lock on LockRequestQueue.
  request_queue.erase(it);
  if (request_queue.empty()) {
    lock_table_.erase(rid);
    return true;
  }
  lock_table_latch.unlock();

  // Check whether the released lock can be granted to other transactions.
  // After the unlock operation, it's guarenteed that there's no exclusive lock granted in the lock_request_queue.
  assert(lock_request_queue.exclusive_count == 0);
  for (auto& lock_request : request_queue) {
    if (lock_request.granted_) {
      continue;
    }
    LockMode cur_lock_mode = lock_request.lock_mode_;
    if (cur_lock_mode == LockMode::EXCLUSIVE && lock_request_queue.shared_count == 0) {
      lock_request.granted_ = true;
      ++lock_request_queue.exclusive_count;
      break;
    } else if (cur_lock_mode == LockMode::SHARED) {
      lock_request.granted_ = true;
      ++lock_request_queue.shared_count;
    }
  }
  lock_request_queue.cv_.notify_all();
  return true;
}

// Adds an edge in graph from t1 to t2(t2 waits for t1). If the edge already exists, nothing will be done.
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lck(waits_for_latch_);
  auto& wait_txns = waits_for_[t1];
  auto it = std::find_if(wait_txns.begin(), wait_txns.end(), [t2](const txn_id_t txn_id){ return txn_id == t2; });
  if (it != wait_txns.end()) {
    return;
  }
  wait_txns.push_back(t2);
}

// Remove an edge in graph from t1 to t2(t2 waits for t1). If the edge doesn't exists, nothing will be done.
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lck(waits_for_latch_);
  auto& wait_txns = waits_for_[t1];
  auto it = std::find_if(wait_txns.begin(), wait_txns.end(), [t2](const txn_id_t txn_id){ return txn_id == t2; });
  if (it == wait_txns.end()) {
    return;
  }
  wait_txns.erase(it);
  if (wait_txns.empty()) {
    waits_for_.erase(t1);
  }
}

// waits_for_latch_ is guarenteed to be acquired here.
// @return true for cycle exists, false for no cycle.
bool LockManager::CycleDetectImpl(txn_id_t txn, const std::unordered_set<txn_id_t>& visited, txn_id_t *txn1, txn_id_t *txn2) {
  if (waits_for_.find(txn) == waits_for_.end()) {
    return false;
  }
  const auto& wait_txns = waits_for_[txn];
  for (txn_id_t wait_txn : wait_txns) {
    if (visited.find(wait_txn) != visited.end()) {
      *txn1 = txn;
      *txn2 = wait_txn;
      return true;
    }

    std::unordered_set<txn_id_t> new_visited = visited;
    new_visited.insert(wait_txn);
    bool cycle_detected = CycleDetectImpl(wait_txn, new_visited, txn1, txn2);
    if (cycle_detected) {
      return true;
    }
  }
  return false;
}

// For efficiency consideration, return and abort the youngest transaction.
bool LockManager::HasCycle(txn_id_t *txn_id) {
  txn_id_t txn1, txn2;  // two inter-dependent transactions
  std::unique_lock<std::mutex> lck(waits_for_latch_);
  for (const auto& wait_txn_vec : waits_for_) {
    txn_id_t cur_txn = wait_txn_vec.first;
    bool cycle_detected = CycleDetectImpl(cur_txn, {} /* visited */, &txn1, &txn2);
    if (cycle_detected) {
      *txn_id = std::max(txn1, txn2);
      return true;
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> waits_for_pair;
  std::unique_lock<std::mutex> lck(waits_for_latch_);
  for (auto& [txn, wait_txns] : waits_for_) {
    for (txn_id_t wait_txn : wait_txns) {
      waits_for_pair.emplace_back(txn, wait_txn);
    }
  }
  return waits_for_pair;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
