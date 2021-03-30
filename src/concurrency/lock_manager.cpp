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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

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
