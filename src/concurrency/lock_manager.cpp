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

#include <iostream>
#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

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
  LockRequestQueue &lock_request_queue = lock_table_[rid];
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
  if (lock_table_.find(rid) == lock_table_.end()) {
    return false;
  }
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_request_queue.request_queue_;
  std::unique_lock<std::mutex> request_queue_lock(lock_request_queue.latch_);

  auto it = std::find_if(request_queue.begin(), request_queue.end(), [txn](const LockRequest &lock_request) {
    return txn->GetTransactionId() == lock_request.txn_id_;
  });
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

  bool no_lock_reschedule = UnlockImpl(txn, rid, &it, &lock_table_latch, true /* is_holding */);
  if (no_lock_reschedule) {
    return true;
  }

  lock_request_queue.cv_.notify_all();
  return true;
}

// It's guarenteed that global latch_ is acquired when invoked.
// @arg lck: if invoked by Unlock() method could unlock global latch.
// @arg is_holding: if the transaction is holding the lock to be unlocked.
// @return: true for RID-corresponding lock request queue has been empty, no need to reschedule lock grant.
bool LockManager::UnlockImpl(Transaction *txn, const RID &rid,
                             std::list<LockRequest>::iterator *lock_request_queue_iter,
                             std::unique_lock<std::mutex> *lck, bool is_holding) {
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_request_queue.request_queue_;

  // Remove lock on Transaction.
  LockMode lock_mode = (*lock_request_queue_iter)->lock_mode_;
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->RemoveExclusiveLock(rid);
    if (is_holding) {
      --lock_request_queue.exclusive_count;
    }
  } else if (lock_mode == LockMode::SHARED) {
    txn->RemoveSharedLock(rid);
    if (is_holding) {
      --lock_request_queue.shared_count;
    }
  }

  // Remove lock on LockRequestQueue.
  request_queue.erase(*lock_request_queue_iter);
  if (request_queue.empty()) {
    lock_table_.erase(rid);
    return true;
  }

  // If the transaction is not holding the lock, there's no reason to do rescheduling.
  if (!is_holding) {
    return true;
  }

  // It's acceptable for unlock for RID-granularity Unlock(), while RunCycleDetection() has to hold until completion.
  if (lck != nullptr) {
    lck->unlock();
  }

  // Check whether the released lock can be granted to other transactions.
  // After the unlock operation, it's guarenteed that there's no exclusive lock granted in the lock_request_queue.
  assert(lock_request_queue.exclusive_count == 0);
  for (auto &lock_request : request_queue) {
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

  return false;
}

// Adds an edge in graph from t1 to t2(t2 waits for t1). If the edge already exists, nothing will be done.
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &wait_txns = waits_for_[t1];
  auto it = std::find_if(wait_txns.begin(), wait_txns.end(), [t2](const txn_id_t txn_id) { return txn_id == t2; });
  if (it != wait_txns.end()) {
    return;
  }
  wait_txns.push_back(t2);
}

// Remove an edge in graph from t1 to t2(t2 waits for t1). If the edge doesn't exists, nothing will be done.
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &wait_txns = waits_for_[t1];
  auto it = std::find_if(wait_txns.begin(), wait_txns.end(), [t2](const txn_id_t txn_id) { return txn_id == t2; });
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
bool LockManager::CycleDetectImpl(txn_id_t txn, const std::unordered_set<txn_id_t> &visited, txn_id_t *txn1,
                                  txn_id_t *txn2) {
  if (waits_for_.find(txn) == waits_for_.end()) {
    return false;
  }
  const auto &wait_txns = waits_for_[txn];
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
  txn_id_t txn1;
  txn_id_t txn2;
  for (const auto &wait_txn_vec : waits_for_) {
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
  for (auto &[txn, wait_txns] : waits_for_) {
    for (txn_id_t wait_txn : wait_txns) {
      waits_for_pair.emplace_back(txn, wait_txn);
    }
  }
  return waits_for_pair;
}

std::unordered_map<txn_id_t, std::vector<std::pair<RID, bool>>> LockManager::ReconstructWaitForGraph() {
  waits_for_.clear();
  std::unordered_map<txn_id_t, std::vector<std::pair<RID, bool>>> txns;

  for (const auto &rid_request_queue : lock_table_) {
    const RID &rid = rid_request_queue.first;
    const LockRequestQueue &lock_request_queue = rid_request_queue.second;
    std::vector<std::pair<txn_id_t, LockMode>> granted_txns;
    std::vector<std::pair<txn_id_t, LockMode>> waiting_txns;
    for (const LockRequest &lock_request : lock_request_queue.request_queue_) {
      if (lock_request.granted_) {
        granted_txns.emplace_back(lock_request.txn_id_, lock_request.lock_mode_);
      } else {
        waiting_txns.emplace_back(lock_request.txn_id_, lock_request.lock_mode_);
      }
    }

    // Assert lock granting validility.
    assert(!granted_txns.empty());
    if (granted_txns.size() == 1 && granted_txns[0].second == LockMode::EXCLUSIVE) {
      // nothing to assert
    } else if (granted_txns.size() == 1 && granted_txns[0].second == LockMode::SHARED) {
      if (!waiting_txns.empty()) {
        for (const auto &txn_id_lock_mode : waiting_txns) {
          LockMode lock_mode = txn_id_lock_mode.second;
          assert(lock_mode != LockMode::SHARED);
        }
      }
    } else {
      for (const auto &txn_id_lock_mode : granted_txns) {
        LockMode lock_mode = txn_id_lock_mode.second;
        assert(lock_mode == LockMode::SHARED);
      }
      if (!waiting_txns.empty()) {
        for (const auto &txn_id_lock_mode : waiting_txns) {
          LockMode lock_mode = txn_id_lock_mode.second;
          assert(lock_mode != LockMode::SHARED);
        }
      }
    }

    // (1) Add all transactions into maps, used when aborting transactions.
    // (2) Add dependent transaction pairs into wait-for graph.
    for (const auto &granted_pair : granted_txns) {
      for (const auto &waiting_pair : waiting_txns) {
        txn_id_t cur_granted_txn = granted_pair.first;
        txn_id_t cur_waiting_txn = waiting_pair.first;
        AddEdge(cur_granted_txn, cur_waiting_txn);

        txns[cur_granted_txn].emplace_back(rid, HOLDING);
        txns[cur_waiting_txn].emplace_back(rid, WAITING);
      }
    }
  }
  return txns;
}

// (1) Iterate lock_table_, and reconstruct wait_for_ graph via AddEdge() and RemoveEdge() method.
// (2) Perform deadlock detection via HasCycle() method.
// (3) Abort younger transaction, check whether the released lock can be granted to other transactions.
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> lck(latch_);
      auto txns = ReconstructWaitForGraph();
      txn_id_t txn_id;
      if (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        assert(txn != nullptr);
        txn->SetState(TransactionState::ABORTED);
        const std::vector<std::pair<RID, bool>> &related_lock_requests = txns[txn_id];
        assert(!related_lock_requests.empty());
        for (const auto &[rid, is_holding] : related_lock_requests) {
          LockRequestQueue &lock_request_queue = lock_table_[rid];
          std::list<LockRequest> request_queue = lock_request_queue.request_queue_;
          auto lock_request_queue_iter =
              std::find_if(request_queue.begin(), request_queue.end(),
                           [txn_id](const LockRequest &lock_request) { return lock_request.txn_id_ == txn_id; });

          // If the aborted transaction is holding the lock, unlock it.
          // (1) Remove lock from transaction
          // (2) Remove transaction from LockRequestQueue
          // (3) Reschedule other transactions if needed(there's other transactions waiting in the LockRequestQueue)
          // (4) Update aborted_ at LockRequest.
          if (is_holding) {
            UnlockImpl(txn, rid, &lock_request_queue_iter, nullptr /* global latch */, true /* is_holding */);
          } else {
            // Otherwise,
            // (1) Remove lock from transaction
            // (2) Remove transaction from LockRequestQueue
            lock_request_queue_iter->aborted_ = true;
            UnlockImpl(txn, rid, &lock_request_queue_iter, nullptr /* global latch */, false /* is_holding */);
          }
          lock_table_[rid].cv_.notify_all();
        }
      }
    }
  }
}

}  // namespace bustub
