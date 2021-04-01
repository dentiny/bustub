//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <iostream>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, bool granted)
        : txn_id_(txn_id), lock_mode_(lock_mode), granted_(granted), aborted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
    bool aborted_;
  };

  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;
    std::mutex latch_;            // mutex at RID granularity
    std::condition_variable cv_;  // for notifying blocked transactions on this rid
    bool upgrading_ = false;      // if there's already txn requesting for upgrading

    // number of shared locks and exclusive locks granted, updated when the txn is decided
    // able to grant lock, or unlock
    uint32_t shared_count = 0;
    uint32_t exclusive_count = 0;

    // (1) Check whether the transaction could upgrade lock.
    // (2) If true, remove the transaction from request_queue_.
    bool CheckUpgradeAndRemoveLock(Transaction *txn) {
      if (upgrading_) {
        return false;
      }

      auto it = std::find_if(request_queue_.begin(), request_queue_.end(), [txn](const LockRequest &lock_request) {
        return lock_request.txn_id_ == txn->GetTransactionId();
      });
      if (it == request_queue_.end() || it->lock_mode_ != LockMode::SHARED || !it->granted_) {
        return false;
      }

      // The transaction could upgrade, remove lock from request_queue_.
      request_queue_.erase(it);
      return true;
    }

    void GrantLock(Transaction *txn, const RID &rid, LockMode lock_mode, bool is_upgrade,
                   std::unique_lock<std::mutex> *lck) {
      bool can_grant = CanGrantLock(lock_mode);
      auto it = request_queue_.emplace(request_queue_.end(), txn->GetTransactionId(), lock_mode, can_grant);

      // Note: if transaction get blocked, shared_count/exclusive_count will be updated within Unlock() method.
      if (!can_grant) {
        upgrading_ |= is_upgrade;
        cv_.wait(*lck, [&it]() { return it->granted_ || it->aborted_; });
      } else if (lock_mode == LockMode::SHARED) {
        ++shared_count;
      } else if (lock_mode == LockMode::EXCLUSIVE) {
        ++exclusive_count;
      }

      // The transaction has been aborted due to deadlock.
      if (it->aborted_) {
        return;
      }

      // The transaction can be granted lock now.
      if (lock_mode == LockMode::SHARED) {
        txn->AddSharedLock(rid);
      } else if (lock_mode == LockMode::EXCLUSIVE) {
        txn->AddExclusiveLock(rid);
      }
      if (is_upgrade) {
        upgrading_ = false;
      }
      it->granted_ = true;
    }

   private:
    // Check whether transaction can be granted lock right now, otherwise it'll be added into request queue and wait.
    bool CanGrantLock(LockMode lock_mode) {
      bool can_grant_lock = false;
      if (lock_mode == LockMode::SHARED) {
        can_grant_lock = request_queue_.back().granted_ && request_queue_.back().lock_mode_ == LockMode::SHARED;
      } else if (lock_mode == LockMode::EXCLUSIVE) {
        can_grant_lock = request_queue_.empty();
      } else {
        BUSTUB_ASSERT(0, "Meet unexpected lock mode when CanGrantLock() method.");
      }
      return can_grant_lock;
    }
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    LOG_INFO("Cycle detection thread launched");
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
    LOG_INFO("Cycle detection thread stopped");
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
   *    is responsible for keeping track of its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /*** Graph API ***/
  /**
   * Adds edge t1->t2
   */

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

 private:
  // Cycle detection util.
  bool CycleDetectImpl(txn_id_t txn, const std::unordered_set<txn_id_t> &visited, txn_id_t *txn1, txn_id_t *txn2);

  // Util for all lock methods.
  bool LockImpl(Transaction *txn, const RID &rid, LockMode lock_mode, bool is_upgrading);

  // Util for Unlock() and RunCycleDetection() methods.
  bool UnlockImpl(Transaction *txn, const RID &rid, std::list<LockRequest>::iterator *lock_request_queue_iter,
                  std::unique_lock<std::mutex> *lck, bool is_holding);

  // Construct wait-for graph on the fly everytime RunCycleDetection() launches.
  // map: <txn, all RID waiting or holding>, true for holding, false for waiting
  std::unordered_map<txn_id_t, std::vector<std::pair<RID, bool>>> ReconstructWaitForGraph();

 private:
  static constexpr bool HOLDING = true;
  static constexpr bool WAITING = false;

  std::mutex latch_;            // mutex at lock_table granularity
  std::mutex waits_for_latch_;  // mutex for waits_for_
  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
};

}  // namespace bustub
