#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "common/thread_annotation.h"
#include "common/util/hash_util.h"

/** @brief Capacity of the bitset stream. */
#define BITSET_CAPACITY 64

namespace bustub {

template <typename KeyType>
class HyperLogLog {
  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disable default constructor. */
  HyperLogLog() = delete;

  /** @brief Parameterized constructor. */
  explicit HyperLogLog(int16_t n_bits);

  /**
   * @brief Getter value for cardinality.
   *
   * @returns cardinality value
   */
  auto GetCardinality() {
    std::lock_guard lck(mtx_);
    return cardinality_;
  }

  /**
   * @brief Adds a value into the HyperLogLog.
   *
   * @param[in] val - value that's added into hyperloglog
   */
  auto AddElem(KeyType val) -> void;

  /**
   * @brief Function that computes cardinality.
   */
  auto ComputeCardinality() -> void;

 private:
  /**
   * @brief Calculates Hash of a given value.
   *
   * @param[in] val - value
   * @returns hash integer of given input value
   */
  inline auto CalculateHash(const KeyType &val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
    } else {
      val_obj = Value(BIGINT, val);
    }
    return bustub::HashUtil::HashValue(&val_obj);
  }

  /**
   * @brief Function that computes binary.
   *
   *
   * @param[in] hash
   * @returns binary of a given hash
   */
  auto ComputeBinary(const hash_t &hash) const LOCK_REQUIRES(mtx_) -> std::bitset<BITSET_CAPACITY>;
  /**
   * @brief Function that computes leading zeros.
   *
   * @param[in] bset - binary values of a given bitset
   * @returns number of leading zeros of given binary set
   */
  auto PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  // Get the index of bucket.
  size_t GetIndexOfBucket(const std::bitset<BITSET_CAPACITY> &bset) const;

  std::mutex mtx_;
  /** @brief Cardinality value. */
  size_t cardinality_ GUARDED_BY(mtx_);
  // Number of initial bits.
  const int64_t num_of_bits_;
  // Buckets to store counts.
  std::vector<uint8_t> buckets_ GUARDED_BY(mtx_);
};

}  // namespace bustub
