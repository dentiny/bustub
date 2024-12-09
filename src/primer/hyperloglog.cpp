#include "primer/hyperloglog.h"

#include <iostream>

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), num_of_bits_(n_bits) {
  if (num_of_bits_ >= 0) {
    buckets_ = std::vector<uint8_t>(1 << num_of_bits_, 0);
  }
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  std::bitset<BITSET_CAPACITY> bs{hash};
  return bs;
}

// The index returned is relative to starting [num_of_bits_].
// For example, | 0 0 1 | 0 1 0 ...| -> 2
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (size_t idx = num_of_bits_; idx < BITSET_CAPACITY; ++idx) {
    if (bset[BITSET_CAPACITY - 1 - idx]) {
      return idx + 1 - num_of_bits_;
    }
  }
  return BITSET_CAPACITY - num_of_bits_;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  if (num_of_bits_ < 0) {
    return;
  }

  std::lock_guard lck(mtx_);
  const hash_t hash_val = CalculateHash(val);
  const auto bset = ComputeBinary(hash_val);
  const size_t bkt_idx = GetIndexOfBucket(bset);
  const uint8_t zero_count = PositionOfLeftmostOne(bset);
  buckets_[bkt_idx] = std::max(buckets_[bkt_idx], zero_count);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  if (num_of_bits_ < 0) {
    return;
  }

  std::lock_guard lck(mtx_);
  double sum = 0;
  for (size_t bkt_val : buckets_) {
    sum += 1.0 / (1ULL << bkt_val);
  }
  cardinality_ = static_cast<size_t>(CONSTANT * buckets_.size() * buckets_.size() / sum);
}

template <typename KeyType>
size_t HyperLogLog<KeyType>::GetIndexOfBucket(const std::bitset<BITSET_CAPACITY> &bset) const {
  // Read first [num_of_bits_] bits to decide index.
  size_t bucket_index = 0;
  for (int64_t idx = 0; idx < num_of_bits_; ++idx) {
    bucket_index = bucket_index * 2 + static_cast<size_t>(bset[BITSET_CAPACITY - 1 - idx]);
  }
  return bucket_index;
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
