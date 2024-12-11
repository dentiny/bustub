//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <limits>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

namespace {

constexpr size_t kMaxAccessTimestamp = std::numeric_limits<size_t>::max();
constexpr size_t kInfTimestampDist = std::numeric_limits<size_t>::max();

// The frame with smallest eviction score will be evicted.
struct EvictionScore {
  size_t earliest_access_timestamp;
  size_t timestamp_distance;

  bool operator<(const EvictionScore &rhs) const {
    if (timestamp_distance > rhs.timestamp_distance) {
      return true;
    }
    if (timestamp_distance < rhs.timestamp_distance) {
      return false;
    }
    return earliest_access_timestamp < rhs.earliest_access_timestamp;
  }
};

}  // namespace

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  if (evictable_size_ == 0) {
    return std::nullopt;
  }
  EvictionScore smallest_eviction_score{
      .earliest_access_timestamp = kMaxAccessTimestamp,
      .timestamp_distance = 0,
  };
  frame_id_t frame_to_evict = 0;
  for (auto &[cur_frame_id, cur_record] : records_) {
    if (!cur_record.is_evictable) {
      continue;
    }
    size_t cur_ts_dist = cur_record.timestamps.size() == k_
                             ? (cur_record.timestamps.back() - cur_record.timestamps.front())
                             : kInfTimestampDist;
    EvictionScore cur_eviction_score{
        .earliest_access_timestamp = cur_record.timestamps.front(),
        .timestamp_distance = cur_ts_dist,
    };
    if (cur_eviction_score < smallest_eviction_score) {
      smallest_eviction_score = cur_eviction_score;
      frame_to_evict = cur_frame_id;
    }
  }
  auto record_iter = records_.find(frame_to_evict);
  lru_queue_.erase(record_iter->second.lru_iterator);
  records_.erase(record_iter);
  --evictable_size_;
  return frame_to_evict;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT_AND_LOG(frame_id < static_cast<frame_id_t>(replacer_size_));
  auto record_iter = records_.find(frame_id);

  // Record already existing frame id.
  if (record_iter != records_.end()) {
    auto &cur_record = record_iter->second;
    if (cur_record.timestamps.size() >= k_) {
      cur_record.timestamps.pop_front();
    }
    cur_record.timestamps.emplace_back(current_timestamp_++);
    lru_queue_.splice(lru_queue_.begin(), lru_queue_, cur_record.lru_iterator);
    return;
  }

  // Record not accessed before.
  lru_queue_.emplace_front(frame_id);
  records_.emplace(frame_id, Record{
                                 .timestamps = std::deque<size_t>{current_timestamp_++},
                                 .lru_iterator = lru_queue_.begin(),
                                 .is_evictable = false,
                             });
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto record_iter = records_.find(frame_id);
  if (record_iter == records_.end()) {
    return;
  }
  auto &cur_record = record_iter->second;
  if (cur_record.is_evictable && !set_evictable) {
    --evictable_size_;
  } else if (!cur_record.is_evictable && set_evictable) {
    ++evictable_size_;
  }
  cur_record.is_evictable = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto record_iter = records_.find(frame_id);
  BUSTUB_ASSERT_AND_LOG(record_iter != records_.end()) << "Frame id " << frame_id << " doesn't exist in replacer.";
  const auto &cur_record = record_iter->second;
  BUSTUB_ASSERT_AND_LOG(cur_record.is_evictable) << "Frame id " << frame_id << " is not evictable.";
  lru_queue_.erase(cur_record.lru_iterator);
  records_.erase(record_iter);
  --evictable_size_;
}

}  // namespace bustub
