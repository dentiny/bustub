//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "buffer/lru_replacer.h"

using std::mutex;
using std::unique_lock;

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

// It's guarenteed latch is acquired when invoked.
void LRUReplacer::Remove(frame_id_t frame_id) {
  auto map_it = frame_map_.find(frame_id);
  assert(map_it != frame_map_.end());
  auto list_it = map_it->second;
  frames_.erase(list_it);
  frame_map_.erase(map_it);
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  unique_lock<mutex> lck(latch_);
  if (frames_.empty()) {
    return false;
  }
  *frame_id = *frames_.rbegin();
  Remove(*frame_id);
  return true;
}

// Remove the frame from LRUReplacer.
void LRUReplacer::Pin(frame_id_t frame_id) {
  unique_lock<mutex> lck(latch_);
  auto map_it = frame_map_.find(frame_id);
  if (map_it == frame_map_.end()) {
    return;  // the frame to pin is not in the LRUReplacer
  }
  Remove(frame_id);
}

// Add the frame from LRUReplacer.
void LRUReplacer::Unpin(frame_id_t frame_id) {
  unique_lock<mutex> lck(latch_);
  auto map_it = frame_map_.find(frame_id);
  if (map_it != frame_map_.end()) {
    return;  // the frame to unpin is already in the LRUReplacer
  }

  // If the LRU replacer has been full, evict the least frequent accessed one.
  if (frames_.size() == capacity_) {
    frame_id_t victim_frame_id = frames_.back();
    Remove(victim_frame_id);
  }
  assert(frames_.size() < capacity_);
  frames_.push_front(frame_id);
  frame_map_.emplace(frame_id, frames_.begin());
}

size_t LRUReplacer::Size() {
  unique_lock<mutex> lck(latch_);
  size_t map_size = frame_map_.size();
  size_t list_size = frames_.size();
  assert(map_size == list_size);
  return map_size;
}

}  // namespace bustub
