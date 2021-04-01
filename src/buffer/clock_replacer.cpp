//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "buffer/clock_replacer.h"

using std::mutex;
using std::pair;
using std::unique_lock;
using std::vector;

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : size_(0),
      capacity_(num_pages),
      hand_(0),
      refs_(vector<pair<bool, bool>>(capacity_, std::make_pair(false, false))) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  unique_lock<mutex> lck(mtx_);
  if (size_ == 0) {
    return false;
  }

  while (true) {
    assert(hand_ < capacity_);
    auto &entry = refs_[hand_];
    if (entry.first) {     // exists
      if (entry.second) {  // referenced
        entry.second = false;
      } else {  // not referenced
        *frame_id = static_cast<frame_id_t>(hand_);
        entry.first = false;
        --size_;
        break;
      }
    }
    hand_ = (hand_ + 1) % capacity_;
  }
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  unique_lock<mutex> lck(mtx_);
  assert(static_cast<size_t>(frame_id) < capacity_);
  auto &entry = refs_[frame_id];
  if (entry.first) {
    entry.first = false;
    entry.second = false;
    --size_;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  unique_lock<mutex> lck(mtx_);
  assert(static_cast<size_t>(frame_id) < capacity_);
  auto &entry = refs_[frame_id];
  if (!entry.first) {
    entry.first = true;
    entry.second = true;
    ++size_;
  }
}

size_t ClockReplacer::Size() {
  unique_lock<mutex> lck(mtx_);
  return size_;
}

}  // namespace bustub
