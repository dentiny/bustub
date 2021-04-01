//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 * ClockReplacer doesn't directly manages buffer frame; it only takes charge of replacement policy.
 * Frame in the replacer are those unpinned, in other words they are not currently being read by threads, which could be
 * victimized.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  // Called after a page is pinned to a frame in BufferPoolManager; remove the
  // frame containing the pinned page from ClockReplacer.
  void Pin(frame_id_t frame_id) override;

  // Called when the pin_count of a page becomes 0; add the frame containing
  // the unpinned page to the ClockReplacer.
  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  size_t size_;  // actual size of buffer frames
  const size_t capacity_;
  size_t hand_;
  std::mutex mtx_;
  std::vector<std::pair<bool, bool>> refs_;  // <if exists, if referenced>
};

}  // namespace bustub
