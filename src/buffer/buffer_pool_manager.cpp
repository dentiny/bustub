//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <cassert>
#include <list>
#include <unordered_map>

using std::mutex;
using std::unique_lock;

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::ReplaceAndUpdate(page_id_t new_page_id, bool is_new_page, unique_lock<mutex>* lck) {
  assert(!free_list_.empty() || replacer_->Size() > 0);
  Page *page = nullptr;
  frame_id_t frame_idx;
  if (!free_list_.empty()) {
    frame_idx = free_list_.front();
    free_list_.pop_front();
    page_table_.emplace(new_page_id, frame_idx);
    page = pages_ + frame_idx;
    page->WLatch();
    lck->unlock();
    if (!is_new_page) {
      disk_manager_->ReadPage(new_page_id, page->data_);
    }
  } else {
    assert(replacer_->Victim(&frame_idx));  // victim_res is a placeholder
    page = pages_ + frame_idx;
    page_table_.erase(page->page_id_);
    page_table_.emplace(new_page_id, frame_idx);
    replacer_->Pin(frame_idx);
    page->WLatch();
    lck->unlock();

    // If page dirty, flush original page to disk.
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    if (!is_new_page) {
      disk_manager_->ReadPage(new_page_id, page->data_);
    } else {
      page->ResetMemory();
    }
  }

  // Update target page's metadata.
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = is_new_page;
  page->WUnlatch();
  return page;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // Target page already in the buffer pool, pin and return.
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t idx = it->second;
    Page *targetPage = pages_ + idx;
    ++targetPage->pin_count_;
    replacer_->Pin(idx);
    return targetPage;
  }

  // If no page is available in the free list, and all other pages are
  // currently pinned.
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  return ReplaceAndUpdate(page_id, false, &lck);
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  assert(it != page_table_.end());
  auto frame_idx = it->second;
  Page *page = pages_ + frame_idx;
  if (page->pin_count_ <= 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->Unpin(frame_idx);
  }
  page->is_dirty_ |= is_dirty;
  return true;
}

// Flush the target page to disk.
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  Page *page = pages_ + it->second;
  page->WLatch();
  lck.unlock();
  if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
    page->is_dirty_ = false;
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page->WUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  unique_lock<mutex> lck(latch_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  page_id_t id = disk_manager_->AllocatePage();
  *page_id = id;
  return ReplaceAndUpdate(id, true, &lck);
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);

  // 1.   If P does not exist, return true.
  if (it == page_table_.end()) {
    lck.unlock();
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  auto frame_idx = it->second;
  Page *page = pages_ + frame_idx;
  page->WLatch();

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->pin_count_ > 0) {
    page->WUnlatch();
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  replacer_->Pin(frame_idx);
  page_table_.erase(page_id);
  free_list_.emplace_back(frame_idx);
  lck.unlock();

  // Reset metadata and return to free list.
  disk_manager_->DeallocatePage(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->WUnlatch();
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  unique_lock<mutex> lck(latch_);
  for (size_t ii = 0; ii < pool_size_; ++ii) {
    Page *page = pages_ + ii;
    if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
