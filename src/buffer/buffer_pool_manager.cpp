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

/*
 * Notes on log flushing:
 * Before your buffer pool manager evicts a dirty page from the clock replacer and writes this page back to the disk,
 * it needs to flush all logs up to pageLSN. You need to compare persistent_lsn_ (a member variable in LogManager)
 * with your pageLSN. However, unlike with group commit, the buffer pool can force the log manager to flush the log
 * buffer (but still needs to wait for the logs to be stored before continuing).
 */

#include "buffer/buffer_pool_manager.h"

#include <cassert>
#include <iostream>
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

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  /*
    Four cases:
    (1) page already in the buffer pool
    (2) page not in the buffer pool, but there's position in the free list
    (3) no free page, so have to evict unpinned page in the buffer pool
    (4) no unpinned page to evict, nothing to fetch
  */

  // case1: page already in the buffer pool
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t idx = it->second;
    Page *targetPage = pages_ + idx;
    ++targetPage->pin_count_;
    replacer_->Pin(idx);
    return targetPage;
  }

  // case2: page not in the buffer pool, but there's position in the free list
  if (!free_list_.empty()) {
    frame_id_t frame_idx = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_idx;
    Page *page = pages_ + frame_idx;
    disk_manager_->ReadPage(page_id, page->data_);
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    return page;
  }

  // case3: no free page, so have to evict unpinned page in the buffer pool
  frame_id_t victim_frame_id = INVALID_PAGE_ID;
  if (replacer_->Victim(&victim_frame_id)) {
    // Evict victim page.
    Page *page = pages_ + victim_frame_id;
    page_table_.erase(page->page_id_);
    page_table_[page_id] = victim_frame_id;
    replacer_->Pin(victim_frame_id);
    if (page->IsDirty()) {
      if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
        log_manager_->Flush(true /* is_forced */);
      }
      disk_manager_->WritePage(page->page_id_, page->data_);
    }

    // Load new page into buffer pool.
    disk_manager_->ReadPage(page_id, page->data_);
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    return page;
  }

  // case4: no unpinned page to evict, nothing to fetch
  return nullptr;
}

/*
  A page in the buffer pool needs to be pinned when it's read, so that other
  threads won't write to it; it shouldn't be victim page to be replaced, so it
  won't appear in the replacer. Also, it will be unpinned after reading, and
  add to the replacer.
*/
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // the unpinned page is not in the buffer pool
  }
  frame_id_t frame_idx = it->second;
  Page *page = pages_ + frame_idx;
  page->is_dirty_ |= is_dirty;
  if (page->GetPinCount() == 0) {
    return true;  // the unpinned page is not pinned
  }
  if (--page->pin_count_ == 0) {
    replacer_->Unpin(frame_idx);
  }
  return true;
}

// Flush the target page to disk.
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  unique_lock<mutex> lck(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // the target flushed page is not in the buffer pool
  }
  frame_id_t frame_idx = it->second;
  Page *page = pages_ + frame_idx;
  if (page->IsDirty()) {
    if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
      log_manager_->Flush(true /* is_forced */);
    }
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
  return true;
}

// Creates a new page in the buffer pool.
// Note: new buffered page is considered dirty and pinned.
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  unique_lock<mutex> lck(latch_);
  *page_id = disk_manager_->AllocatePage();

  // Allocate the page from the free list.
  if (!free_list_.empty()) {
    frame_id_t frame_idx = free_list_.front();
    free_list_.pop_front();
    page_table_[*page_id] = frame_idx;
    Page *page = pages_ + frame_idx;
    page->ResetMemory();
    page->page_id_ = *page_id;
    page->is_dirty_ = true;
    page->pin_count_ = 1;
    return page;
  }

  // Victimize a page via replacer.
  frame_id_t victim_idx = INVALID_PAGE_ID;
  if (replacer_->Victim(&victim_idx)) {
    Page *page = pages_ + victim_idx;
    page_id_t victim_page_id = page->GetPageId();
    if (page->IsDirty()) {
      if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
        log_manager_->Flush(true /* is_forced */);
      }
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page->ResetMemory();
    page_table_.erase(victim_page_id);
    page_table_[*page_id] = victim_idx;
    page->page_id_ = *page_id;
    page->is_dirty_ = true;
    page->pin_count_ = 1;
    return page;
  }

  // No pages in the free list, and all pages inside buffer pool are pinned.
  return nullptr;
}

// Delete a page from buffer pool.
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  unique_lock<mutex> lck(latch_);
  disk_manager_->DeallocatePage(page_id);
  auto it = page_table_.find(page_id);

  // The page is not in the buffer pool.
  if (it == page_table_.end()) {
    return true;
  }

  // The page is currently be used by others, cannot delete from buffer pool.
  frame_id_t frame_idx = it->second;
  Page *page = pages_ + frame_idx;
  if (page->GetPinCount() > 0) {
    return false;
  }

  assert(page->pin_count_ == 0);
  if (page->IsDirty()) {
    if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
      log_manager_->Flush(true /* is_forced */);
    }
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page_table_.erase(page_id);
  page->page_id_ = page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  free_list_.push_back(frame_idx);
  return true;
}

// Note: cannot call FlushPageImpl() since mutex is not re-entrant.
void BufferPoolManager::FlushAllPagesImpl() {
  unique_lock<mutex> lck(latch_);
  for (size_t ii = 0; ii < pool_size_; ++ii) {
    Page *page = pages_ + ii;
    if (page->IsDirty()) {
      if (enable_logging && log_manager_->GetPersistentLSN() < page->GetLSN()) {
        log_manager_->Flush(true /* is_forced */);
      }
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
