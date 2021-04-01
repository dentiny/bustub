//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

using std::pair;
using std::vector;

namespace bustub {

/*
  1. Allocate pages in the buffer pool for hash table page, and pages for each bucket.
  2. Set metadata for hash table page, unpin the above pages.
  3. Note: hash table metadata(members in HashTableHeaderPage) is data stored at
  head_page, which is stored and fetched via header_page_id_.
*/
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)),
      bucket_num_(num_buckets),
      // Calculate the number of pages needed for num_buckets entries.
      // Note: BLOCK_ARRAY_SIZE represents how many entries could be stored at a page.
      bucket_page_num_((num_buckets - 1) / bucket_num_per_page_ + 1),
      last_page_bucket_num_(num_buckets - bucket_num_per_page_ * (bucket_page_num_ - 1)) {
  // Allocate head_page, which is stored and fetched on header_page_id_;
  // ht_header_page(metadata of hash table) is the data of head_page.
  Page *head_page = buffer_pool_manager_->NewPage(&header_page_id_);
  head_page->WLatch();
  HashTableHeaderPage *ht_header_page = reinterpret_cast<HashTableHeaderPage *>(head_page->GetData());
  ht_header_page->SetPageId(header_page_id_);
  ht_header_page->SetLSN(0);
  ht_header_page->SetSize(num_buckets);
  head_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true /* is_dirty */);

  // Allocate pages for hash buckets; bucket_page_ids_ map bucket page id
  // (index of vector) to real page id.
  bucket_page_ids_ = vector<page_id_t>(bucket_page_num_, 0);
  page_id_t temp_bucket_page_id = INVALID_PAGE_ID;
  for (size_t ii = 0; ii < bucket_page_num_; ++ii) {
    buffer_pool_manager_->NewPage(&temp_bucket_page_id);
    buffer_pool_manager->UnpinPage(temp_bucket_page_id, false /* is_dirty */);
    bucket_page_ids_[ii] = temp_bucket_page_id;
  }

  // Add bucket page ids into hash table page, so that it could fetch
  // buckets via their page id.
  head_page = buffer_pool_manager_->FetchPage(header_page_id_);
  head_page->WLatch();
  ht_header_page = reinterpret_cast<HashTableHeaderPage *>(head_page->GetData());
  for (size_t ii = 0; ii < bucket_page_num_; ++ii) {
    ht_header_page->AddBlockPageId(bucket_page_ids_[ii]);
  }
  head_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true /* is_dirty */);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // Locate page and slot offset of the key.
  const uint64_t hash_value = hash_fn_.GetHash(key);
  table_latch_.RLock();
  auto [start_page_idx, start_slot_idx] = GetBucketPosition(hash_value % bucket_num_);
  size_t cur_page_idx = start_page_idx;
  slot_offset_t cur_slot_idx = start_slot_idx;

  // Fetch bucket page via buffer pool manager.
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
  bucket_page->RLatch();
  auto ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());

  // Iterate through until unoccupied.
  while (ht_block_page->IsOccupied(cur_slot_idx)) {
    if (ht_block_page->IsReadable(cur_slot_idx) && comparator_(key, ht_block_page->KeyAt(cur_slot_idx)) == 0) {
      result->emplace_back(ht_block_page->ValueAt(cur_slot_idx));
    }

    // Current ht_block_page has been read over, fetch next one.
    if (++cur_slot_idx == ((cur_page_idx == (bucket_page_num_ - 1)) ? last_page_bucket_num_ : bucket_num_per_page_)) {
      // Release currently holding bucket_page.
      // Note: when fetching, FetchPage() involves pinning the page; while release, have to manually unpin.
      bucket_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], false /* is_dirty */);

      cur_page_idx = (cur_page_idx + 1) % bucket_page_num_;
      cur_slot_idx = 0;

      bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
      bucket_page->RLatch();
      ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());
    }

    // Check if goes back to where it starts.
    if (cur_page_idx == start_page_idx && cur_slot_idx == start_slot_idx) {
      break;
    }
  }
  bucket_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], false /* is_dirty */);
  table_latch_.RUnlock();
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/

// table_latch_ is guarenteed to be acquired when invoked.
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InsertImpl(const KeyType &key, const ValueType &value) {
  // Locate page and slot offset of the key.
  const uint64_t hash_value = hash_fn_.GetHash(key);
  auto [start_page_idx, start_slot_idx] = GetBucketPosition(hash_value % bucket_num_);
  size_t cur_page_idx = start_page_idx;
  slot_offset_t cur_slot_idx = start_slot_idx;

  // Fetch bucket page via buffer pool manager.
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
  bucket_page->WLatch();
  auto ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());

  bool insert_succeeds = true;
  while (!ht_block_page->Insert(cur_slot_idx, key, value)) {
    // The new kv-pair has already been inserted in the hash table.
    if (ht_block_page->IsReadable(cur_slot_idx) && comparator_(key, ht_block_page->KeyAt(cur_slot_idx)) == 0 &&
        value == ht_block_page->ValueAt(cur_slot_idx)) {
      insert_succeeds = false;
      break;
    }

    // The slot has been occupied by other kv-pairs, iterate the next slot. If
    // current ht_block_page has been read over, fetch next one.
    if (++cur_slot_idx == ((cur_page_idx == (bucket_page_num_ - 1)) ? last_page_bucket_num_ : bucket_num_per_page_)) {
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], false /* is_dirty */);

      cur_page_idx = (cur_page_idx + 1) % bucket_page_num_;
      cur_slot_idx = 0;

      bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
      bucket_page->WLatch();
      ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());
    }

    // The hash table has been full, no free slots to insert.
    if (cur_page_idx == start_page_idx && cur_slot_idx == start_slot_idx) {
      insert_succeeds = false;
      break;
    }
  }

  bucket_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], insert_succeeds /* is_dirty */);
  return insert_succeeds;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  bool insert_succeeds = InsertImpl(key, value);
  table_latch_.WUnlock();
  return insert_succeeds;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Locate page and slot offset of the key.
  const uint64_t hash_value = hash_fn_.GetHash(key);
  table_latch_.WLock();
  auto [start_page_idx, start_slot_idx] = GetBucketPosition(hash_value % bucket_num_);
  size_t cur_page_idx = start_page_idx;
  slot_offset_t cur_slot_idx = start_slot_idx;

  // Fetch bucket page via buffer pool manager.
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
  bucket_page->WLatch();
  auto ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());

  bool remove_succeeds = false;
  while (ht_block_page->IsOccupied(cur_slot_idx)) {
    // Find the target kv-pair in the slot.
    if (ht_block_page->IsReadable(cur_slot_idx) && comparator_(key, ht_block_page->KeyAt(cur_slot_idx)) == 0 &&
        value == ht_block_page->ValueAt(cur_slot_idx)) {
      ht_block_page->Remove(cur_slot_idx);
      remove_succeeds = true;
      break;
    }

    if (++cur_slot_idx == ((cur_page_idx == (bucket_page_num_ - 1)) ? last_page_bucket_num_ : bucket_num_per_page_)) {
      bucket_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], false /* is_dirty */);

      cur_page_idx = (cur_page_idx + 1) % bucket_page_num_;
      cur_slot_idx = 0;

      bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
      bucket_page->WLatch();
      ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());
    }

    // The hash table has been full, no free slots to insert.
    if (cur_page_idx == start_page_idx && cur_slot_idx == start_slot_idx) {
      break;
    }
  }

  bucket_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], remove_succeeds /* is_dirty */);
  table_latch_.WUnlock();
  return remove_succeeds;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  /*
    1. Fetch all kv-pairs into memory.
    2. Calculate the new hash table metadata(bucket_num_, bucket_page_num_, last_page_bucket_num_, bucket_page_ids_).
    3. Allocate new pages, and update ht_header_page: ht_header_page->SetSize(num_buckets).
    4. Rehash kv-pairs into disk.
  */

  // 1. Fetch all kv-pairs into memory.
  table_latch_.WLock();
  vector<pair<KeyType, ValueType>> kv;
  for (size_t cur_page_idx = 0; cur_page_idx < bucket_page_num_; ++cur_page_idx) {
    Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_ids_[cur_page_idx]);
    bucket_page->RLatch();
    auto ht_block_page = reinterpret_cast<HashTableBlockPageType *>(bucket_page->GetData());
    for (slot_offset_t cur_slot_idx = 0;
         cur_slot_idx < ((cur_page_idx == (bucket_page_num_ - 1)) ? last_page_bucket_num_ : bucket_num_per_page_);
         ++cur_slot_idx) {
      if (ht_block_page->IsReadable(cur_slot_idx)) {
        kv.emplace_back(ht_block_page->KeyAt(cur_slot_idx), ht_block_page->ValueAt(cur_slot_idx));
        ht_block_page->Remove(cur_slot_idx);
      }
    }
    bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_ids_[cur_page_idx], false /* is_dirty */);
  }

  // 2. Calculate the new hash table metadata(bucket_num_, bucket_page_num_, last_page_bucket_num_, bucket_page_ids_).
  const size_t old_bucket_page_num = bucket_page_num_;
  bucket_num_ = initial_size * 2;
  bucket_page_num_ = (bucket_num_ - 1) / bucket_num_per_page_ + 1;
  last_page_bucket_num_ = bucket_num_ - bucket_num_per_page_ * (bucket_page_num_ - 1);
  bucket_page_ids_.reserve(bucket_page_num_);

  // 3. Allocate new pages, and update ht_header_page: ht_header_page->SetSize(num_buckets).
  page_id_t temp_bucket_page_id = INVALID_PAGE_ID;
  for (size_t ii = old_bucket_page_num; ii < bucket_page_num_; ++ii) {
    buffer_pool_manager_->NewPage(&temp_bucket_page_id);
    buffer_pool_manager_->UnpinPage(temp_bucket_page_id, false /* is_dirty */);
    bucket_page_ids_.push_back(temp_bucket_page_id);
  }

  // Add bucket page ids into hash table page, so that it could fetch
  // buckets via their page id.
  Page *head_page = buffer_pool_manager_->FetchPage(header_page_id_);
  head_page->WLatch();
  auto ht_header_page = reinterpret_cast<HashTableHeaderPage *>(head_page->GetData());
  for (size_t ii = old_bucket_page_num; ii < bucket_page_num_; ++ii) {
    ht_header_page->AddBlockPageId(bucket_page_ids_[ii]);
  }
  ht_header_page->SetSize(bucket_num_);
  head_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true /* is_dirty */);

  // 4. Rehash kv-pairs into disk.
  for (const auto &[key, val] : kv) {
    assert(InsertImpl(key, val));
  }
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  size_t size = bucket_num_;
  table_latch_.RUnlock();
  return size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
