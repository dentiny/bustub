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

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

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
      // Calculate the number of pages needed for num_buckets entries.
      // Note: BLOCK_ARRAY_SIZE represents how many entries could be stored at a page.
      bucket_page_num_((num_buckets - 1) / BLOCK_ARRAY_SIZE + 1),
      last_page_bucket_num_(num_buckets - BLOCK_ARRAY_SIZE * (bucket_page_num_ - 1)) {
      // Allocate head_page, which is stored and fetched on header_page_id_;
      // ht_header_page(metadata of hash table) is the data of head_page.
      Page *head_page = buffer_pool_manager_->NewPage(&header_page_id_);
      head_page->WLatch();
      HashTableHeaderPage *ht_header_page = reinterpret_cast<HashTableHeaderPage*>(head_page->GetData());
      ht_header_page->SetPageId(header_page_id_);
      ht_header_page->SetLSN(0);
      ht_header_page->SetSize(num_buckets);
      head_page->WUnlatch();
      buffer_pool_manager->UnpinPage(header_page_id_, true /* is_dirty */);

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
      ht_header_page = reinterpret_cast<HashTableHeaderPage*>(head_page->GetData());
      for (size_t ii = 0; ii < bucket_page_num_; ++ii) {
        ht_header_page->AddBlockPageId(bucket_page_ids_[ii]);
      }
      head_page->WUnlatch();
      buffer_pool_manager->UnpinPage(header_page_id_, true /* is_dirty */);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return 0;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
