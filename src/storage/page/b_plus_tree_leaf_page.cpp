//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int idx1 = 0;
  int idx2 = GetSize() - 1;
  while (idx1 <= idx2) {
    int mid = idx1 + (idx2 - idx1) / 2;
    if (comparator(array[mid].first, key) >= 0) {
      idx2 = mid - 1;
    } else {
      idx1 = mid + 1;
    }
  }
  return idx2 + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int idx_to_insert = KeyIndex(key, comparator);
  IncreaseSize(1);
  int cur_size = GetSize();
  for (int ii = cur_size - 1; ii > idx_to_insert; --ii) {
    array[ii] = array[ii - 1];
  }
  array[idx_to_insert] = {key, value};
  return cur_size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(GetSize() > GetMaxSize());
  size_t cur_size = GetSize();
  size_t move_from_index = cur_size / 2;
  for (size_t ii = move_from_index; ii < cur_size; ++ii) {  // move the last half
    recipient->array[ii - move_from_index] = array[ii];
  }

  // Update current leaf page and recipient page's metadata.
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  SetSize(move_from_index);
  recipient->SetSize(cur_size - move_from_index);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int idx = KeyIndex(key, comparator);
  if (idx < GetSize() && comparator(KeyAt(idx), key) == 0) {
    *value = ValueAt(idx);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  // Record to delete doesn't exist in the leaf page.
  int idx_to_delete = KeyIndex(key, comparator);
  int old_size = GetSize();
  if (idx_to_delete >= old_size || comparator(KeyAt(idx_to_delete), key) != 0) {
    return old_size;
  }

  for (int ii = idx_to_delete; ii < old_size - 1; ++ii) {
    array[ii] = array[ii + 1];
  }
  IncreaseSize(-1);
  return old_size - 1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 * Note: current leaf page is the next page of recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, int index_in_parent,
                                           BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);

  int old_size = GetSize();
  int recipient_size = recipient->GetSize();
  for (int ii = 0; ii < old_size; ++ii) {
    recipient->array[recipient_size + ii] = array[ii];
  }
  SetSize(0);
  recipient->IncreaseSize(old_size);
  recipient->SetNextPageId(GetNextPageId());
  assert(recipient->GetSize() <= recipient->GetMaxSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                                                  BufferPoolManager *buffer_pool_manager) {
  // Current leaf page should at least have 2 items: 1 to move, 1 to update parent.
  int old_size = GetSize();
  assert(old_size >= 2);
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);

  // Get and remove the first item of self array, to the end of recipient.
  const MappingType &kv = GetItem(0 /* index */);
  for (int ii = 0; ii < old_size - 1; ++ii) {
    array[ii] = array[ii + 1];
  }
  IncreaseSize(-1);
  recipient->CopyLastFrom(kv);

  // Update parent's key range distribution(always KeyAt(0)).
  page_id_t page_id = GetPageId();
  page_id_t parent_page_id = GetParentPageId();
  Page *page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  int value_idx = parent_page->ValueIndex(page_id);  // index of self leaf page's first key in parent page
  parent_page->SetKeyAt(value_idx /* index */, KeyAt(0 /* index */));
  buffer_pool_manager->UnpinPage(parent_page_id, true /* is_dirty */);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 * Invoked by MoveFirstToEndOf, recipient add item to the end of the array.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,
                                                   BufferPoolManager *buffer_pool_manager) {
  // Current leaf page should at least have 2 items: 1 to move, 1 to update parent.
  int old_size = GetSize();
  assert(old_size >= 2);
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);

  // Get the move the last item to the front of recipient.
  const MappingType &kv = GetItem(old_size - 1 /* index */);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(kv, buffer_pool_manager);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 * Invoked by MoveLastToFrontOf, recipient add item to the front of the array; update parent's key.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  // Copy item to the front of array.
  int old_size = GetSize();
  assert(old_size + 1 <= GetMaxSize());
  for (int ii = 0; ii < old_size; ++ii) {
    array[ii + 1] = array[ii];
  }
  array[0] = item;
  IncreaseSize(1);

  // Update parent's key range distribution(always KeyAt(0)).
  page_id_t page_id = GetPageId();
  page_id_t parent_page_id = GetParentPageId();
  Page *page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  int value_idx = parent_page->ValueIndex(page_id);
  parent_page->SetKeyAt(value_idx /* index */, KeyAt(0 /* index */));
  buffer_pool_manager->UnpinPage(parent_page_id, true /* is_dirty */);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
