//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  size_t cur_size = GetSize();
  for (size_t ii = 0; ii < cur_size; ++ii) {
    if (ValueAt(ii) == value) {
      return ii;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetItem(int index) {
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int idx1 = 1;  // the first key should always be invalid
  int idx2 = GetSize() - 1;
  while (idx1 <= idx2) {
    int mid = idx1 + (idx2 - idx1) / 2;
    const KeyType &cur_key = array[mid].first;
    if (comparator(cur_key, key) <= 0) {
      idx1 = mid + 1;
    } else {
      idx2 = mid - 1;
    }
  }
  return array[idx1 - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetSize(2);
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  size_t index_to_place_new = ValueIndex(old_value) + 1;
  assert(index_to_place_new > 0);
  IncreaseSize(1);
  size_t cur_size = GetSize();
  for (size_t ii = cur_size - 1; ii > index_to_place_new; --ii) {
    array[ii] = array[ii - 1];
  }
  array[index_to_place_new] = {new_key, new_value};
  return cur_size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 *
 * Note: only called after split, when recipient has no kv-pair inside.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);
  assert(GetSize() > GetMaxSize());
  page_id_t recipient_page_id = recipient->GetPageId();
  size_t cur_size = GetSize();
  size_t move_start_index = cur_size / 2;
  for (size_t ii = move_start_index; ii < cur_size; ++ii) {  // move the last half of kv-pairs
    recipient->array[ii - move_start_index] = array[ii];

    // Update child pages' parent page ID.
    page_id_t child_page_id = array[ii].second;
    Page *page = buffer_pool_manager->FetchPage(child_page_id);
    BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child_page->SetParentPageId(recipient_page_id);
    buffer_pool_manager->UnpinPage(child_page_id, true /* is_dirty */);
  }

  // Update current page and recipient page metadata.
  SetSize(move_start_index);
  recipient->SetSize(cur_size - move_start_index);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  size_t old_size = GetSize();
  for (size_t ii = index; ii < old_size - 1; ++ii) {
    array[ii] = array[ii + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  assert(GetSize() == 1);
  ValueType val = ValueAt(0);
  IncreaseSize(-1);
  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * Note: current internal page is the next page of recipient page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent,
                                               BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);

  // Update parent's metadata.
  page_id_t parent_page_id = recipient->GetParentPageId();
  Page *page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  SetKeyAt(0, parent_page->KeyAt(index_in_parent));  // seperation key from parent
  buffer_pool_manager->UnpinPage(parent_page_id, true /* is_dirty */);

  // Move items to recipient, and update child pages' parent.
  int old_size = GetSize();
  int recipient_size = recipient->GetSize();
  page_id_t recipient_page_id = recipient->GetPageId();
  for (int ii = 0; ii < old_size; ++ii) {
    recipient->array[recipient_size + ii] = array[ii];

    // Update child page's parent.
    page_id_t child_page_id = array[ii].second;
    page = buffer_pool_manager->FetchPage(child_page_id);
    assert(page != nullptr);
    BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child_page->SetParentPageId(recipient_page_id);
    buffer_pool_manager->UnpinPage(child_page_id, true /* true */);
  }
  SetSize(0);
  recipient->IncreaseSize(old_size);
  assert(recipient->GetSize() <= recipient->GetMaxSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
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

  // Update child's parent page id.
  page_id_t child_page_id = kv.second;
  Page *page = buffer_pool_manager->FetchPage(child_page_id);
  assert(page != nullptr);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true /* is_dirty */);

  // Update parent's page id.
  page_id_t page_id = GetPageId();
  page_id_t parent_page_id = GetParentPageId();
  page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  int value_idx = parent_page->ValueIndex(page_id);
  parent_page->SetKeyAt(value_idx /* index */, KeyAt(0 /* index */));
  buffer_pool_manager->UnpinPage(parent_page_id, true /* is_dirty */);
}

/*
 * Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 * Invoked by MoveFirstToEndOf, move the item to the end of array.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
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

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 * Invoked by MoveLastToFrontOf, recipient add item to the front of the array; update child and parent's key.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  // Copy item to the front of array.
  int old_size = GetSize();
  assert(old_size + 1 <= GetMaxSize());
  for (int ii = 0; ii < old_size; ++ii) {
    array[ii + 1] = array[ii];
  }
  array[0] = item;
  IncreaseSize(1);

  // Update child's parent page id.
  page_id_t page_id = GetPageId();
  page_id_t child_page_id = item.second;
  Page *page = buffer_pool_manager->FetchPage(child_page_id);
  assert(page != nullptr);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child_page->SetParentPageId(page_id);
  buffer_pool_manager->UnpinPage(child_page_id, true /* is_dirty */);

  // Update parent's key range distribution(always KeyAt(0)).
  page_id_t parent_page_id = GetParentPageId();
  page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  int value_idx = parent_page->ValueIndex(page_id);
  parent_page->SetKeyAt(value_idx /* index */, KeyAt(0 /* index */));
  buffer_pool_manager->UnpinPage(parent_page_id, true /* is_dirty */);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
