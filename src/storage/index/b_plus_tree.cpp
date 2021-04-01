//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_{INVALID_PAGE_ID},
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 * Note: GetValue() method does not guarentee a transaction.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, false /* leftmost */, transaction));
  assert(leaf_page != nullptr);
  result->resize(1);
  bool lookup_res = leaf_page->Lookup(key, result->data(), comparator_);
  FreePageWithinTransaction(leaf_page->GetPageId(), transaction, false /* exclusive */);
  return lookup_res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  assert(transaction != nullptr);
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // Allocate a new page for leaf page.
  assert(IsEmpty());
  page_id_t root_page_id = INVALID_PAGE_ID;  // placeholder
  Page *page = buffer_pool_manager_->NewPage(&root_page_id);
  root_page_id_ = root_page_id;
  assert(page != nullptr);

  // Initialize leaf page metadata.
  LeafPage *root_page = reinterpret_cast<LeafPage *>(page->GetData());
  root_page->Init(root_page_id_, INVALID_PAGE_ID /* parent_id */, leaf_max_size_);  // max_size takes it default value
  UpdateRootPageId(1 /* insert */);  // create a new record<index_name + root_page_id> in header_page

  // Insert kv-pair, and unpin root page(which is pinned via NewPage).
  root_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true /* is_dirty */);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  assert(transaction != nullptr);

  LeafPage *leaf_page =
      reinterpret_cast<LeafPage *>(FindLeafPage(key, false /* leftmost */, transaction, OpType::INSERT));
  assert(leaf_page != nullptr);

  // DB B+ Tree only supports unique key.
  ValueType v;  // placeholder
  bool kv_already_exist = leaf_page->Lookup(key, &v, comparator_);
  if (kv_already_exist) {
    FreePageWithinTransaction(leaf_page->GetPageId(), transaction, true /* exclusive */);
    return false;
  }

  // Insert kv-pair into leaf page, check if it should split.
  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() > leaf_page->GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_page, transaction);
    assert(new_leaf_page != nullptr);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0) /* new key in parent */, new_leaf_page, transaction);
  }
  FreePageWithinTransaction(leaf_page->GetPageId(), transaction, true /* exclusive */);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  assert(node != nullptr);
  assert(transaction != nullptr);

  // Allocate a new page for the new node.
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page != nullptr);

  new_page->WLatch();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(new_page);
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 *
 * Note: old_page is unpinned by caller(InsertIntoLeaf or InsertIntoParent),
 * new_page and other related pages are unpinned by InsertIntoParent
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  assert(old_node != nullptr);
  assert(new_node != nullptr);
  assert(transaction != nullptr);

  // If old node is the root page of B+ tree, allocate parent page and update root page id.
  if (old_node->IsRootPage()) {
    page_id_t root_page_id = INVALID_PAGE_ID;  // placeholder
    Page *page = buffer_pool_manager_->NewPage(&root_page_id);
    root_page_id_ = root_page_id;
    assert(page != nullptr);
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID /* parent_page_id */, internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0 /* update */);
    // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true /* is_dirty */);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true /* is_dirty */);
    return;
  }

  // If parent node already exists, update new page's metadata and insert new key into parent.
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(page != nullptr);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  new_node->SetParentPageId(parent_page_id);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true /* is_dirty */);
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  // old_value, new_key, new_value

  // If parent page exceeeds its capacity, need to split.
  if (parent_page->GetSize() > parent_page->GetMaxSize()) {
    InternalPage *new_internal_page = Split(parent_page, transaction);
    InsertIntoParent(parent_page, new_internal_page->KeyAt(0), new_internal_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true /* is_dirty */);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  assert(transaction != nullptr);
  if (IsEmpty()) {
    return;
  }

  LeafPage *leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, false /* leftmost */, transaction, OpType::DELETE));
  int cur_size = leaf->RemoveAndDeleteRecord(key, comparator_);
  if (cur_size < leaf->GetMinSize()) {
    CoalesceOrRedistribute(leaf, transaction);
  }
  FreePageWithinTransaction(leaf->GetPageId(), transaction, true /* exclusive */);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  /*
    (1) The page falls down of min size is root page, it could be either leaf or internal page.
    (2) If sibling's size + input page's size <= page's max size, then coalesce.
    (3) If sibling's size + input page's size > page's max size, then redistribute.
  */

  assert(node != nullptr);
  assert(transaction != nullptr);
  page_id_t page_id = node->GetPageId();

  // case1: The page falls down of min size is root page, it could be either leaf or internal page
  // If the target node is to be deleted, add it to transaction's deleted_page_set_, clear it at
  // FreePageWithinTransaction() method.
  if (node->IsRootPage()) {
    bool to_delete_node = AdjustRoot(node);
    if (to_delete_node) {
      transaction->AddIntoDeletedPageSet(page_id);
    }
    return to_delete_node;
  }

  // Fetch parent page.
  page_id_t parent_page_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(page != nullptr);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  // Find the sibling node of input page.
  N *sibling_page = nullptr;
  bool is_sibling_prev = GetSibling(parent_page, node, &sibling_page);
  assert(sibling_page != nullptr);
  N *prev_node = is_sibling_prev ? sibling_page : node;
  N *next_node = is_sibling_prev ? node : sibling_page;
  page_id_t prev_page_id = prev_node->GetPageId();
  // page_id_t next_page_id = next_node->GetPageId();

  // case2: If sibling's size + input page's size <= page's max size, then merge.
  // Remove all items within next_node into prev_node.
  if (prev_node->GetSize() + next_node->GetSize() <= next_node->GetSize()) {
    // (1) Both prev_node and next_node is unpinned within Coalesce() method.
    // (2) Parent page may involve chaining Coalesce/Redistribution, so it's unpinned here.
    int index_in_parent = parent_page->ValueIndex(prev_page_id);
    Coalesce(&prev_node, &next_node, &parent_page, index_in_parent, transaction);
    buffer_pool_manager_->UnpinPage(parent_page_id, true /* is_dirty */);
    return true;
  }

  // case3: If sibling's size + input page's size > page's max size, then redistribute.
  // (1) If node_in_parent_index = 0, sibling->MoveFirstToEndOf(node);
  // If node_in_parent_index != 0, sibling->MoveLastToFirstOf(node);
  // (2) Both input node and sibling node is unpinned within Redistribute() method.
  // (3) Parent page is unpinned here, and it's already flushed back to disk within MoveFirstToEndOf()
  // and MoveLastToFirstOf() method.
  int node_in_parent_index = parent_page->ValueIndex(page_id);
  Redistribute(sibling_page, node, node_in_parent_index);
  buffer_pool_manager_->UnpinPage(parent_page_id, false /* is_dirty */);
  return false;
}

/*
 * Util method invoked only within CoalesceOrRedistribute() method.
 * @return: true means sibling is previous node, false means it's next node.
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::GetSibling(InternalPage *parent_page, N *node, N **sibling) {
  assert(parent_page != nullptr);
  assert(node != nullptr);
  assert(!node->IsRootPage());

  // Fetch sibling page.
  page_id_t page_id = node->GetPageId();
  page_id_t parent_page_id = parent_page->GetPageId();
  int index = parent_page->ValueIndex(page_id);
  int sibling_index = index == 0 ? 1 : (index - 1);
  page_id_t sibling_page_id = parent_page->ValueAt(sibling_index);
  Page *page = buffer_pool_manager_->FetchPage(sibling_page_id);
  assert(page != nullptr);
  *sibling = reinterpret_cast<N *>(page->GetData());

  // Unpin parent page.
  buffer_pool_manager_->UnpinPage(parent_page_id, false /* is_dirty */);
  return sibling_index < index;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index_in_parent,
                              Transaction *transaction) {
  assert(transaction != nullptr);

  // Note:
  // (1) Both pages are unpinned not in MoveAllTo() method. Instead they're unpinned here.
  // (2) neighbor_node(recipient) is the previous node as node(method issuer).
  N *issuer_page = *node;
  N *recipient_page = *neighbor_node;
  issuer_page->MoveAllTo(recipient_page, index_in_parent, buffer_pool_manager_);

  // Unpin both issure and recipient pages.
  page_id_t issuer_page_id = issuer_page->GetPageId();
  page_id_t recipient_page_id = recipient_page->GetPageId();
  buffer_pool_manager_->UnpinPage(recipient_page_id, true /* is_dirty */);
  buffer_pool_manager_->UnpinPage(issuer_page_id, true /* is_dirty */);
  transaction->AddIntoDeletedPageSet(issuer_page_id);

  // Update parent page, and decide whether further coalesce and redistribution is needed.
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page = *parent;
  parent_page->Remove(index_in_parent);
  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    return CoalesceOrRedistribute(parent_page, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * Note:
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  assert(neighbor_node != nullptr);
  assert(node != nullptr);

  // Note:
  // (1) Both MoveFirstToEndOf() and MoveLastToFrontOf() method don't unpin any of input pages.
  // (2) Instead, they are both unpinned here.
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, buffer_pool_manager_);
  }
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true /* is_dirty */);
  // buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true /* is_dirty */);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  assert(old_root_node != nullptr);

  // case 1: when you delete the last element in root page, but root page still has one last child
  // Note: root page is an internal page
  if (old_root_node->GetSize() == 1) {
    // Set the only child as new root
    InternalPage *old_root_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_page_id = old_root_page->RemoveAndReturnOnlyChild();
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0 /* update */);

    // Update child page's parent page id
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    assert(page != nullptr);
    BPlusTreePage *new_root_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true /* is_dirty */);

    // Delete current root page
    // buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false /* is_dirty */);
    // buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  }

  // case 2: when you delete the last element in whole b+ tree
  // Note: root page is a leaf page
  if (old_root_node->IsLeafPage()) {
    assert(old_root_node->GetSize() == 0);
    assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    // buffer_pool_manager_->UnpinPage(root_page_id_, false /* is_dirty */);
    // buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0 /* update */);
    return true;
  }

  // Leaf page is allowed to have kv-pairs less than MinSize.
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key{};  // placeholder
  LeafPage *leftmost_leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, true /* leftmost */));
  Unlock(leftmost_leaf->GetPageId(), false /* exclusive */);
  return INDEXITERATOR_TYPE(leftmost_leaf, 0 /* index */, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LeafPage *leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key));
  if (leaf == nullptr) {
    return INDEXITERATOR_TYPE(nullptr /* leaf */, 0 /* index */, buffer_pool_manager_);
  }
  int idx = leaf->KeyIndex(key, comparator_);
  Unlock(leaf->GetPageId(), false /* exclusive */);
  return INDEXITERATOR_TYPE(leaf, idx, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0 /* index */, buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 *
 * Note:
 * (1) leftmost is used when fetch begin iterator.
 * (2) This method does not guarentee a transaction.
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Transaction *transaction, OpType op) {
  assert(!IsEmpty());  // FindLeafPage should be called after the B+ tree is constructed

  page_id_t cur_page_id = root_page_id_;
  BPlusTreePage *cur_page = CrabbingProtocalFetchPage(cur_page_id, INVALID_PAGE_ID /* prev_page */, transaction, op);
  for (; !cur_page->IsLeafPage();) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(cur_page);
    page_id_t new_page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, comparator_);
    cur_page = CrabbingProtocalFetchPage(new_page_id, cur_page_id, transaction, op);
    cur_page_id = new_page_id;
  }
  return cur_page;
}

/*
 * Basic Latch Crabbing Protocol:
 * Search: Start at root and go down, repeatedly acquire latch on child and then unlatch parent.
 * Insert/Delete: Start at root and go down, obtaining X latches as needed. Once child is latched, check
 * if it is safe. If the child is safe, release latches on all its ancestors.
 *
 * (1) Lock current page.
 * (2) Release all previous latched pages if no chaining exclusive operations.
 * (3) (For transaction)Add current page into transaction latched page set.
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabbingProtocalFetchPage(page_id_t page_id, page_id_t prev_page_id,
                                                         Transaction *transaction, OpType op) {
  bool exclusive = op != OpType::SEARCH;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  assert(page != nullptr);
  Lock(page, exclusive);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (prev_page_id != INVALID_PAGE_ID && tree_page->IsSafeOp(op)) {
    // With no chaining exclusive operation, free all previous latched pages.
    FreePageWithinTransaction(prev_page_id, transaction, exclusive);
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  return tree_page;
}

/*
 * Unlock and unpin/delete page within transaction.
 * Note: FreePageWithinTransaction() method does not guarentee a transaction.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePageWithinTransaction(page_id_t page_id, Transaction *transaction, bool exclusive) {
  // If there's no transaction, unpin current page directly.
  if (transaction == nullptr) {
    Unlock(page_id, exclusive);
    buffer_pool_manager_->UnpinPage(page_id, exclusive);
    return;
  }

  // For transaction, release latches on all ancestors.
  auto pages_under_latch = transaction->GetPageSet();
  auto pages_to_delete = transaction->GetDeletedPageSet();
  for (Page *page : *pages_under_latch) {
    page_id_t cur_page_id = page->GetPageId();
    Unlock(cur_page_id, exclusive);
    buffer_pool_manager_->UnpinPage(cur_page_id, exclusive);
    if (transaction->IsWithinDeletedPageSet(cur_page_id)) {
      buffer_pool_manager_->DeletePage(cur_page_id);
      transaction->RemoveFromDeletedPageSet(cur_page_id);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->ClearPageSet();
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
