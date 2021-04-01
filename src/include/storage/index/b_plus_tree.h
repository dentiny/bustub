//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

/*
 * Notes:
 * (1) Insert() and Remove() are required to give a transaction, while GetValue() is not.
 * (2) For Insert() and Remove(), they have to unlock pages and unpin pages fetched, call FreePageWithinTransaction()
 * at the end of execution; while for iterator fetching, they keep the reference to page, so only unlock needed.
 * (3) All public methods(Insert, Remove, GetValue) should handle cases when B+ tree's empty.
 */

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);
  INDEXITERATOR_TYPE end();

  void Print(BufferPoolManager *bpm) {
    ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
  }

  void Draw(BufferPoolManager *bpm, const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
    out << "}" << std::endl;
    out.close();
  }

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // expose for test purpose
  // Return type is LeafPage*, should reinterpret_cast<LeafPage*> when use.
  BPlusTreePage *FindLeafPage(const KeyType &key, bool leftMost = false, Transaction *transaction = nullptr,
                              OpType op = OpType::SEARCH);

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N>
  N *Split(N *node, Transaction *transaction);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr);

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  // Util invoked at CoalesceOrRedistribute() method.
  template <typename N>
  bool GetSibling(InternalPage *parent_page, N *node, N **sibling);

  // Apply Basic Latch Crabbing Protocol to fetch new page.
  BPlusTreePage *CrabbingProtocalFetchPage(page_id_t page_id, page_id_t prev_page_id, Transaction *transaction,
                                           OpType op);

  // Unlock and unpin/delete page within transaction.
  void FreePageWithinTransaction(page_id_t page_id, Transaction *transaction, bool exclusive);

  // Lock utils.
  inline void Lock(Page *page, bool exclusive) {
    assert(page != nullptr);
    if (exclusive) {
      page->WLatch();
    } else {
      page->RLatch();
    }
  }

  inline void Lock(page_id_t page_id, bool exclusive) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    Lock(page, exclusive);
  }

  inline void Unlock(Page *page, bool exclusive) {
    assert(page != nullptr);
    if (exclusive) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
  }

  inline void Unlock(page_id_t page_id, bool exclusive) {
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    Unlock(page, exclusive);
  }

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  std::atomic<page_id_t> root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
