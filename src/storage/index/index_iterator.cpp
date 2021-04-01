/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * leaf and index within uniquely identifies an index.
 * leaf argument could be nullptr, which represents the logical end iterator.
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int idx, BufferPoolManager *buffer_pool_manager)
    : idx_(idx), leaf_(leaf), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false /* is_dirty */);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(idx_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  ++idx_;
  if (idx_ >= leaf_->GetSize()) {
    page_id_t next_leaf_page_id = leaf_->GetNextPageId();
    // The B+ tree finishes iteration, set leaf and index to default value, which is same as end() at b_plus_tree.cpp.
    if (next_leaf_page_id == INVALID_PAGE_ID) {
      leaf_ = nullptr;
      idx_ = 0;
    } else {
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false /* is_dirty */);
      Page *next_page = buffer_pool_manager_->FetchPage(next_leaf_page_id);
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page);
      idx_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
