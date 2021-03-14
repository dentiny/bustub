//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

using std::atomic_char;

namespace bustub {

static constexpr uint8_t BIT_MASK[8] = {0b10000000, 0b01000000, 0b00100000, 0b00010000,
                                        0b00001000, 0b00000100, 0b00000010, 0b00000001};

inline static bool GET_N_TH_BIT(const atomic_char *arr, slot_offset_t bucket_ind) {
  const auto arr_idx = bucket_ind / 8;
  const auto off_idx = bucket_ind % 8;
  return (arr[arr_idx] & BIT_MASK[off_idx]) != 0;
}

inline static void SET_N_TH_BIT(atomic_char *arr, slot_offset_t bucket_ind) {
  const auto arr_idx = bucket_ind / 8;
  const auto off_idx = bucket_ind % 8;
  arr[arr_idx] |= BIT_MASK[off_idx];
}

inline static void UNSET_N_TH_BIT(atomic_char *arr, slot_offset_t bucket_ind) {
  const auto arr_idx = bucket_ind / 8;
  const auto off_idx = bucket_ind % 8;
  arr[arr_idx] &= ~BIT_MASK[off_idx];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  if (GET_N_TH_BIT(occupied_, bucket_ind)) {
    return false;
  }
  SET_N_TH_BIT(occupied_, bucket_ind);
  SET_N_TH_BIT(readable_, bucket_ind);
  array_[bucket_ind] = std::make_pair(key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  UNSET_N_TH_BIT(readable_, bucket_ind);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  return GET_N_TH_BIT(occupied_, bucket_ind);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  return GET_N_TH_BIT(readable_, bucket_ind);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
