#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    // names_: <table_name, table_oid>
    table_oid_t cur_table_oid = next_table_oid_++;
    names_[table_name] = cur_table_oid;

    // tables_: <table_oid, unique_ptr<TableMetadata*>>
    std::unique_ptr<TableHeap> table_heap = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);
    std::unique_ptr<TableMetadata> table_metadata =
        std::make_unique<TableMetadata>(schema, table_name, std::move(table_heap), cur_table_oid);
    tables_[cur_table_oid] = std::move(table_metadata);
    return tables_[cur_table_oid].get();
  }

  /** @return table metadata by name */
  // (1) Get table_oid by table_name in names_.
  // (2) Call GetTable(table_oid) to get TableMetadata* in tables_.
  TableMetadata *GetTable(const std::string &table_name) {
    const auto &it = names_.find(table_name);
    if (it == names_.end()) {
      throw std::out_of_range{"GetTable() method: " + table_name + " not found!"};
    }
    return GetTable(it->second);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    const auto &it = tables_.find(table_oid);
    if (it == tables_.end()) {
      throw std::out_of_range{"GetTable() method: table oid " + std::to_string(table_oid) + " not found!"};
    }
    return it->second.get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    // Make sure acccessed table exists.
    try {
      GetTable(table_name);
    } catch (std::out_of_range &e) {
      CreateTable(txn, table_name, schema);
    }

    // Create IndexMetadata, Index, and IndexInfo
    IndexMetadata *index_metadata = new IndexMetadata(index_name, table_name, &schema, key_attrs);
    Index *b_plus_index = new BPLUSTREE_INDEX_TYPE(index_metadata, bpm_);
    IndexInfo *index_info = new IndexInfo(key_schema, index_name, std::unique_ptr<Index>(b_plus_index),
                                          next_index_oid_++, table_name, keysize);

    // Make sure table_name exists at index_names_. <table_name, <index_name, index_oid>>
    auto it = index_names_.find(table_name);
    if (it == index_names_.end()) {
      index_names_.insert(std::make_pair(table_name, std::unordered_map<std::string, index_oid_t>{}));
      it = index_names_.find(table_name);
    }

    // Insert into index_names_, indexes_.
    auto &index_name_oid_map = it->second;  // <index_name, index_oid>
    BUSTUB_ASSERT(index_name_oid_map.count(index_name) == 0, "index names should be unique!");
    index_name_oid_map.insert(std::make_pair(index_name, index_info->index_oid_));
    indexes_.insert(std::make_pair(index_info->index_oid_, std::unique_ptr<IndexInfo>(index_info)));
    return index_info;
  }

  // (1) Get table_oid by table_name and index_name in index_names_.
  // (2) Call GetIndex(table_oid) to get IndexInfo* by table_oid in indexes_.
  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    const auto &it1 = index_names_.find(table_name);
    if (it1 == index_names_.end()) {
      throw std::out_of_range{"GetIndex() method: " + table_name + " not found!"};
    }
    const auto &index_oid_map = it1->second;  // <index_name, table_oid>
    const auto &it2 = index_oid_map.find(index_name);
    if (it2 == index_oid_map.end()) {
      throw std::out_of_range{"GetIndex() method: " + index_name + " not found!"};
    }
    return GetIndex(it2->second);
  }

  // Get IndexInfo* by table_oid in indexes_.
  IndexInfo *GetIndex(index_oid_t index_oid) {
    const auto &it = indexes_.find(index_oid);
    if (it == indexes_.end()) {
      throw std::out_of_range{"index Id of " + std::to_string(index_oid) + " not found!"};
    }
    return it->second.get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> index_info;
    auto it = index_names_.find(table_name);
    if (it != index_names_.end()) {
      auto &index_name_oid_map = it->second;  // <index_name, index_oid>
      for (auto &index_name_oid_pair : index_name_oid_map) {
        index_info.push_back(GetIndex(index_name_oid_pair.second));
      }
    }
    return index_info;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
