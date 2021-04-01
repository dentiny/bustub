//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
  // Check whether complete log record header could be loaded.
  if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE) {
    return false;
  }

  // Copy log record header and perform integrity checking.
  memcpy(log_record, data, LogRecord::HEADER_SIZE);
  if (log_record->size_ <= 0 || data + log_record->size_ > log_buffer_ + LOG_BUFFER_SIZE) {
    return false;
  }

  data += LogRecord::HEADER_SIZE;
  LogRecordType log_type = log_record->GetLogRecordType();
  assert(log_type != LogRecordType::INVALID);
  if (log_type == LogRecordType::BEGIN || log_type == LogRecordType::COMMIT || log_type == LogRecordType::ABORT) {
    // nothing to do
  } else if (log_type == LogRecordType::NEWPAGE) {
    log_record->prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
    log_record->page_id_ = *reinterpret_cast<const page_id_t *>(data + sizeof(page_id_t));
  } else if (log_type == LogRecordType::UPDATE) {
    log_record->update_rid_ = *reinterpret_cast<const RID *>(data);
    log_record->old_tuple_.DeserializeFrom(data + sizeof(RID));
    log_record->new_tuple_.DeserializeFrom(data + sizeof(RID) + sizeof(uint32_t) + log_record->old_tuple_.GetLength());
  } else if (log_type == LogRecordType::INSERT) {
    log_record->insert_rid_ = *reinterpret_cast<const RID *>(data);
    log_record->insert_tuple_.DeserializeFrom(data + sizeof(RID));
  } else if (log_type == LogRecordType::MARKDELETE || log_type == LogRecordType::APPLYDELETE ||
             log_type == LogRecordType::ROLLBACKDELETE) {
    log_record->delete_rid_ = *reinterpret_cast<const RID *>(data);
    log_record->delete_tuple_.DeserializeFrom(data + sizeof(RID));
  } else {
    BUSTUB_ASSERT(0, "Unknown log record type");
  }

  return true;
}

/*
 * redo phase on TABLE PAGE level(table/table_page.h)
 * read log file from the beginning to end (you must prefetch log records into
 * log buffer to reduce unnecessary I/O operations), remember to compare page's
 * LSN with log_record's sequence number, and also build active_txn_ table &
 * lsn_mapping_ table
 *
 * How to buffer log:
 * (1) load log from storage via disk manager, the log file offset is indicated by log_file_offset
 * (2) deserialize and redo log record by record. Since log record could be partial, either due to
 * incomplete logging, or log loading this time breaks the originally complete log record apart.
 * Move the partial log record to the front of the log_buffer, and continue read from storage after
 * it next time. The buffer offset is indicated by log_buffer_offset.
 *
 * For different types for logging redo:
 * (1) BEGIN: ignore
 * (2) ABORT/COMMIT: the redo will be completed elsewhere
 * (3) NEWPAGE: set current and prev table page's header
 * (4) UPDATE: table_page->UpdateTuple
 * (5) INSERT: table_page->InsertTuple
 * (6) MARKDELETE: table_page->MarkDelete
 * (7) APPLYDELETE: table_page->ApplyDelete
 * (8) ROLLBACKDELETE: table_page->RollbackDelete
 *
 * Three offset variables:
 * (1) log_file_offset: offset of logging file, for DiskManager->ReadLog to begin reading
 * (2) log_buffer_offset: data in front of log_buffer_offset is previously loaded partial log record
 */
void LogRecovery::Redo() {
  assert(!enable_logging);
  int log_file_offset = 0;
  int log_buffer_offset = 0;  // data in front of log_buffer_offset is previously loaded partial log record
  while (
      disk_manager_->ReadLog(log_buffer_ + log_buffer_offset, LOG_BUFFER_SIZE - log_buffer_offset, log_file_offset)) {
    int buffer_start = log_file_offset;
    log_file_offset += LOG_BUFFER_SIZE - log_buffer_offset;
    log_buffer_offset = 0;
    LogRecord log_record;
    while (DeserializeLogRecord(log_buffer_ + log_buffer_offset, &log_record)) {
      lsn_t log_lsn = log_record.GetLSN();
      int32_t log_size = log_record.GetSize();
      txn_id_t log_txn_id = log_record.GetTxnId();
      LogRecordType log_type = log_record.GetLogRecordType();
      active_txn_[log_txn_id] = log_lsn;
      lsn_mapping_[log_lsn] = buffer_start + log_buffer_offset;
      log_buffer_offset += log_size;
      assert(log_type != LogRecordType::INVALID);

      if (log_type == LogRecordType::BEGIN) {
        continue;
      } else if (log_type == LogRecordType::COMMIT || log_type == LogRecordType::ABORT) {
        active_txn_.erase(log_txn_id);
        continue;
      } else if (log_type == LogRecordType::NEWPAGE) {
        page_id_t cur_page_id = log_record.page_id_;
        page_id_t prev_page_id = log_record.prev_page_id_;
        Page *page = buffer_pool_manager_->FetchPage(cur_page_id);
        assert(page != nullptr);
        TablePage *table_page = static_cast<TablePage *>(page);
        lsn_t page_lsn = page->GetLSN();
        bool cur_page_redo = log_lsn > page_lsn;
        if (cur_page_redo) {
          // Redo current TablePage.
          table_page->Init(cur_page_id, PAGE_SIZE, prev_page_id, nullptr /* log_manager */, nullptr /* transaction */);
          table_page->SetLSN(log_lsn);

          // Redo prev TablePage.
          if (prev_page_id != INVALID_PAGE_ID) {
            Page *prev_page = buffer_pool_manager_->FetchPage(prev_page_id);
            assert(prev_page != nullptr);
            TablePage *prev_table_page = static_cast<TablePage *>(prev_page);
            bool update_next_page = prev_table_page->GetNextPageId() == cur_page_id;
            prev_table_page->SetNextPageId(cur_page_id);
            buffer_pool_manager_->UnpinPage(prev_page_id, update_next_page /* is_dirty */);
          }
        }
        buffer_pool_manager_->UnpinPage(cur_page_id, cur_page_redo /* is_dirty */);
        continue;
      }

      // Redo specific operation: UPDATE, INSERT, DELETE(MARKDELETE, APPLYDELETE, ROLLBACKDELETE)
      RID rid = log_type == LogRecordType::UPDATE
                    ? log_record.update_rid_
                    : log_type == LogRecordType::INSERT ? log_record.insert_rid_ : log_record.delete_rid_;
      page_id_t page_id = rid.GetPageId();
      Page *page = buffer_pool_manager_->FetchPage(page_id);
      assert(page != nullptr);
      TablePage *table_page = static_cast<TablePage *>(page);
      lsn_t page_lsn = table_page->GetLSN();
      bool cur_page_redo = log_lsn > page_lsn;
      if (cur_page_redo) {
        if (log_type == LogRecordType::UPDATE) {
          table_page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
        } else if (log_type == LogRecordType::INSERT) {
          table_page->InsertTuple(log_record.insert_tuple_, &rid, nullptr, nullptr, nullptr);
        } else if (log_type == LogRecordType::MARKDELETE) {
          table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
        } else if (log_type == LogRecordType::APPLYDELETE) {
          table_page->ApplyDelete(rid, nullptr, nullptr);
        } else if (log_type == LogRecordType::ROLLBACKDELETE) {
          table_page->RollbackDelete(rid, nullptr, nullptr);
        } else {
          BUSTUB_ASSERT(0, "Unknown log record type");
        }
        table_page->SetLSN(log_lsn);
      }
      buffer_pool_manager_->UnpinPage(page_id, cur_page_redo /* is_dirty */);
    }

    // Move the partial log record to the front of the log_buffer.
    // Note: memcpy() doesn't allow overlapping memory, memmove() does.
    memmove(log_buffer_, log_buffer_ + log_buffer_offset, LOG_BUFFER_SIZE - log_buffer_offset);
    log_buffer_offset = LOG_BUFFER_SIZE - log_buffer_offset;
  }
}

/*
 * undo phase on TABLE PAGE level(table/table_page.h)
 * iterate through active txn map and undo each operation
 *
 * For different types for logging undo:
 * (1) BEGIN: ignore
 * (2) ABORT/COMMIT: Redo() method error
 * (3) NEWPAGE: BufferPoolManager->DeletePage
 * (4) UPDATE: TablePage->Update to recover old tuple
 * (5) INSERT: TablePage->ApplyDelete
 * (6) MARKDELETE: TablePage->RollbackDelete
 * (7) APPLYDELETE: TablePage->InsertTuple
 * (8) ROLLBACKDELETE: TablePage->MarkDelete
 */
void LogRecovery::Undo() {
  assert(!enable_logging);
  for (auto &txn_lsn_pair : active_txn_) {
    lsn_t lsn = txn_lsn_pair.second;
    assert(lsn != INVALID_LSN);
    LogRecord log_record;
    assert(lsn_mapping_.find(lsn) != lsn_mapping_.end());
    disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, lsn_mapping_[lsn]);
    assert(DeserializeLogRecord(log_buffer_, &log_record));

    // lsn_t log_lsn = log_record.GetLSN();
    // int32_t log_size = log_record.GetSize();
    // txn_id_t log_txn_id = log_record.GetTxnId();
    page_id_t cur_page_id = log_record.page_id_;
    page_id_t prev_page_id = log_record.prev_page_id_;
    LogRecordType log_type = log_record.GetLogRecordType();

    if (log_type == LogRecordType::BEGIN) {
      assert(prev_page_id == INVALID_LSN);
      continue;
    } else if (log_type == LogRecordType::COMMIT || log_type == LogRecordType::ABORT) {
      BUSTUB_ASSERT(0, "Transaction should have been committed or aborted.");
    } else if (log_type == LogRecordType::NEWPAGE) {
      assert(buffer_pool_manager_->DeletePage(cur_page_id));
      if (prev_page_id != INVALID_PAGE_ID) {
        // Suppose we have page1 and page2. page1 has already been deleted due to undo, page2's prev page
        // is not guarenteed to exist.
        Page *page = buffer_pool_manager_->FetchPage(prev_page_id);
        if (page != nullptr) {
          TablePage *table_page = static_cast<TablePage *>(page);
          table_page->SetNextPageId(INVALID_PAGE_ID);
          buffer_pool_manager_->UnpinPage(prev_page_id, true /* is_dirty */);
        }
      }
      continue;
    }

    RID rid = log_type == LogRecordType::UPDATE
                  ? log_record.update_rid_
                  : log_type == LogRecordType::INSERT ? log_record.insert_rid_ : log_record.delete_rid_;
    page_id_t page_id = rid.GetPageId();
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    TablePage *table_page = static_cast<TablePage *>(page);
    // lsn_t page_lsn = table_page->GetLSN();
    if (log_type == LogRecordType::UPDATE) {
      Tuple tuple;  // placeholder
      table_page->UpdateTuple(log_record.old_tuple_, &tuple, rid, nullptr, nullptr, nullptr);
    } else if (log_type == LogRecordType::INSERT) {
      table_page->ApplyDelete(rid, nullptr, nullptr);
    } else if (log_type == LogRecordType::MARKDELETE) {
      table_page->RollbackDelete(rid, nullptr, nullptr);
    } else if (log_type == LogRecordType::APPLYDELETE) {
      table_page->InsertTuple(log_record.delete_tuple_, &rid, nullptr, nullptr, nullptr);
    } else if (log_type == LogRecordType::ROLLBACKDELETE) {
      table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
    } else {
      BUSTUB_ASSERT(0, "Unknown log record type");
    }
  }
  active_txn_.clear();
  lsn_mapping_.clear();
}

}  // namespace bustub
