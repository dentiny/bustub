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
  } else if (log_type == LogRecordType::MARKDELETE ||
             log_type == LogRecordType::APPLYDELETE ||
             log_type == LogRecordType::ROLLBACKDELETE) {
    log_record->delete_rid_ = *reinterpret_cast<const RID *>(data);
    log_record->delete_tuple_.DeserializeFrom(data + sizeof(RID));
  } else {
    assert(0);
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
 * (1) load log from storage via disk manager, the log file offset is indicated by log_file_offset_
 * (2) deserialize and redo log record by record. Since log record could be partial, either due to 
 * incomplete logging, or log loading this time breaks the originally complete log record apart. 
 * Move the partial log record to the front of the log_buffer, and continue read from storage after
 * it next time. The buffer offset is indicated by log_buffer_offset.
 *
 * For different types for logging:
 * (1) BEGIN: ignore
 * (2) ABORT/COMMIT: the redo will be completed elsewhere
 * (3) NEWPAGE: set current and prev table page's header
 * (4) UPDATE: table_page->UpdateTuple
 * (5) INSERT: table_page->InsertTuple
 * (6) MARKDELETE: table_page->MarkDelete
 * (7) APPLYDELETE: table_page->APPLYDELETE
 * (8) ROLLBACKDELETE: table_page->ROLLBACKDELETE
 */
void LogRecovery::Redo() {
  assert(!enable_logging);
  log_file_offset_ = 0;
  int log_buffer_offset = 0;  // data in front of log_buffer_offset is previously loaded partial log record
  while (disk_manager_->ReadLog(log_buffer_ + log_buffer_offset, LOG_BUFFER_SIZE - log_buffer_offset, log_file_offset_)) {
    LogRecord log_record;
    log_buffer_offset = 0;
    while (DeserializeLogRecord(log_buffer_ + log_buffer_offset, &log_record)) {
      lsn_t log_lsn = log_record.GetLSN();
      int32_t log_size = log_record.GetSize();
      LogRecordType log_type = log_record.GetLogRecordType();
      lsn_mapping_[log_lsn] = log_file_offset_ + log_buffer_offset;
      log_file_offset_ += LOG_BUFFER_SIZE - log_buffer_offset;
      log_buffer_offset += log_size;
      assert(log_type != LogRecordType::INVALID);

      // Special operation: BEGIN, ABORT, COMMIT, NEWPAGE
      if (log_type == LogRecordType::BEGIN) {
        continue;
      } else if (log_type == LogRecordType::COMMIT || log_type == LogRecordType::ABORT) {
        txn_id_t log_txn_id = log_record.GetTxnId();
        assert(active_txn_.find(log_txn_id) != active_txn_.end());
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
      RID rid = log_type == LogRecordType::UPDATE ? log_record.update_rid_ :
                log_type == LogRecordType::INSERT ? log_record.insert_rid_ :
                log_record.delete_rid_;
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
          assert(0);
        }
        table_page->SetLSN(log_lsn);
      }
      buffer_pool_manager_->UnpinPage(page_id, cur_page_redo /* is_dirty */);
    }

    // Move the partial log record to the front of the log_buffer.
    memcpy(log_buffer_, log_buffer_ + log_buffer_offset, LOG_BUFFER_SIZE - log_buffer_offset);
    log_buffer_offset = LOG_BUFFER_SIZE - log_buffer_offset;
  }
}

/*
 * undo phase on TABLE PAGE level(table/table_page.h)
 * iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  assert(!enable_logging);
}

}  // namespace bustub
