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
  // Check whether log record complete.
  if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE) {
    return false;
  }

  memcpy(log_)
}




if (data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE)
    return false;
  memcpy(&log_record, data, LogRecord::HEADER_SIZE); //COPY HEADER
  if (log_record.size_ <= 0 || data + log_record.size_ > log_buffer_ + LOG_BUFFER_SIZE)
    return false;
  data += LogRecord::HEADER_SIZE;
  switch (log_record.log_record_type_) {
    case LogRecordType::INSERT:
      log_record.insert_rid_ = *reinterpret_cast<const RID *>(data);
      log_record.insert_tuple_.DeserializeFrom(data + sizeof(RID));
      break;
    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      log_record.delete_rid_ = *reinterpret_cast<const RID *>(data);
      log_record.delete_tuple_.DeserializeFrom(data + sizeof(RID));
      break;
    case LogRecordType::UPDATE:
      log_record.update_rid_ = *reinterpret_cast<const RID *>(data);
      log_record.old_tuple_.DeserializeFrom(data + sizeof(RID));
      log_record.new_tuple_.DeserializeFrom(data + sizeof(RID) + 4 + log_record.old_tuple_.GetLength());
      break;
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;
    case LogRecordType::NEWPAGE:
      log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
      log_record.page_id_ = *reinterpret_cast<const page_id_t *>(data + sizeof(page_id_t));
      break;
    default:
      assert(false);
  }
  return true;




/*
 * redo phase on TABLE PAGE level(table/table_page.h)
 * read log file from the beginning to end (you must prefetch log records into
 * log buffer to reduce unnecessary I/O operations), remember to compare page's
 * LSN with log_record's sequence number, and also build active_txn_ table &
 * lsn_mapping_ table
 *
 * For different types for logging:
 * (1) BEGIN: ignore
 * (2) ABORT/COMMIT: the redo will be completed elsewhere
 * (3) NEWPAGE: set current and prev table page's header
 */
void LogRecovery::Redo() {
  assert(!enable_logging);
  log_file_offset_ = 0;
  while (disk_manager_->ReadLog(log_buffer_ + log_buffer_offset_, LOG_BUFFER_SIZE, log_file_offset_)) {
    int buffer_start_pos = log_file_offset_;
    log_file_offset_ += LOG_BUFFER_SIZE;
    LogRecord log_record;
    int log_buffer_offset = 0;
    while (DeserializeLogRecord(log_buffer_ + log_buffer_offset, &log_record)) {
      lsn_t log_lsn = log_record.GetLSN();
      int32_t log_size = log_record.GetSize();
      txn_id_t log_txn_id = log_record.GetTxnId();
      LogRecordType log_type = log_record.GetLogRecordType();
      lsn_mapping_[log_lsn] = log_file_offset_;
      log_buffer_offset += log_size;
      if (log_type == LogRecordType::BEGIN) {
        continue;
      } else if (log_type == LogRecordType::COMMIT || log_type == LogRecordType::ABORT) {
        assert(active_txn_.find(log_txn_id) != active_txn_.end());
        active_txn_.erase(log_txn_id);
      } else if (log_type == LogRecordType::NEWPAGE) {
        page_id_t cur_page_id = log_record.page_id_;
        page_id_t prev_page_id = log_record.prev_page_id;
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
    }
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
