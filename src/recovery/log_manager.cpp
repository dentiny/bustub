//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  if (enable_logging) {
    return;
  }
  enable_logging = true;
  flush_future_ = std::async([&]() {
    while (enable_logging) {
      std::unique_lock<std::mutex> lck(latch_);
      flush_cv_.wait_for(lck, log_timeout, [&]() { return request_flush_.load(); });
      assert(flush_buffer_size_ == 0);
      if (log_buffer_size_ > 0) {
        std::swap(log_buffer_size_, flush_buffer_size_);
        std::swap(log_buffer_, flush_buffer_);
        disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
        flush_buffer_size_ = 0;
        SetPersistentLSN(cur_lsn_);
      }
      request_flush_ = false;
      append_cv_.notify_all();
    }
  });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  if (!enable_logging) {
    return;
  }
  Flush(true /* is_forced */);
  enable_logging = false;
  flush_future_.get();
  assert(log_buffer_size_ == 0);
  assert(flush_buffer_size_ == 0);
}

// @param: true for group commit
// For force commit, awaken flush thread to flush log records immediately
// For group commit, block until log_timeout or other operations triggers flush
void LogManager::Flush(bool is_forced) {
  std::unique_lock<std::mutex> lck(latch_);
  if (is_forced) {
    request_flush_ = true;
    flush_cv_.notify_one();
    append_cv_.wait(lck, [&]() { return !request_flush_; });  // block until flush completes
  } else {
    append_cv_.wait(lck);
  }
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  // Make sure log_buffer_ has enough space to hold the log record.
  std::unique_lock<std::mutex> lck(latch_);
  if (log_buffer_size_ + log_record->GetSize() >= LOG_BUFFER_SIZE) {
    request_flush_ = true;
    flush_cv_.notify_one();
    append_cv_.wait(lck, [&]() { return log_buffer_size_ + log_record->GetSize() < LOG_BUFFER_SIZE; });
  }

  log_record->lsn_ = next_lsn_++;
  memmove(log_buffer_ + log_buffer_size_, log_record, LogRecord::HEADER_SIZE);
  int offset = log_buffer_size_ + LogRecord::HEADER_SIZE;  // offset of log_buffer_ to write into

  LogRecordType log_type = log_record->log_record_type_;
  if (log_type == LogRecordType::INSERT) {
    memcpy(log_buffer_ + offset, &log_record->insert_rid_, sizeof(RID));
    offset += sizeof(RID);
    log_record->insert_tuple_.SerializeTo(log_buffer_ + offset);
  } else if (log_type == LogRecordType::MARKDELETE || log_type == LogRecordType::APPLYDELETE ||
             log_type == LogRecordType::ROLLBACKDELETE) {
    memcpy(log_buffer_ + offset, &log_record->delete_rid_, sizeof(RID));
    offset += sizeof(RID);
    log_record->delete_tuple_.SerializeTo(log_buffer_ + offset);
  } else if (log_type == LogRecordType::UPDATE) {
    memcpy(log_buffer_ + offset, &log_record->update_rid_, sizeof(RID));
    offset += sizeof(RID);
    log_record->old_tuple_.SerializeTo(log_buffer_ + offset);
    offset += static_cast<int>(log_record->old_tuple_.GetLength()) + sizeof(int32_t);
    log_record->new_tuple_.SerializeTo(log_buffer_ + offset);
  } else if (log_type == LogRecordType::NEWPAGE) {
    memcpy(log_buffer_ + offset, &log_record->prev_page_id_, sizeof(page_id_t));
    offset += sizeof(page_id_t);
    memcpy(log_buffer_ + offset, &log_record->page_id_, sizeof(page_id_t));
  }
  log_buffer_size_ += log_record->GetSize();
  cur_lsn_ = log_record->lsn_;
  return cur_lsn_;
}

}  // namespace bustub
