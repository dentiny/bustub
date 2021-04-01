//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.h
//
// Identification: src/include/recovery/log_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <future>              // NOLINT
#include <mutex>               // NOLINT

#include "recovery/log_record.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
class LogManager {
 public:
  explicit LogManager(DiskManager *disk_manager)
      : next_lsn_(0),
        persistent_lsn_(INVALID_LSN),
        cur_lsn_{INVALID_LSN},
        log_buffer_size_{0},
        flush_buffer_size_{0},
        disk_manager_(disk_manager) {
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }

  void Flush(bool is_forced);  // Invoked within StopFlushThread() and BufferPoolManager.
  void RunFlushThread();
  void StopFlushThread();

  lsn_t AppendLogRecord(LogRecord *log_record);

  inline lsn_t GetNextLSN() { return next_lsn_; }
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }

 private:
  /** The atomic counter which records the next log sequence number. */
  std::atomic<lsn_t> next_lsn_;
  /** The log records before and including the persistent lsn have been written to disk. */
  std::atomic<lsn_t> persistent_lsn_;
  /** The log records updated by AppendLogRecord(), assigned to persistent_lsn_ when flush. */
  std::atomic<lsn_t> cur_lsn_;

  int log_buffer_size_;
  int flush_buffer_size_;

  char *log_buffer_;
  char *flush_buffer_;

  DiskManager *disk_manager_ __attribute__((__unused__));

  std::mutex latch_;
  std::condition_variable flush_cv_;
  std::condition_variable append_cv_;
  std::future<void> flush_future_;
  // std::thread *flush_thread_ __attribute__((__unused__));

  // Set true by AppendLogRecord() method.
  std::atomic<bool> request_flush_;
};

}  // namespace bustub
