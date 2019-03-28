/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {

    // the flushing thread is already running
    if (ENABLE_LOGGING) return;

    ENABLE_LOGGING = true;



    flush_thread_ = new std::thread([&](){

        // keep looping
        while (ENABLE_LOGGING) {
            // TODO: if we create the lock outside the loop, then the wait fails. Why?
            std::unique_lock<std::mutex> lock(latch_);

            const std::cv_status cv_status = cv_.wait_for(lock, LOG_TIMEOUT);

            // even if the ENABLE_LOGGING is set to false, still need to flush everything before existing, hence it
            // is better to simply put ENABLE_LOGGING inside the while loop

            // buffer is empty, continue
            if (log_buffer_offset_ == 0) {
                continue;
            }

            // the thread is awaken due to timeout, swap the two buffers
            // Here the swap is done only for timeout, because the swap is already done for other awakening signal
            if (cv_status == std::cv_status::timeout) {
                SwapBuffer();
            }

            // copy the flush_lsn_ since we are going to unlock before flushing
            const lsn_t lsn = flush_lsn_;

            // unlock before flushing the logs
            lock.unlock();

            // flush the logs
            // TODO: we might need to check the flush state
            // TODO: this is still not safe since we might have the log_buffer full and do the swap again
            disk_manager_->WriteLog(flush_buffer_, flush_buffer_offset_);

            // set the persistent lsn
            // TODO: we might need to check the relationship between persistentLSN and flushLSN
            assert(persistent_lsn_ < lsn);
            persistent_lsn_ = lsn;

            // if the main thread is waiting as shown by the promise, set the promise to let the main thread
            // know the flushing is complete
            if (promise_ != nullptr) {
                promise_->set_value();
            }
        }
    });

}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {

    // return if the flushing thread is not started
    if (!ENABLE_LOGGING) return;

    // notify the flushing the thread to stop
    // TODO: do we need to protect ENABLE_LOGGING with a mutex
    ENABLE_LOGGING = false;
    cv_.notify_one();

    // join the flush thread
    flush_thread_->join();

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
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {

    // since we are going to deal with the buffer, need to lock first
    std::lock_guard<std::mutex> lock_guard(latch_);

    // check if the buffer can fit the log record
    // TODO: not sure if we need the >= or > here
    // TODO: this is still not safe since we might be flushing the flush_buffer
    if (log_record.GetSize() + log_buffer_offset_ >= LOG_BUFFER_SIZE) {
        SwapBuffer();
        cv_.notify_one();
    }
    // TODO: why the assertion failed?
    //assert(log_buffer_offset_ == 0);

    // get the lsn for log_record
    log_record.lsn_ = next_lsn_++;

    // copy the common header
    memcpy(log_buffer_ + log_buffer_offset_, &log_record, 20);
    log_buffer_offset_ += 20;

    // copy the unique fields for each type of log_record
    switch(log_record.GetLogRecordType()) {
        case LogRecordType::INSERT: {
            memcpy(log_buffer_ + log_buffer_offset_, &log_record.insert_rid_, sizeof(RID));
            log_buffer_offset_ += sizeof(RID);
            log_record.insert_tuple_.SerializeTo(log_buffer_ + log_buffer_offset_);
            log_buffer_offset_ += sizeof(int32_t) + log_record.insert_tuple_.GetLength();
            break;
        }
        case LogRecordType::MARKDELETE:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE: {
            memcpy(log_buffer_ + log_buffer_offset_, &log_record.delete_rid_, sizeof(RID));
            log_buffer_offset_ += sizeof(RID);
            log_record.delete_tuple_.SerializeTo(log_buffer_ + log_buffer_offset_);
            log_buffer_offset_ += sizeof(int32_t) + log_record.delete_tuple_.GetLength();
            break;
        }
        case LogRecordType::UPDATE: {
            memcpy(log_buffer_ + log_buffer_offset_, &log_record.update_rid_, sizeof(RID));
            log_buffer_offset_ += sizeof(RID);
            log_record.old_tuple_.SerializeTo(log_buffer_ + log_buffer_offset_);
            log_buffer_offset_ += sizeof(int32_t) + log_record.old_tuple_.GetLength();
            log_record.new_tuple_.SerializeTo(log_buffer_ + log_buffer_offset_);
            log_buffer_offset_ += sizeof(int32_t) + log_record.new_tuple_.GetLength();
            break;
        }
        case LogRecordType::BEGIN:
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT: {
            break;
        }
        case LogRecordType::NEWPAGE: {
            memcpy(log_buffer_ + log_buffer_offset_, &log_record.prev_page_id_, sizeof(page_id_t));
            log_buffer_offset_ += sizeof(page_id_t);
            break;
        }
        case LogRecordType::INVALID: {
            throw Exception("Trying to append lof with LogRecordType::INVALID.");
        }

    }

    return log_record.GetLSN();
}

void LogManager::SwapBuffer() {

    // swap the buffer content
    char* temp = log_buffer_;
    log_buffer_ = flush_buffer_;
    flush_buffer_ = temp;

    // swap the buffer offset
    flush_buffer_offset_ = log_buffer_offset_;
    log_buffer_offset_ = 0;
}

void LogManager::ForceFlush(std::promise<void> *promise) {
    {
        // get the lock before setting the promise
        std::lock_guard<std::mutex> lock_guard(latch_);

        // the promise can also be nullptr
        promise_ = promise;

        // swap the two buffer
        SwapBuffer();
    }

    // release the lock before notifying the thread
    // wakeup the flush thread
    cv_.notify_one();

    // wait for the promise to finish flushing if the promise is not a nullptr
    if (promise != nullptr) {
        std::future<void> future = promise->get_future();
        future.wait();
    }

    // after the flushing is done, set the promise to nullptr
    // TODO: do I need to protect log_manager from accessing from multiple threads?
    promise_ = nullptr;




}

} // namespace cmudb
