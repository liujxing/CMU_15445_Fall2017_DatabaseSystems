/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record) {

  int offset = 0;

  try {
    // size
    // here the const in cast is needed because data is const char*
    int32_t size = *reinterpret_cast<const int32_t*>(data);
    offset += sizeof(int32_t);

    // lsn
    lsn_t lsn = *reinterpret_cast<const lsn_t*>(data + offset);
    offset += sizeof(lsn_t);

    // transaction ID
    txn_id_t txn_id = *reinterpret_cast<const txn_id_t *>(data + offset);
    offset += sizeof(txn_id_t);

    // prevLSN
    lsn_t prev_lsn = *reinterpret_cast<const lsn_t*>(data + offset);
    offset += sizeof(lsn_t);

    // LogType
    auto log_record_type = *reinterpret_cast<const LogRecordType*>(data + offset);
    offset += sizeof(LogRecordType);

    // check the validity of data
    if (size < 0 || lsn == INVALID_LSN || txn_id == INVALID_TXN_ID || prev_lsn == INVALID_LSN || log_record_type == LogRecordType::INVALID)
      return false;


    log_record.size_ = size;
    log_record.lsn_ = lsn;
    log_record.txn_id_ = txn_id;
    log_record.prev_lsn_ = prev_lsn;
    log_record.log_record_type_ = log_record_type;


    switch (log_record_type) {
      case LogRecordType::INVALID:
        return false;
      case LogRecordType::BEGIN:
      case LogRecordType::COMMIT:
      case LogRecordType::ABORT: {
        return true;
      }
      case LogRecordType::NEWPAGE: {
        log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(data + offset);
        offset += sizeof(page_id_t);
        return true;
      }
      case LogRecordType::INSERT: {
        // TODO: use memcpy or reinterpret_cast for getting rid?
        memcpy(&log_record.insert_rid_, data + offset, sizeof(RID));
        offset += sizeof(RID);
        log_record.insert_tuple_.DeserializeFrom(data + offset);
        offset += sizeof(int32_t) + log_record.insert_tuple_.GetLength();
        return true;
      }
      case LogRecordType::MARKDELETE:
      case LogRecordType::APPLYDELETE:
      case LogRecordType::ROLLBACKDELETE: {
        // TODO: use memcpy or reinterpret_cast for getting rid?
        memcpy(&log_record.delete_rid_, data + offset, sizeof(RID));
        offset += sizeof(RID);
        log_record.delete_tuple_.DeserializeFrom(data + offset);
        offset += sizeof(int32_t) + log_record.delete_tuple_.GetLength();
        return true;
      }
      case LogRecordType::UPDATE: {
        memcpy(&log_record.update_rid_, data + offset, sizeof(RID));
        offset += sizeof(RID);
        log_record.old_tuple_.DeserializeFrom(data + offset);
        offset += sizeof(int32_t) + log_record.old_tuple_.GetLength();
        log_record.new_tuple_.DeserializeFrom(data + offset);
        offset += sizeof(int32_t) + log_record.new_tuple_.GetLength();
        return true;
      }
    }
  } catch(...) {
    // this line is used to catch any potential failure from loading the data on disk
    return false;

  }
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {

    LogRecord log_record;

    // keep looping to read the log, each time read LOG_BUFFER_SIZE into log_buffer_
    int disk_log_offset = 0;
    while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, disk_log_offset)) {
        disk_log_offset += LOG_BUFFER_SIZE;

        // keep looping to convert the log buffer into log record
        int log_buffer_offset = 0;
        while (DeserializeLogRecord(log_buffer_ + log_buffer_offset, log_record)) {
            log_buffer_offset += log_record.GetSize();

            // now we have a valid log record

            // update lsn_mapping table which maps the lsn to offset in disk
            // the equation is ugly because we updated disk_log_offset and log_buffer_offset early
            lsn_mapping_[log_record.GetLSN()] = disk_log_offset - LOG_BUFFER_SIZE + log_buffer_offset - log_record.GetSize();

            // update the active_txn_ table
            if (log_record.log_record_type_ == LogRecordType::ABORT || log_record.log_record_type_ == LogRecordType::COMMIT)
                active_txn_.erase(log_record.GetTxnId());
            else
                active_txn_[log_record.GetTxnId()] = log_record.GetLSN();

            switch (log_record.log_record_type_) {

                // the NEWPAGE log record
                // need to get or create the corresponding TablePage in TableHeap
                case LogRecordType::NEWPAGE: {
                    // TODO: do we need to look at the lsn of log record?
                    const page_id_t prev_page_id = log_record.prev_page_id_;
                    // current page is the first page of TableHeap
                    if (prev_page_id == INVALID_PAGE_ID) {
                        // create page
                        page_id_t page_id;
                        TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(page_id));
                        page->WLatch();
                        page->Init(page_id, PAGE_SIZE, INVALID_PAGE_ID, nullptr, nullptr);
                        page->WUnlatch();
                        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    } else {
                        // fetch the previous page
                        TablePage* prev_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(prev_page_id));
                        // check if the prev_page has already pointed to current page
                        if (prev_page->GetNextPageId() == INVALID_PAGE_ID) {
                            // create page
                            page_id_t page_id;
                            TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(page_id));
                            page->WLatch();
                            page->Init(page_id, PAGE_SIZE, prev_page_id, nullptr, nullptr);
                            page->WUnlatch();
                            prev_page->SetNextPageId(page_id);
                            buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                        }
                        buffer_pool_manager_->UnpinPage(prev_page_id, true);
                    }
                    break;
                }
                // the BEGIN log record does not do anything to the database
                case LogRecordType::BEGIN: {
                    break;
                }
                // the COMMIT log record
                case LogRecordType::COMMIT: {
                    break;
                }
                // the ABORT log record
                case LogRecordType::ABORT: {
                    break;
                }
                // the INSERT log record
                case LogRecordType::INSERT: {
                    TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(log_record.insert_rid_.GetPageId()));
                    if (page->GetLSN() < log_record.GetLSN()) {
                        page->WLatch();
                        page->InsertTuple(log_record.insert_tuple_, log_record.insert_rid_, nullptr, nullptr, nullptr);
                        page->WUnlatch();
                    }
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    break;
                }
                // the DELETE log record
                case LogRecordType::ROLLBACKDELETE:
                case LogRecordType::APPLYDELETE:
                case LogRecordType::MARKDELETE: {
                    TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId()));
                    if (page->GetLSN() < log_record.GetLSN()) {
                        page->WLatch();
                        if (log_record.log_record_type_ == LogRecordType::MARKDELETE)
                            page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
                        else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE)
                            page->ApplyDelete(log_record.delete_rid_, nullptr, nullptr);
                        else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE)
                            page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
                        page->WUnlatch();
                    }
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    break;
                }
                // the UPDATE log record
                case LogRecordType::UPDATE: {
                    TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(log_record.update_rid_.GetPageId()));
                    if (page->GetLSN() < log_record.GetLSN()) {
                        page->WLatch();
                        page->UpdateTuple(log_record.new_tuple_, log_record.old_tuple_, log_record.update_rid_, nullptr,
                                          nullptr, nullptr);
                    }
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    break;
                }
                case LogRecordType::INVALID: {
                    break;
                }
            }
        }
    }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {

    // loop through all transactions
    for (auto iterator = active_txn_.begin(); iterator != active_txn_.end(); iterator++) {
        lsn_t lsn = iterator->second;
        // iterate through all records of current transaction
        while (lsn != INVALID_LSN) {
            // get the log record of current lsn
            disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, lsn_mapping_[lsn]);
            LogRecord log_record;
            DeserializeLogRecord(log_buffer_, log_record);

            // now we have a valid log record
            switch (log_record.log_record_type_) {
                case LogRecordType::NEWPAGE: {
                    break;
                }
                case LogRecordType::BEGIN: {
                    break;
                }
                case LogRecordType::COMMIT: {
                    break;
                }
                case LogRecordType::ABORT: {
                    break;
                }
                case LogRecordType::INVALID: {
                    break;
                }
                // undo INSERT: delete the record from page using APPLYDELETE
                case LogRecordType::INSERT: {
                    TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(log_record.GetInsertRID().GetPageId()));
                    page->WLatch();
                    page->ApplyDelete(log_record.GetInsertRID(), nullptr, nullptr);
                    page->WUnlatch();
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    break;
                }
                case LogRecordType::APPLYDELETE:
                case LogRecordType::MARKDELETE:
                case LogRecordType::ROLLBACKDELETE: {
                    TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(log_record.GetDeleteRID().GetPageId()));
                    page->WLatch();
                    // reverse of APPLYDELETE: INSERT
                    if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
                        page->InsertTuple(log_record.delete_tuple_, log_record.GetDeleteRID(), nullptr, nullptr, nullptr);
                    // reverse of MARKDELETE: ROLLBACKDELETE
                    } else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
                        page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
                    } else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                        page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
                    }
                    page->WUnlatch();
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    break;
                }
                case LogRecordType::UPDATE: {
                    TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(log_record.update_rid_.GetPageId()));
                    page->WLatch();
                    page->UpdateTuple(log_record.old_tuple_, log_record.new_tuple_, log_record.update_rid_, nullptr,
                                      nullptr, nullptr);
                    page->WUnlatch();
                    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
                    break;
                }

            }

            // go to the previous log record of current transaction
            lsn = log_record.GetPrevLSN();
        }

        // clear the two unordered_map
        lsn_mapping_.clear();
        active_txn_.clear();
    }


}

} // namespace cmudb
