/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {

    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock;

    // get/assign timestamp to transaction
    const int txn_timestamp = AssignTransactionTimestamp(txn);

    // check lock request legality with 2PL or strict 2PL. If illegal, return false.
    LockRequest lock_request(txn->GetTransactionId(), false, LockType::S, txn->GetTransactionId(), txn_timestamp);

    // check lock eligibility under (strict) 2PL
    if (!CheckLockRequestLegalityUnder2PL(txn)) return false;

    // check lock compatibility with existing locks.
    // If compatible, grant lock.
    if (CheckLockRequestCompatibility(lock_request)) {
        // todo: change the grantlock to use lock_request* as argument
        GrantLock(lock_request, txn);
        return true;
    }

    // If incompatible, wait or kill using the deadlock prevention rule
    // use deadlock prevention policy to check whether to add current lock queue
    if (!CheckDeadlockPolicy(lock_request)) return false;

    // add deadlock to queue and wait
    AddLockRequestToQueue(lock_request);
    return WaitForLockRequest(lock_request, manager_lock);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {

    // get/assign timestamp to transaction

    // check lock request legality with 2PL or strict 2PL. If illegal, return false.

    // check lock compatibility with existing locks.
    // If compatible, grant lock.
    // If incompatible, wait or kill using the deadlock prevention rule

    return false;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {

    // get/assign timestamp to transaction

    // check lock request legality with 2PL or strict 2PL. If illegal, return false.

    // check lock compatibility with existing locks.
    // If compatible, grant lock.
    // If incompatible, wait or kill using the deadlock prevention rule

    return false;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {

    // get/assign timestamp to transaction

    // check unlock request legality with 2PL or strict 2PL. If illegal, return false.

    // If legal, unlock and notify other threads. If illegal, return false.

    return false;
}



int LockManager::AssignTransactionTimestamp(const cmudb::txn_id_t txn_id) {
    int timestamp;
    if (txn_timestamp_table_.Find(txn_id, timestamp)) return timestamp;
    else {
        timestamp = current_timestamp_;
        current_timestamp_++;
        txn_timestamp_table_.Insert(txn_id, timestamp);
        return timestamp;
    }
}

int LockManager::AssignTransactionTimestamp(const Transaction* transaction) {
      const txn_id_t txn_id = transaction->GetTransactionId();
      return AssignTransactionTimestamp(txn_id);
}

bool LockManager::CheckLockRequestCompatibility(const LockRequest &request) {

    const RID& rid = request.rid_;
    std::shared_ptr<LockQueue> lock_queue;

    if (!rid_lock_table_.Find(rid, lock_queue)) {
        return true;
    }

    for (auto iterator = lock_queue->queue_.rbegin(); iterator != lock_queue->queue_.rend(); iterator++) {
        if (!request.IsCompatible(*iterator)) return false;
    }
    return true;
}

void LockManager::AddLockRequestToQueue(const LockRequest &request) {
    const RID& rid = request.rid_;
    std::shared_ptr<LockQueue> lock_queue;

    if (!rid_lock_table_.Find(rid, lock_queue)) {
        rid_lock_table_.Insert(rid, lock_queue);
    }
    lock_queue->queue_.emplace_back(request);
}

void LockManager::GrantLock(LockRequest &request, Transaction* txn) {
    // set request state
    request.granted_ = true;

    // add request to transaction lock set
    if (request.lock_type_ == LockType::S)
        txn->GetSharedLockSet()->emplace(request.rid_);
    else if (request.lock_type_ == LockType::X)
        txn->GetExclusiveLockSet()->emplace(request.rid_);

    AddLockRequestToQueue(request);
}

bool LockManager::WaitForLockRequest(LockRequest &request, std::unique_lock<std::mutex>& lock_manager) {

    // fetch the lock request from deque

    // put to sleep until the lock

    return false;
}

bool LockManager::CheckDeadlockPolicy(LockRequest& request) {
    const RID& rid = request.rid_;
    std::shared_ptr<LockQueue> lock_queue;
    if (!rid_lock_table_.Find(rid, lock_queue)) return true;
    for (auto iter = lock_queue->queue_.rbegin(); iter != lock_queue->queue_.rend(); iter++) {
        if (request.timestamp_ > iter->timestamp_) return false;
    }
    return true;
}

bool LockManager::CheckLockRequestLegalityUnder2PL(Transaction* txn) const {
    TransactionState state = txn->GetState();
    return state == TransactionState ::GROWING;
}

bool LockManager::CheckUpgradeRequestLegalityUnder2PL(Transaction* txn) const {
    TransactionState state = txn->GetState();
    return state == TransactionState ::GROWING;
}

bool LockManager::CheckUnlockReqestLegalityUnder2PL(const LockRequest& lock_request, Transaction* txn) const {
    TransactionState state = txn->GetState();
    switch (state) {
        case TransactionState::GROWING:
            txn->SetState(TransactionState::SHRINKING);
            return true;
        case TransactionState::SHRINKING:
            if (!strict_2PL_) return true;
            return lock_request.lock_type_ == LockType::S;
        default:
            return true;
    }
}



} // namespace cmudb
