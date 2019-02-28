/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

LockManager::LockManager(bool strict_2PL):strict_2PL_(strict_2PL) {
    txn_timestamp_table_ = new ExtendibleHash<txn_id_t, int>(BUCKET_SIZE);
    rid_lock_table_ = new ExtendibleHash<RID, std::shared_ptr<LockQueue>> (BUCKET_SIZE);
}

// TODO: either add destructor or change the pointer to hashtable to stack variable


bool LockManager::LockShared(Transaction *txn, const RID &rid) {

    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock;

    // get/assign timestamp to transaction
    const int txn_timestamp = AssignTransactionTimestamp(txn);

    // create lock request as shared_ptr.
    // Here we use shared_ptr instead of stack variable because we want other functions to be able to modify this
    // lock request instead of modifying its copy
    std::shared_ptr<LockRequest> lock_request(
            new LockRequest(txn->GetTransactionId(), LockType::S, txn, txn_timestamp)
    );

    // check lock eligibility under (strict) 2PL
    if (!CheckLockRequestLegalityUnder2PL(lock_request)) return false;

    // check lock compatibility with existing locks.
    // If compatible, grant lock.
    if (CheckLockRequestCompatibility(lock_request)) {
        GrantLock(lock_request);
        AddLockRequestToQueue(lock_request);
        return true;
    }

    // If incompatible, wait or kill using the deadlock prevention rule
    // use deadlock prevention policy to check whether to add current lock queue
    if (!CheckDeadlockPolicy(lock_request)) return false;

    // add lock request to queue and wait
    AddLockRequestToQueue(lock_request);
    return WaitForLockRequest(lock_request, manager_lock);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {


    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock;

    // get/assign timestamp to transaction
    const int txn_timestamp = AssignTransactionTimestamp(txn);

    // create lock request as shared_ptr.
    // Here we use shared_ptr instead of stack variable because we want other functions to be able to modify this
    // lock request instead of modifying its copy
    std::shared_ptr<LockRequest> lock_request(
            new LockRequest(txn->GetTransactionId(), LockType::X, txn, txn_timestamp)
    );

    // check lock eligibility under (strict) 2PL
    if (!CheckLockRequestLegalityUnder2PL(lock_request)) return false;

    // check lock compatibility with existing locks.
    // If compatible, grant lock.
    if (CheckLockRequestCompatibility(lock_request)) {
        GrantLock(lock_request);
        AddLockRequestToQueue(lock_request);
        return true;
    }

    // If incompatible, wait or kill using the deadlock prevention rule
    // use deadlock prevention policy to check whether to add current lock queue
    if (!CheckDeadlockPolicy(lock_request)) return false;

    // add lock request to queue and wait
    AddLockRequestToQueue(lock_request);
    return WaitForLockRequest(lock_request, manager_lock);
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {



    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock;

    // fetch the corresponding queue and request from table
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(rid);
    std::shared_ptr<LockRequest> lock_request = lock_queue->FetchLockRequest(txn);

    // check upgrade eligibility under (strict) 2PL
    if (!CheckUpgradeRequestLegalityUnder2PL(lock_request)) return false;

    // check upgrade compatibility with existing locks. If compatible, grant lock.
    if (CheckLockUpgradeCompatibility(lock_request)) {
        GrantLockUpgrade(lock_request);
        return true;
    }

    // If incompatible, wait or kill using the deadlock prevention rule
    // use deadlock prevention policy to check whether to add current lock queue
    if (!CheckDeadlockPolicyForUpgrade(lock_request)) return false;

    // set lock upgrade to wait
    return WaitForLockUpgrade(lock_request, manager_lock);
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {

    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock;

    // fetch the corresponding queue and request from table
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(rid);
    std::shared_ptr<LockRequest> lock_request = lock_queue->FetchLockRequest(txn);

    // check unlock eligibility under (strict) 2PL
    if (!CheckUnlockRequestLegalityUnder2PL(lock_request)) return false;

    // unlock the lock, and grant access to correponding requests, and notify these threads
    return UnlockRequest(lock_request);
}



int LockManager::AssignTransactionTimestamp(const cmudb::txn_id_t txn_id) {
    int timestamp;
    if (txn_timestamp_table_->Find(txn_id, timestamp)) return timestamp;
    else {
        timestamp = current_timestamp_;
        current_timestamp_++;
        txn_timestamp_table_->Insert(txn_id, timestamp);
        return timestamp;
    }
}

int LockManager::AssignTransactionTimestamp(const Transaction* transaction) {
      const txn_id_t txn_id = transaction->GetTransactionId();
      return AssignTransactionTimestamp(txn_id);
}

bool LockManager::CheckLockRequestCompatibility(const std::shared_ptr<LockRequest> & request) {

    const RID& rid = request->rid_;
    std::shared_ptr<LockQueue> lock_queue = nullptr;

    // current RID is not locked by any transaction
    if (!rid_lock_table_->Find(rid, lock_queue)) {
        return true;
    }

    // check if current request is compatible with all existing lock
    return lock_queue->IsCompatible(request);
}

bool LockManager::CheckLockUpgradeCompatibility(const std::shared_ptr<LockRequest>& request) {

    const RID& rid = request->rid_;
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(rid);

    // create a copy of LockRequest with its type set to X
    std::shared_ptr<LockRequest> upgraded_request(
            new LockRequest(request->rid_, request->lock_type_, request->txn_, request->timestamp_, false));

    // check compatibility with existing locks
    // note here we also need to check the granted lock request after current request, since it is possible that
    // some S locks are granted after current S lock
    // Note: by definition, the upgraded request is compatible with original request
    for (auto iterator = lock_queue->queue_.begin(); iterator != lock_queue->queue_.end(); iterator++) {
        if (!iterator->get()->granted_) break;
        if (!iterator->get()->IsCompatible(*upgraded_request)) return false;
    }
    return true;


}

void LockManager::AddLockRequestToQueue(const std::shared_ptr<LockRequest> &request) {

    const RID& rid = request->rid_;
    std::shared_ptr<LockQueue> lock_queue = std::make_shared<LockQueue>();

    if (!rid_lock_table_->Find(rid, lock_queue)) {
        rid_lock_table_->Insert(rid, lock_queue);
    }
    lock_queue->queue_.emplace_back(request);
}

void LockManager::GrantLock(std::shared_ptr<LockRequest> &request) {
    // set request state
    request->granted_ = true;

    // add request to transaction lock set
    if (request->lock_type_ == LockType::S)
        request->txn_->GetSharedLockSet()->emplace(request->rid_);
    else if (request->lock_type_ == LockType::X)
        request->txn_->GetExclusiveLockSet()->emplace(request->rid_);

}

bool LockManager::WaitForLockRequest(std::shared_ptr<LockRequest> &request,
                                     std::unique_lock<std::mutex>& lock_manager) {

    // fetch the lock request from deque
    std::shared_ptr<LockQueue> lock_queue = nullptr;
    rid_lock_table_->Find(request->rid_, lock_queue);
    assert(lock_queue != nullptr);

    // put to sleep until the lock is granted
    lock_queue->condition_variable_.wait(lock_manager, [&](){return request->granted_;});

    // now the lock is granted, need to grant the lock and check the queue
    GrantLock(request);
    UpdateFollowingLockRequests(lock_queue, request);

    // now notify other threads about the change of lock granting status
    lock_queue->condition_variable_.notify_all();
    return true;
}


bool LockManager::CheckDeadlockPolicy(std::shared_ptr<LockRequest>& request) {
    const RID& rid = request->rid_;
    std::shared_ptr<LockQueue> lock_queue;
    if (!rid_lock_table_->Find(rid, lock_queue)) return true;
    for (auto iter = lock_queue->queue_.rbegin(); iter != lock_queue->queue_.rend(); iter++) {
        if (request->timestamp_ > iter->get()->timestamp_) return false;
    }
    return true;
}

bool LockManager::CheckLockRequestLegalityUnder2PL(std::shared_ptr<LockRequest>& lock_request) const {
    // when requesting a lock, it is only legal under the GROWING phase of (strict)2PL
    return lock_request->txn_->GetState() == TransactionState::GROWING;
}

bool LockManager::CheckUpgradeRequestLegalityUnder2PL(std::shared_ptr<LockRequest>& lock_request) const {
    // when upgrading a lock, it is only legal under the GROWING phase of (strict)2PL
    return lock_request->txn_->GetState() == TransactionState::GROWING;
}

bool LockManager::CheckUnlockRequestLegalityUnder2PL(const std::shared_ptr<LockRequest>& lock_request) const {
    Transaction* txn = lock_request->txn_;
    TransactionState state = txn->GetState();
    switch (state) {
        case TransactionState::GROWING:
            txn->SetState(TransactionState::SHRINKING);
            return true;
        case TransactionState::SHRINKING:
            if (!strict_2PL_) return true;
            // under strict 2PL, unlocking for S is done at SHRINKING phase, and unlocking for X is done after COMMITTED phase
            return lock_request->lock_type_ == LockType::S;
        default:
            return true;
    }
}

void LockManager::UpdateFollowingLockRequests(std::shared_ptr<LockQueue> & lock_queue,
                                              std::shared_ptr<LockRequest> & request) {
    for (auto iterator = lock_queue->queue_.begin(); iterator != lock_queue->queue_.end(); iterator++) {
        // move iterator to the next position of current request
        if (*iterator == request) {
            iterator++;
            if (iterator == lock_queue->queue_.end()) return;
            // check if the following requests can be granted
            while (iterator != lock_queue->queue_.end()) {
                if (lock_queue->IsCompatible(*iterator)) {
                    GrantLock(*iterator);
                    iterator++;
                }
                else return;
            }
        }
    }
}


std::shared_ptr<LockQueue> LockManager::FetchLockQueue(const RID& rid) {
    std::shared_ptr<LockQueue> queue = nullptr;
    rid_lock_table_->Find(rid, queue);
    assert(queue != nullptr);
    return queue;
}

void LockManager::GrantLockUpgrade(std::shared_ptr<LockRequest> & request) {
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    std::shared_ptr<LockRequest> lock_request = lock_queue->FetchLockRequest(request->txn_);
    assert(lock_request->granted_);
    assert(*lock_request  == *request);
    lock_request->granted_ = true;

}

bool LockManager::CheckDeadlockPolicyForUpgrade(std::shared_ptr<LockRequest>& request) {
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    auto iterator = lock_queue->queue_.begin();
    while (iterator->get()->txn_ != request->txn_) iterator++;

    // TODO: assert check that iterator is indeed found
    iterator++;
    while (iterator != lock_queue->queue_.end()) {
        if (iterator->get()->granted_) return false;
        iterator++;
    }

    return true;
}


bool LockManager::WaitForLockUpgrade(std::shared_ptr<LockRequest>& request,
                                     std::unique_lock<std::mutex>& manager_lock) {

    // set the state of lock upgrade
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    std::shared_ptr<LockRequest> lock_request = lock_queue->FetchLockRequest(lock_request->txn_);
    assert(!lock_request->granted_);
    assert(lock_request->lock_type_ == LockType::S);

    lock_request->granted_ = false;
    lock_request->lock_type_ = LockType::X;

    // let the lock upgrade request to wait
    lock_queue->condition_variable_.wait(manager_lock, [&](){return lock_request->granted_;});

    // lock is now granted, also grant lock for following locks (although impossible, since this lock is now a X lock
    auto iterator = lock_queue->queue_.begin();
    while (*iterator != lock_request) iterator++;
    iterator++;
    while (iterator != lock_queue->queue_.end()) {
        if (lock_queue->IsCompatible(*iterator)) {
            iterator->get()->granted_ = true;
            iterator++;
        } else break;
    }

    // notify all other threads about the change of state
    lock_queue->condition_variable_.notify_all();
    return true;
}

bool LockManager::UnlockRequest(std::shared_ptr<LockRequest>& request) {

    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    // erase the request from deque
    auto iterator = lock_queue->queue_.begin();
    while (iterator != lock_queue->queue_.end()) {
        if (iterator->get()->txn_ == request->txn_) {
            iterator = lock_queue->queue_.erase(iterator);
            break;
        } else {
            iterator++;
        }
    }

    // grant the locks following the the iterator
    while (iterator != lock_queue->queue_.end()) {
        if (lock_queue->IsCompatible(*iterator)) {
            iterator->get()->granted_ = true;
            iterator++;
        } else break;
    }

    lock_queue->condition_variable_.notify_all();

    return true;

}

} // namespace cmudb
