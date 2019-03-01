/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

LockManager::LockManager(bool strict_2PL):strict_2PL_(strict_2PL) {
    txn_timestamp_table_ = new ExtendibleHash<txn_id_t, int>(BUCKET_SIZE);
    rid_lock_table_ = new ExtendibleHash<RID, std::shared_ptr<LockQueue>> (BUCKET_SIZE);
}

LockManager::~LockManager() {
    delete txn_timestamp_table_;
    delete rid_lock_table_;
}



bool LockManager::LockShared(Transaction *txn, const RID &rid) {

    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock(mutex_);

    // get/assign timestamp to transaction
    const int txn_timestamp = AssignTransactionTimestamp(txn);

    // create lock request as shared_ptr.
    // Here we use shared_ptr instead of stack variable because we want other functions to be able to modify this
    // lock request instead of modifying its copy
    std::shared_ptr<LockRequest> lock_request(
            new LockRequest(rid, LockType::S, txn, txn_timestamp)
    );

    // check lock eligibility under (strict) 2PL
    if (!CheckLockRequestLegalityUnder2PL(lock_request)) return false;

    // check lock compatibility with existing locks. If compatible, grant lock.
    if (CheckLockRequestCompatibility(lock_request)) {
        GrantLockRequest(lock_request);
        AddLockRequestToQueue(lock_request);
        return true;
    }

    // If incompatible, wait or kill using the deadlock prevention rule
    // use deadlock prevention policy to check whether to add current lock queue
    if (!CheckDeadlockPolicyForRequest(lock_request)) return false;

    // add lock request to queue and wait
    AddLockRequestToQueue(lock_request);
    return WaitForLockRequest(lock_request, manager_lock);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {


    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock(mutex_);

    // get/assign timestamp to transaction
    const int txn_timestamp = AssignTransactionTimestamp(txn);

    // create lock request as shared_ptr.
    std::shared_ptr<LockRequest> lock_request(
            new LockRequest(rid, LockType::X, txn, txn_timestamp)
    );

    // check lock eligibility under (strict) 2PL
    if (!CheckLockRequestLegalityUnder2PL(lock_request)) return false;

    // check lock compatibility with existing locks. If compatible, grant lock.
    if (CheckLockRequestCompatibility(lock_request)) {
        GrantLockRequest(lock_request);
        AddLockRequestToQueue(lock_request);
        return true;
    }

    // If incompatible, wait or kill using the deadlock prevention rule
    // use deadlock prevention policy to check whether to add current lock queue
    if (!CheckDeadlockPolicyForRequest(lock_request)) return false;

    // add lock request to queue and wait
    AddLockRequestToQueue(lock_request);
    return WaitForLockRequest(lock_request, manager_lock);
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {

    // obtain mutex of LockManager
    std::unique_lock<std::mutex> manager_lock(mutex_);

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
    std::unique_lock<std::mutex> manager_lock(mutex_);

    // fetch the corresponding queue and request from table
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(rid);
    std::shared_ptr<LockRequest> lock_request = lock_queue->FetchLockRequest(txn);

    // check unlock eligibility under (strict) 2PL
    if (!CheckUnlockRequestLegalityUnder2PL(lock_request)) return false;

    // unlock the lock, and grant access to corresponding requests, and notify these threads
    return UnlockRequest(lock_request);
}

/***************************** transaction timestamp related variables ***********************************/

int LockManager::AssignTransactionTimestamp(const txn_id_t txn_id) {
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


/******************************** lock request related variables ***********************************/

std::shared_ptr<LockQueue> LockManager::FetchLockQueue(const RID& rid) {
    std::shared_ptr<LockQueue> queue = nullptr;
    rid_lock_table_->Find(rid, queue);
    return queue;
}


/************************ lock request compatibility with existing locks related variables *************/

bool LockManager::CheckLockRequestCompatibility(const std::shared_ptr<LockRequest> & request) {

    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);

    // there is no existing lock_queue associated with current RID
    if (lock_queue == nullptr) return true;

    // check if current request is compatible with all existing lock
    return lock_queue->IsCompatibleForRequest(request);
}


bool LockManager::CheckLockUpgradeCompatibility(const std::shared_ptr<LockRequest>& request) {

    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);

    return lock_queue->IsCompatibleForUpgrade(request);

}


void LockManager::GrantLockRequest(std::shared_ptr<LockRequest> &request) {
    // set request state
    request->granted_ = true;

    // add request to transaction lock set
    if (request->lock_type_ == LockType::S)
        request->txn_->GetSharedLockSet()->insert(request->rid_);
    else if (request->lock_type_ == LockType::X)
        request->txn_->GetExclusiveLockSet()->insert(request->rid_);
}


void LockManager::GrantLockUpgrade(std::shared_ptr<LockRequest> & request) {
    assert(request->granted_);
    assert(request->lock_type_==LockType::S);
    request->lock_type_ = LockType::X;
}

void LockManager::AddLockRequestToQueue(const std::shared_ptr<LockRequest> &request) {

    // fetch/insert a LockQueue to rid_lock_table_
    const RID& rid = request->rid_;
    std::shared_ptr<LockQueue> lock_queue;

    if (!rid_lock_table_->Find(rid, lock_queue)) {
        lock_queue = std::make_shared<LockQueue>();
        rid_lock_table_->Insert(rid, lock_queue);
    }

    // insert the LockRequest to LockQueue
    lock_queue->InsertGrantedLockToQueue(request);
}

/**************************** deadlock prevention related variables *********************/


bool LockManager::CheckDeadlockPolicyForRequest(std::shared_ptr<LockRequest> &request) {
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    assert(lock_queue != nullptr);
    return lock_queue->CheckDeadlockPolicyForRequest(request);
}

bool LockManager::CheckDeadlockPolicyForUpgrade(std::shared_ptr<LockRequest>& request) {
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    assert(lock_queue != nullptr);
    return lock_queue->CheckDeadlockPolicyForUpgrade(request);
}

/******************************** thread communication related variables ****************************/

bool LockManager::WaitForLockRequest(std::shared_ptr<LockRequest> &request,
                                 std::unique_lock<std::mutex>& lock_manager) {

    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    assert(lock_queue != nullptr);

    // put to sleep until the lock is granted
    lock_queue->WaitForLockGrant(request, lock_manager);

    // now the lock is granted, need to grant the lock and check the queue
    GrantLockRequest(request);
    lock_queue->GrantPossibleLocks();

    // now notify other threads about the change of lock granting status
    lock_queue->condition_variable_.notify_all();
    return true;
}




bool LockManager::WaitForLockUpgrade(std::shared_ptr<LockRequest>& request,
                                     std::unique_lock<std::mutex>& manager_lock) {

    // set the state of lock upgrade
    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);
    std::shared_ptr<LockRequest> lock_request = lock_queue->FetchLockRequest(lock_request->txn_);
    assert(lock_request->granted_);
    assert(lock_request->lock_type_ == LockType::S);

    // remove original request from queue
    //lock_queue->Remove(lock_request);

    // upgrade the lock request to X
    // TODO: there is no need to remove and add again, since the order in deque should be the same as before
    lock_request->granted_ = false;
    lock_request->lock_type_ = LockType::X;

    // put the lock request back to queue
    //AddLockRequestToQueue(lock_request);

    // wait for the lock request to get granted as lock request
    return WaitForLockRequest(lock_request, manager_lock);
}



bool LockManager::UnlockRequest(std::shared_ptr<LockRequest>& request) {

    std::shared_ptr<LockQueue> lock_queue = FetchLockQueue(request->rid_);

    // erase the request from deque
    lock_queue->Remove(request);

    // grant the possible locks after this unlocking
    lock_queue->GrantPossibleLocks();

    // now notify other threads about the change of lock granting status
    lock_queue->condition_variable_.notify_all();
    return true;
}



/************************************ 2PL related methods **************************************/

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


} // namespace cmudb
