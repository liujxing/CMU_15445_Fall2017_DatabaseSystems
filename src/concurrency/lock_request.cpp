//
// Created by Jiaxing Liu on 2/28/19.
//

#include <unordered_set>
#include "concurrency/lock_request.h"


namespace cmudb {

/********************************************************************************************************************/

LockRequest::LockRequest(const RID &rid, const LockType &lock_type, Transaction *txn,
                         const int timestamp, const bool granted):
        rid_(rid), lock_type_(lock_type), txn_(txn), timestamp_(timestamp), granted_(granted) {};

bool LockRequest::operator==(const LockRequest& other) const {
    return rid_ == other.rid_ &&
           lock_type_ == other.lock_type_ &&
           txn_ == other.txn_;
}

bool LockRequest::operator!=(const LockRequest& other) const {
    return this->operator==(other);
}

bool LockRequest::IsCompatible(const LockRequest& other) const {
    // TODO: might need to check if these two requests are the same requests
    if (!(rid_ == other.rid_)) return true; // request on different tuple
    if (txn_ == other.txn_) return true; // request from the same transaction
    if (lock_type_ == LockType::S && other.lock_type_ == LockType::S) return true; // two transactions requesting S on the same tuple
    return false;
}


std::ostream& operator<<(std::ostream& os, const LockRequest& request) {
    os << "LockRequest: "
       << "RID:" << request.rid_
       << ", Type:" << (request.lock_type_ == LockType::S ? "S" : "X")
       << ", TxnID:" << request.txn_->GetTransactionId()
       << ", Timestamp:" << request.timestamp_
       << ", Granted:" << request.granted_;

    return os;

}


/********************************************************************************************************************/


bool LockQueue::IsCompatibleForRequest(const std::shared_ptr<LockRequest> &request) const {

    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (!iterator->get()->granted_) return true;
        if (!iterator->get()->IsCompatible(*request)) return false;
    }
    return true;
}

bool LockQueue::IsCompatibleForUpgrade(const std::shared_ptr<LockRequest>& request) const {

    assert(request->lock_type_ == LockType::S);

    // create a copy of LockRequest with its type set to X
    std::shared_ptr<LockRequest> upgraded_request(
            new LockRequest(request->rid_, LockType::X, request->txn_, request->timestamp_, false));

    // check compatibility with existing locks
    // Note: by definition, the upgraded request is compatible with original request
    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (!iterator->get()->granted_) return true;
        if (!iterator->get()->IsCompatible(*upgraded_request)) return false;
    }
    return true;
}



std::shared_ptr<LockRequest> LockQueue::FetchLockRequest(const Transaction* txn) const {

    std::shared_ptr<LockRequest> request = nullptr;
    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (iterator->get()->txn_ == txn) {
            request = *iterator;
            break;
        }
    }
    return request;
}

bool LockQueue::IsValid() const {

    auto iterator = queue_.begin();
    bool all_granted = true;
    std::unordered_set<Transaction*> transactions;

    while (iterator!= queue_.end()) {
        // check if all granted locks are before the waiting locks
        if (iterator->get()->granted_ && !all_granted) return false;
        if (!iterator->get()->granted_ && all_granted) all_granted = false;

        // check if the transactions are unique
        if (transactions.find(iterator->get()->txn_) != transactions.end()) return false;
        transactions.insert(iterator->get()->txn_);

        // check decreasing order of timestamp
        if (iterator + 1 != queue_.end()) {
            auto iterator_next = iterator + 1;
            if (iterator_next->get()->timestamp_ <= iterator_next->get()->timestamp_) return false;
        }

        iterator++;
    }

    return true;
}

void LockQueue::InsertGrantedLockToQueue(const std::shared_ptr<LockRequest>& request) {
    assert(request->granted_);
    // empty queue
    if (queue_.empty()) {
        queue_.push_back(request);
        return;
    }
    // insert to the front
    if (request->timestamp_ > queue_.front()->timestamp_) {
        queue_.push_front(request);
        return;
    }
    // insert to the back
    if (request->timestamp_ < queue_.back()->timestamp_) {
        queue_.push_back(request);
        return;
    }

    auto iterator = queue_.begin();
    while (iterator->get()->timestamp_ > request->timestamp_) iterator++;
    queue_.insert(iterator, request);

    assert(IsValid());
}

bool LockQueue::CheckDeadlockPolicyForRequest(const std::shared_ptr<LockRequest>& request) const {
    assert(!IsCompatibleForRequest(request));
    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (iterator->get()->granted_ && iterator->get()->timestamp_ < request->timestamp_) return false;
    }
    return true;

}


bool LockQueue::CheckDeadlockPolicyForUpgrade(const std::shared_ptr<LockRequest>& request) const {

    assert(request->lock_type_ == LockType::S);
    // check if all granted locks have timestamp larger than current request
    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (iterator->get()->granted_ && iterator->get()->timestamp_ < request->timestamp_) return false;
    }
    return true;
}


void LockQueue::WaitForLockGrant(std::shared_ptr<LockRequest>& request,
                                 std::unique_lock<std::mutex>& lock_manager) {

    condition_variable_.wait(lock_manager, [&](){return request->granted_;});
}

void LockQueue::GrantLock(std::shared_ptr<LockRequest> &request) {
    request->granted_ = true;
    Transaction* txn = request->txn_;
    if (request->lock_type_ == LockType::S)
        txn->GetSharedLockSet()->insert(request->rid_);
    else if (request->lock_type_ == LockType::X)
        txn->GetExclusiveLockSet()->insert(request->rid_);
}

void LockQueue::GrantPossibleLocks() {

    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (!iterator->get()->granted_) {
            if (IsCompatibleForRequest(*iterator)) {
                GrantLock(*iterator);
            } else return;
        }
    }
}

void LockQueue::Remove(std::shared_ptr<LockRequest> &request) {
    for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
        if (iterator->get()->txn_ == request->txn_) {
            queue_.erase(iterator);
            return;
        }
    }

}




}