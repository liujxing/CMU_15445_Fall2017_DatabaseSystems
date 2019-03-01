//
// Created by Jiaxing Liu on 2/28/19.
//


#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {



// type of locks
enum class LockType {
    X, S
};

/********************************************************************************************************************/

struct LockRequest {

    const RID rid_;
    LockType lock_type_;
    Transaction* txn_;
    const int timestamp_;
    bool granted_;

    // constructors and destructors
    LockRequest(const RID& rid, const LockType& lock_type,
                Transaction* txn, const int timestamp, const bool granted = false);


    LockRequest(const LockRequest&) = default;
    LockRequest(LockRequest&&) = default;
    LockRequest& operator=(const LockRequest&) = default;
    LockRequest& operator=(LockRequest&&) = default;

    ~LockRequest() = default;

    // comparison operators
    bool operator==(const LockRequest& other) const;
    bool operator!=(const LockRequest& other) const;

    // compatibility function with another lock
    bool IsCompatible(const LockRequest& other) const;

    // print
    friend std::ostream& operator<<(std::ostream& os, const LockRequest& request);

};


/********************************************************************************************************************/

struct LockQueue {

    std::condition_variable condition_variable_;
    std::deque<std::shared_ptr<LockRequest>> queue_;

    // constructors
    LockQueue() = default;
    ~LockQueue() = default;

    // Check if this lock request is compatible with all granted requests within the queue
    bool IsCompatibleForRequest(const std::shared_ptr<LockRequest>& request) const;

    // Check if this lock upgrade is compatible with all granted requests within the queue
    bool IsCompatibleForUpgrade(const std::shared_ptr<LockRequest>& request) const;

    // Fetch a lock request from transaction
    std::shared_ptr<LockRequest> FetchLockRequest(const Transaction* txn) const;

    // Check if LockQueue satisfies the invariance that the granted requests are before waiting requests,
    // and the timestamps are in decreasing order, and the transactions are unique
    bool IsValid() const;

    // Insert a granted lock to queue and maintain the decreasing order of timestamp
    void InsertGrantedLockToQueue(const std::shared_ptr<LockRequest>& request);

    // Check if the lock request should be added to queue according to wait-die policy
    bool CheckDeadlockPolicyForRequest(const std::shared_ptr<LockRequest>& request) const;

    // Check if the lock upgrade should be added to queue according to wait-die policy
    bool CheckDeadlockPolicyForUpgrade(const std::shared_ptr<LockRequest>& request) const;

    // Wait for a specific LockRequest to be granted
    void WaitForLockGrant(std::shared_ptr<LockRequest>& request,
                          std::unique_lock<std::mutex>& lock_manager);

    // Grant a lock request
    void GrantLock(std::shared_ptr<LockRequest>& request);

    // Grant possible locks when the lock queue status changes
    void GrantPossibleLocks();

    // Remove lock request from queue
    void Remove(std::shared_ptr<LockRequest>& request);

};




} // namespace cmudb