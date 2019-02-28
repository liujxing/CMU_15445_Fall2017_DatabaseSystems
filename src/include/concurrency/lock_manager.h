/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"
#include "hash/extendible_hash.h"

namespace cmudb {

enum class LockType {
    X, S
};


struct LockRequest {

    const RID rid_;
    bool granted_ = false;
    LockType lock_type_;
    Transaction* txn_;
    const int timestamp_;

    // constructors and destructors
    LockRequest(const RID& rid, const LockType& lock_type,
                Transaction* txn, const int timestamp, const bool granted = false):
            rid_(rid),
            granted_(granted),
            lock_type_(lock_type),
            txn_(txn),
            timestamp_(timestamp)
    {};

    LockRequest(const LockRequest&) = default;
    LockRequest(LockRequest&&) = default;
    LockRequest& operator=(const LockRequest&) = default;
    LockRequest& operator=(LockRequest&&) = default;

    ~LockRequest() = default;

    // comparison operators
    bool operator==(const LockRequest& other) const {
        return rid_ == other.rid_ &&
               lock_type_ == other.lock_type_ &&
               txn_ == other.txn_;
    }

    bool operator!=(const LockRequest& other) const {
        return !(*this == other);
    }

    // compatibility function with another lock
    bool IsCompatible(const LockRequest& other) const {
        assert(*this != other);

        if (!(rid_ == other.rid_)) return true; // request on different tuple
        if (txn_ == other.txn_) return true; // request from the same transaction
        if (lock_type_ == LockType::S && other.lock_type_ == LockType::S) return true; // two transactions requesting S on the same tuple
        return false;
    }



};

struct LockQueue {

    std::condition_variable condition_variable_;
    std::deque<std::shared_ptr<LockRequest>> queue_;

    // constructors
    LockQueue() = default;
    ~LockQueue() = default;


    // Check if this request is compatible with all request within the queue up to this request
    bool IsCompatible(const std::shared_ptr<LockRequest>& request) const {

        for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
            if (*iterator == request) return true;
            if (!request->IsCompatible(*(iterator->get()))) return false;
        }
        return true;
    }

    // Fetch a lock request from transaction
    std::shared_ptr<LockRequest> FetchLockRequest(const Transaction* txn) {
        std::shared_ptr<LockRequest> request = nullptr;
        for (auto iterator = queue_.begin(); iterator != queue_.end(); iterator++) {
            if (iterator->get()->txn_ == txn) {
                request = *iterator;
                break;
            }
        }
        assert(request != nullptr);
        return request;

    }
};

class LockManager {

public:
    LockManager(bool strict_2PL);

    /*** below are APIs need to implement ***/
    // lock:
    // return false if transaction is aborted
    // it should be blocked on waiting and should return true when granted
    // note the behavior of trying to lock locked rids by same txn is undefined
    // it is transaction's job to keep track of its current locks
    bool LockShared(Transaction *txn, const RID &rid);
    bool LockExclusive(Transaction *txn, const RID &rid);
    bool LockUpgrade(Transaction *txn, const RID &rid);

    // unlock:
    // release the lock hold by the txn
    bool Unlock(Transaction *txn, const RID &rid);
    /*** END OF APIs ***/

private:
    bool strict_2PL_;
    std::mutex mutex_;

    /** transaction timestamp related variables **/
    int current_timestamp_ = 0;
    HashTable<txn_id_t, int>* txn_timestamp_table_;
    // assign/get timestamp to transaction, return the timestamp
    int AssignTransactionTimestamp(const Transaction* transaction);
    int AssignTransactionTimestamp(const txn_id_t txn_id);

    /** lock request related variables **/
    ExtendibleHash<RID, std::shared_ptr<LockQueue>>* rid_lock_table_;
    // abort current transaction
    inline bool AbortTransaction(Transaction* transaction) {
        //TODO: when to remove transaction from current timestamp table
        transaction->SetState(TransactionState::ABORTED);
        return false;
    }
    // check if current LockRequest is compatible with existing
    bool CheckLockRequestCompatibility(const std::shared_ptr<LockRequest>& request);


    // grant a lock
    void GrantLock(std::shared_ptr<LockRequest>& request);

    // add lock request to queue
    void AddLockRequestToQueue(const std::shared_ptr<LockRequest>& request);

    // check whether this lock request satisfies deadlock prevention rule
    bool CheckDeadlockPolicy(std::shared_ptr<LockRequest>& request);

    // wait for current lock to be granted or aborted
    bool WaitForLockRequest(std::shared_ptr<LockRequest>& request,
                            std::unique_lock<std::mutex>& lock_manager);

    // check if lock request is compatibility with (strict) 2PL protocol
    bool CheckLockRequestLegalityUnder2PL(std::shared_ptr<LockRequest>& lock_request) const;
    bool CheckUpgradeRequestLegalityUnder2PL(std::shared_ptr<LockRequest>& lock_request) const;

    // check legality of unlock request under (strict) 2PL protocol
    bool CheckUnlockRequestLegalityUnder2PL(const std::shared_ptr<LockRequest>& lock_request) const;

    void UpdateFollowingLockRequests(std::shared_ptr<LockQueue> & lock_queue,
                                     std::shared_ptr<LockRequest> & request);

    std::shared_ptr<LockQueue> FetchLockQueue(const RID& rid);

    bool CheckLockUpgradeCompatibility(const std::shared_ptr<LockRequest>& request);

    void GrantLockUpgrade(std::shared_ptr<LockRequest> & request);

    bool CheckDeadlockPolicyForUpgrade(std::shared_ptr<LockRequest>& request);

    bool WaitForLockUpgrade(std::shared_ptr<LockRequest>& lock_request,
                            std::unique_lock<std::mutex>& manager_lock);

    bool UnlockRequest(std::shared_ptr<LockRequest>& request);







    };

} // namespace cmudb
