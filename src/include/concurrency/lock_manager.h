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
    const LockType lock_type_;
    const txn_id_t transaction_id_;
    const int timestamp_;

    // constructors
    LockRequest(const RID& rid, const bool granted,
                const LockType& lock_type, const txn_id_t transaction_id, const int timestamp):
            rid_(rid),
            granted_(granted),
            lock_type_(lock_type),
            transaction_id_(transaction_id),
            timestamp_(timestamp)
    {};

    LockRequest(const RID& rid, const LockType& lock_type, const txn_id_t transaction_id, const int timestamp):
            rid_(rid),
            granted_(false),
            lock_type_(lock_type),
            transaction_id_(transaction_id),
            timestamp_(timestamp)
    {};

    LockRequest(const LockRequest&) = default;

    // destructor
    ~LockRequest() = default;

    // compatibility function with another lock
    bool IsCompatible(const LockRequest& other) const {
        // TODO: add assertion check on whether this is a duplicate request
        if (!(rid_ == other.rid_)) return true;
        if (transaction_id_ == other.transaction_id_) return true;
        if (lock_type_ == LockType::S && other.lock_type_ == LockType::S) return true;
        return false;
    }
};

struct LockQueue {
    std::condition_variable condition_variable_;
    std::deque<LockRequest> queue_;
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
    bool CheckLockRequestCompatibility(const LockRequest& request);


    // grant a lock
    void GrantLock(LockRequest& request, Transaction* txn);

    // add lock request to queue
    void AddLockRequestToQueue(const LockRequest& request);

    // check whether this lock request satisfies deadlock prevention rule
    bool CheckDeadlockPolicy(LockRequest& request);

    // wait for current lock to be granted or aborted
    bool WaitForLockRequest(LockRequest& request, std::unique_lock<std::mutex>& lock_manager);

    // check if lock request is compatibility with (strict) 2PL protocol
    bool CheckLockRequestLegalityUnder2PL(Transaction* txn) const;
    bool CheckUpgradeRequestLegalityUnder2PL(Transaction* txn) const;

    // check legality of unlock request under (strict) 2PL protocol
    bool CheckUnlockReqestLegalityUnder2PL(const LockRequest& lock_request, Transaction* txn) const;




};

} // namespace cmudb
