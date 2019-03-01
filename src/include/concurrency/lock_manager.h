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

#include "hash/extendible_hash.h"
#include "concurrency/lock_request.h"

namespace cmudb {



class LockManager {

public:
    LockManager(bool strict_2PL);
    ~LockManager();

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

    /************************* transaction timestamp related variables *************/
    int current_timestamp_ = 0;
    HashTable<txn_id_t, int>* txn_timestamp_table_;

    // assign/get timestamp to transaction, return the timestamp
    int AssignTransactionTimestamp(const Transaction* transaction);
    int AssignTransactionTimestamp(const txn_id_t txn_id);


    /************************ lock request related variables ************************/
    HashTable<RID, std::shared_ptr<LockQueue>>* rid_lock_table_;

    // Fetch a LockQueue with given rid
    std::shared_ptr<LockQueue> FetchLockQueue(const RID& rid);


    /************************ lock request compatibility with existing locks related variables *************/
    // check if current lock request is compatible with existing
    bool CheckLockRequestCompatibility(const std::shared_ptr<LockRequest>& request);

    // check if current lock upgrade is compatible with existing
    bool CheckLockUpgradeCompatibility(const std::shared_ptr<LockRequest>& request);

    // grant a lock request
    void GrantLockRequest(std::shared_ptr<LockRequest> &request);

    // grant a lock upgrade
    void GrantLockUpgrade(std::shared_ptr<LockRequest> & request);

    // add lock request to queue
    void AddLockRequestToQueue(const std::shared_ptr<LockRequest>& request);


    /************************ deadlock prevention related variables *************/

    // check whether this lock request satisfies deadlock prevention policy
    bool CheckDeadlockPolicyForRequest(std::shared_ptr<LockRequest> &request);

    // check whether this lock upgrade satisfies deadlock prevention policy
    bool CheckDeadlockPolicyForUpgrade(std::shared_ptr<LockRequest>& request);


    /************************ thread communication related variables ****************/

    // wait for current lock request to be granted or aborted
    bool WaitForLockRequest(std::shared_ptr<LockRequest>& request,
                            std::unique_lock<std::mutex>& lock_manager);

    // wait for current lock upgrade to be granted or aborted
    bool WaitForLockUpgrade(std::shared_ptr<LockRequest>& lock_request,
                            std::unique_lock<std::mutex>& manager_lock);

    // unlock current requested lock
    bool UnlockRequest(std::shared_ptr<LockRequest>& request);


    /************************* 2PL related methods **********************************/
    // check if a new lock request is compatible with (strict) 2PL protocol
    bool CheckLockRequestLegalityUnder2PL(std::shared_ptr<LockRequest>& lock_request) const;
    // check if a lock upgrade request is compatible with (strict) 2PL protocol
    bool CheckUpgradeRequestLegalityUnder2PL(std::shared_ptr<LockRequest>& lock_request) const;
    // check if a unlock request is compatible with (strict) 2PL protocol
    bool CheckUnlockRequestLegalityUnder2PL(const std::shared_ptr<LockRequest>& lock_request) const;

    };

} // namespace cmudb
