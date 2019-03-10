/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {


// operation mode of B-Plus tree
enum class Mode {
    LOOKUP, INSERT, DELETE
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                           BufferPoolManager *buffer_pool_manager,
                           const KeyComparator &comparator,
                           page_id_t root_page_id = INVALID_PAGE_ID);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // Print this B+ tree to stdout using a simple command-line
  std::string ToString(bool verbose = false);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // expose for test purpose
  B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key,
                                           Transaction* txn,
                                           Mode mode,
                                           bool leftMost = false);

private:
  void StartNewTree(const KeyType &key, const ValueType &value, Transaction* transaction);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N> N *Split(N *node, Transaction* transaction);

  template <typename N>
  bool CoalesceOrRedistribute(N *& node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(
      N *&neighbor_node, N *&node,
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
      int index, Transaction *transaction = nullptr);

  template <typename N> void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = false);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  mutable std::mutex root_mutex_; // mutex used to protect the root node
  bool root_locked_; // boolean to indicate whether the root node is locked

  // helper methods
  // lock current root id
  void LockRoot();
  // unlock root
  void UnlockRoot();


  // lock a page according to mode, and add to the set of transactions
  void LockPage(Page* page, Transaction* txn, Mode mode);
  // check whether it is safe to release lock on a page when walking down the B-Plus tree
  bool IsSafeToRelease(const BPlusTreePage* page, const Mode mode) const;
  // unlock a single page held by a transaction, remove it from transaction, and unpin the page
  void UnlockUnpinSinglePage(Page* page, Transaction* txn, const Mode mode, const bool is_dirty = false);
  // unlock all pages held by a transaction, remove them from transaction, and unpin the pages
  void UnlockUnpinAllPagesInTransaction(Transaction *txn, const Mode mode);
  // unlock all pages held by transaction except for the input page, remove them from transaction, and unpin the pages
  void UnlockUnpinPageExcept(Page *page, Transaction *txn, const Mode mode);
  // unlock the last page store in transaction
  void UnlockUnpinLastPageInTransaction(Transaction* txn, const Mode mode);

};

} // namespace cmudb
