/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    // remove all data in result
    result.clear();

    // try to find the value on a page
    if (IsEmpty()) return false;

    // Lock the root
    LockRoot();

    // fetch the leaf page
    B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = FindLeafPage(key, transaction, Mode::LOOKUP, false);

    // search the key
    ValueType value;
    const bool find_key = leaf_page->Lookup(key, value, comparator_);
    if (find_key) result.emplace_back(value);

    // unlock and unpin the pages
    assert(transaction == nullptr || transaction->GetPageSet()->empty());
    Page* page = buffer_pool_manager_->FetchPage(leaf_page->GetPageId());
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    return find_key;

}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    LockRoot();

    // check if the tree is empty
    if (IsEmpty()) {
        StartNewTree(key, value);
        UnlockRoot();
        return true;
    } else {
        // insert the node into leaf page
        return InsertIntoLeaf(key, value, transaction);
    }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    // get page from buffer pool manager
    page_id_t page_id;
    Page* page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr) throw std::bad_alloc();
    // The new leaf page does not need to be locked

    // make this new page leaf node
    this->root_page_id_ = page_id;
    UpdateRootPageId(true);

    // convert this page data into leaf page
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(page->GetData());
    leaf_page->Init(page_id);
    // insert entry into leaf_page
    leaf_page->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {

    // find the leaf page potentially containing the insertion target
    B_PLUS_TREE_LEAF_PAGE_TYPE* page_data = FindLeafPage(key, transaction, Mode::INSERT, false);

    // check if the data is within the leaf page
    ValueType value_temp;
    if (page_data->Lookup(key, value_temp, comparator_)) {
        ReleaseAllLocksFromTransaction(transaction, Mode::INSERT);
        return false;
    }

    // if insertion does not cause overflow, insert record into leaf page
    if (page_data->GetSize() < page_data->GetMaxSize()) {
        page_data->Insert(key, value, comparator_);
        ReleaseAllLocksFromTransaction(transaction, Mode::INSERT);
        return true;
    }

    // insertion causes overflow, need to split the leaf node
    page_data->Insert(key, value, comparator_); // here the size of page_data is temporarily larger than max_size
    auto companion_leaf_page = Split<B_PLUS_TREE_LEAF_PAGE_TYPE>(page_data);
    InsertIntoParent(page_data, companion_leaf_page->KeyAt(0), companion_leaf_page, transaction);
    buffer_pool_manager_->UnpinPage(companion_leaf_page->GetPageId(), true);
    ReleaseAllLocksFromTransaction(transaction, Mode::INSERT);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    // get a new page from buffer pool
    page_id_t page_id;
    Page* page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr) throw std::bad_alloc();

    // cast page to leaf/internal page
    auto recipient_page = reinterpret_cast<N*>(page->GetData());
    recipient_page->Init(page_id, node->GetParentPageId());

    // move half of the key & value pair from input page to newly created page
    node->MoveHalfTo(recipient_page, buffer_pool_manager_);
    return recipient_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    // It is guaranteed the key is the smallest key in new_node

    // the root page is split
    if (old_node->IsRootPage()) {
        // create the new root page
        page_id_t new_root_page_id;
        Page* new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);
        if (new_root_page == nullptr) throw std::bad_alloc();
        auto new_root_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(new_root_page->GetData());
        new_root_page_data->Init(new_root_page_id);

        // set the new root page to point to the original root and its companion node
        old_node->SetParentPageId(new_root_page_id);
        new_node->SetParentPageId(new_root_page_id);
        new_root_page_data->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

        // set the new page as root of tree
        root_page_id_ = new_root_page_data->GetPageId();
        UpdateRootPageId();

        // unpin the pages from buffer pool
        buffer_pool_manager_->UnpinPage(new_root_page_data->GetPageId(), true);
        UnlockRoot();
    } else { // a non-root page is split

        // fetch the parent page
        page_id_t parent_page_id = old_node->GetParentPageId();
        Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
        auto parent_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());

        // insert into parent data
        const int parent_size = parent_page_data->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

        // check if parent page need to split
        if (parent_size <= parent_page_data->GetMaxSize()) {
            buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
            return;
        }
        else {
            auto companion_page = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(parent_page_data);
            const KeyType push_up_key = companion_page->KeyAt(0);

            InsertIntoParent(parent_page_data, push_up_key, companion_page);
            buffer_pool_manager_->UnpinPage(parent_page_data->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(companion_page->GetPageId(), true);
        }
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    // return if current tree is empty
    if (IsEmpty()) return;

    // delete data from corresponding page
    LockRoot();
    // leaf page is WLatched and is added to transaction PageSet
    B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page_data = FindLeafPage(key, transaction, Mode::DELETE, false);
    const int page_size_after_deletion = leaf_page_data->RemoveAndDeleteRecord(key, comparator_);

    // if the page is pretty empty, need to redistribute or coalesce the page
    bool need_to_delete = false;
    if (page_size_after_deletion < leaf_page_data->GetMinSize()) {
        need_to_delete = CoalesceOrRedistribute<B_PLUS_TREE_LEAF_PAGE_TYPE>(leaf_page_data, transaction);
    }

    // if the leaf page needs to be deleted, add it to the transaction's page set
    if (need_to_delete) transaction->AddIntoDeletedPageSet(leaf_page_data->GetPageId());

    // unlock and unpin all pages
    ReleaseAllLocksFromTransaction(transaction, Mode::DELETE);

}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
    // the node is already WLatched and is added to transaction PageSet
    if (node->GetSize() >= node->GetMinSize()) {
        return false;
    }

    // node is root and node has only one remaining child, make the child page as root page
    if (node->IsRootPage()) {
        return AdjustRoot(node);
    }

    // get the sibling: if has left sibling then get left sibling, otherwise get right sibling
    // since we are dealing with current page, it is guaranteed the parent page is also locked
    // There is still possibility that the sibling is being updated by other pages, such as a safe insert on sibling page
    // Therefore, we also need to lock the sibling page
    const page_id_t parent_page_id = node->GetParentPageId();
    auto parent_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    const int node_index_in_parent = parent_page_data->ValueIndex(node->GetPageId());
    page_id_t sibling_page_id;
    if (node_index_in_parent > 0) sibling_page_id = parent_page_data->ValueAt(node_index_in_parent-1);
    else sibling_page_id = parent_page_data->ValueAt(node_index_in_parent+1);
    auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    auto sibling_page_data = reinterpret_cast<N*>(sibling_page->GetData());

    // also need to lock the sibling
    LockPage(sibling_page, transaction, Mode::DELETE);

    // if total number of nodes smaller than MaxSize(), then coalesce
    bool need_to_delete;
    bool parent_need_to_delete;
    if (sibling_page_data->GetSize() + node->GetSize() <= node->GetMaxSize()) {
        parent_need_to_delete = Coalesce(sibling_page_data, node, parent_page_data, node_index_in_parent, transaction);
        need_to_delete = true;
    } else { // greater or equal than MaxSize(), redistribute
        Redistribute(sibling_page_data, node, node_index_in_parent);
        parent_need_to_delete = false;
        need_to_delete = false;
    }

    // here the parent page is fetched again, so need to unpin again. On the other hand, the sibling page is fetched the first time, so no need to unpin
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    if (parent_need_to_delete) transaction->AddIntoDeletedPageSet(parent_page_id);

    return need_to_delete;

}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {

    // index is the index of node in parent
    assert(parent->ValueIndex(node->GetPageId()) == index);
    // It is guaranteed the node is immediately to the right of neighbor_node and share the same parent page.
    // swap node and neighbor_node if node is at 0 position
    if (index == 0) {
        N* temp = node;
        node = neighbor_node;
        neighbor_node = temp;
        index = 1;
    }
    assert(parent->ValueIndex(node->GetPageId()) - parent->ValueIndex(neighbor_node->GetPageId()) == 1);

    // let the parent_page_id of nodes of all nodes to point to the sibling nodes
    if (!node->IsLeafPage()) {
        node->RedirectParentIdOfChildPages(neighbor_node, buffer_pool_manager_);
    }

    // move all of node content to its sibling page, the parent page is not updated
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);

    // recursively delete the key at index from parent node
    parent->Remove(index);
    return CoalesceOrRedistribute<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(parent, transaction);

}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {

    // it is guaranteed the node and neighbor_node are already latched

    // node is the first element of parent, hence node is to the left of neighbor_node
    // therefore borrow the first element from neighbor_node to node end
    // TODO: might need to latch when calling the methods below
    if (index == 0) {
        neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    // node is to the right of neighbor_node, hence borrow the last element from neighbor
    } else {
        const int neighbor_node_index = index - 1;
        neighbor_node->MoveLastToFrontOf(node, neighbor_node_index, buffer_pool_manager_);
    }

    // the neighbor_node and node maintain their latching condition

}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    // has no element left, the tree becomes empty
    if (old_root_node->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        return true;
    }

    // has one element left and the root node is not a leaf node
    if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
        // set root to the child of current page
        const auto root_data = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(old_root_node);
        const page_id_t child_page_id = root_data->ValueAt(0);
        root_page_id_ = child_page_id;
        UpdateRootPageId();

        // set the parent id of the new root page
        // TODO: might need to latch the child page
        Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
        auto child_page_data = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
        child_page_data->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(child_page_data->GetPageId(), true);

        return true;
    }

    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {

    // find the leftmost leaf page
    B_PLUS_TREE_LEAF_PAGE_TYPE* leftmost_leaf_page = nullptr;
    if (!IsEmpty()) {
        page_id_t page_id = root_page_id_;
        BPlusTreePage* tree_page;
        while (true) {
            Page* page = buffer_pool_manager_->FetchPage(page_id);
            tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
            if (tree_page->IsLeafPage()) {
                leftmost_leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
                break;
            } else {

                page_id = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData())->ValueAt(0);
            }
        }
    }
    // generate the iterator
    return INDEXITERATOR_TYPE(leftmost_leaf_page, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {

    // find the leaf page containing the input key
    B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = this->FindLeafPage(key, nullptr, Mode::LOOKUP, true);
    int index_in_leaf_page = leaf_page->KeyIndex(key, comparator_);
    // generate the iterator
    return INDEXITERATOR_TYPE(leaf_page, buffer_pool_manager_, index_in_leaf_page);

}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         Transaction* transaction,
                                                         const Mode mode,
                                                         bool leftMost) {

    // Here we assume the root is already locked from outside
    Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (mode == Mode::LOOKUP) page->RLatch();
    else LockPage(page, transaction, mode);
    auto page_data = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // deal with the case when the root is leaf
    if (page_data->IsLeafPage()) {
        if (mode == Mode::LOOKUP) UnlockRoot();
    }

    while (!page_data->IsLeafPage()) {
        // fetch child page
        auto internal_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
        Page* child_page;
        if (leftMost) child_page = buffer_pool_manager_->FetchPage(internal_page_data->ValueAt(0));
        else child_page = buffer_pool_manager_->FetchPage(internal_page_data->Lookup(key, comparator_));

        // lock child page but does not add to the transaction yet
        if (mode == Mode::LOOKUP) child_page->RLatch();
        else child_page->WLatch();

        // check if it is safe to release parent locks
        if (mode == Mode::LOOKUP) {
            if (page_data->IsRootPage()) UnlockRoot();
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        } else {
            if (IsSafeToRelease(child_page, mode)) ReleaseAllLocksFromTransaction(transaction, mode);
        }

        // add the page to transaction page set
        if (transaction != nullptr) transaction->GetPageSet()->emplace_back(child_page);

        // set the pointers
        page = child_page;
        page_data = reinterpret_cast<BPlusTreePage*>(page->GetData());
    }

    // status: the page is locked in R/W mode, and if the page is safe then the parent pages are unlocked and unpinned
    return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockRoot() {
    root_lock_.lock();
    root_locked_ = true;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockRoot() {
    root_locked_ = false;
    root_lock_.unlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page* page, Transaction* transaction, const Mode mode) {
    // lock the page
    if (mode == Mode::LOOKUP) page->RLatch();
    else page->WLatch();

    // add the page to transaction's page set
    if (transaction == nullptr) return;
    transaction->AddIntoPageSet(page);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafeToRelease(Page *page, const Mode mode) const {
    switch (mode) {
        case Mode::LOOKUP:
            return true;
        case Mode::INSERT: {
            auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
            return page_data->GetSize() < page_data->GetMaxSize();
            }
        case Mode::DELETE: {
            auto page_data = reinterpret_cast<BPlusTreePage *>(page->GetData());
            return page_data->GetSize() > page_data->GetMinSize(); // TODO: check whether DELETE uses this condition
            }
    }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllLocksFromTransaction(Transaction* transaction, const Mode mode) {
    if (transaction == nullptr) return;
    auto page_set = transaction->GetPageSet();

    // unlock and unpin all pages
    if (mode == Mode::LOOKUP) {
        bool root_unlocked = false;
        for (auto iterator = page_set->begin(); iterator != page_set->end(); iterator++) {
            auto page_data = reinterpret_cast<BPlusTreePage*>((*iterator)->GetData());
            if (page_data->IsRootPage() && !root_unlocked) {
                UnlockRoot();
                root_unlocked = true;
            }
            (*iterator)->RUnlatch();
            buffer_pool_manager_->UnpinPage((*iterator)->GetPageId(), false);
        }
    } else {
        bool root_unlocked = false;
        for (auto iterator = page_set->begin(); iterator != page_set->end(); iterator++) {
            auto page_data = reinterpret_cast<BPlusTreePage*>((*iterator)->GetData());
            if (page_data->IsRootPage() && !root_unlocked) {
                UnlockRoot();
                root_unlocked = true;
            }
            (*iterator)->WUnlatch();
            buffer_pool_manager_->UnpinPage((*iterator)->GetPageId(), true); // TODO: probably better to set is_dirty to false
        }
    }

    // clear the pages from transaction
    page_set->clear();

    // delete pages in DELETE mode
    if (mode == Mode::DELETE) {
        auto delete_page_set = transaction->GetDeletedPageSet();
        for (auto iterator = delete_page_set->begin(); iterator != delete_page_set->end(); iterator++) {
            buffer_pool_manager_->DeletePage(*iterator);
        }
        delete_page_set->clear();
    }


}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
    if (IsEmpty()) return "Empty tree";

    std::ostringstream stream;
    std::queue<page_id_t> list;
    list.push(root_page_id_);

    while (!list.empty()) {
        std::queue<page_id_t> next_list;
        stream << "|";

        while (!list.empty()) {
            page_id_t page_id = list.front();
            list.pop();
            auto page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(page_id)->GetData());

            if (page->IsLeafPage()) {
                auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
                stream << leaf_page->ToString(verbose) << "|";
            } else {
                auto internal_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page);
                stream << internal_page->ToString(verbose) << "|";
                for (int i = 0; i < internal_page->GetSize(); i++) {
                    next_list.push(internal_page->ValueAt(i));
                }
            }
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        }

        stream << "\n";
        list.swap(next_list);
    }
    return stream.str();
}



/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
