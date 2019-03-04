/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
#include "common/logger.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / (sizeof(KeyType) + sizeof(ValueType)) - 1);
    SetNextPageId(INVALID_PAGE_ID);
    /*
    LOG_DEBUG("PAGE_SIZE:%d, sizeof(BPlusTreeLeafPage):%d, sizeof(KeyType):%d, sizeof(ValueType):%d",
    PAGE_SIZE, int(sizeof(BPlusTreeLeafPage)), int(sizeof(KeyType)), int(sizeof(ValueType)));
     */
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return this->next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    this->next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
    for (int i = 0; i < GetSize(); i++) {
        if (comparator(KeyAt(i), key) >= 0)
            return i;
    }
    return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
    // replace with your own code
    assert(index >= 0 && index < GetSize());
    return this->array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
    // replace with your own code
    assert(index >= 0 && index < GetSize());
    return this->array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    // After insertion, the size can be temporarily larger than the max size
    // TODO: rewrite using memmove and binary search

    // find the place of the first index >= key
    const int insert_index = this->KeyIndex(key, comparator);
    if (insert_index == -1) {
        this->array[GetSize()] = std::make_pair(key, value);
        IncreaseSize(1);
        return GetSize();
    }
    for (int i = GetSize()-1; i >= insert_index; i--) {
        this->array[i+1] = this->array[i];
    }
    this->array[insert_index] = std::make_pair(key, value);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {

    // It is guaranteed that current size of node is equal to MaxSize + 1
    assert(GetSize() == GetMaxSize() + 1);

    // It is guaranteed that the recipient is a newly created empty page
    assert(recipient->GetSize() == 0);

    // move the elements
    // TODO: the start index might not be textbook value
    const int start_index = GetMaxSize()/2 + 1;
    recipient->CopyHalfFrom(&array[start_index], GetSize() - start_index);
    SetSize(start_index);
    assert(GetSize() + recipient->GetSize() == GetMaxSize() + 1);

    // set the next page pointer
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    // MappingType* is the starting position of an MappingType array, size is the number of items to copy

    // Only works if current leaf page is empty
    assert(GetSize() == 0);

    for (int i = 0; i < size; i++) {
        array[i] = items[i];
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    for (int i = 0; i < GetSize(); i++) {
        if (comparator(key, KeyAt(i)) == 0) {
            value = this->array[i].second;
            return true;
        }
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
    for (int i = 0; i < GetSize(); i++) {
        // find the element
        if (comparator(KeyAt(i), key) == 0) {
            // erase element
            for (int j = i; j < GetSize() - 1; j++) {
                array[j] = array[j+1];
            }
            IncreaseSize(-1);
            return GetSize();
        }
    }
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {


    // It is guaranteed the current page is immediately to the right of recipient page and share the same parent page.
    // It is guaranteed the current page and recipient page elements can fit in a single page.

    // move item from current node to recipient node
    recipient->CopyAllFrom(array, GetSize());
    // set size of nodes
    SetSize(0);

    // set next_page_id
    recipient->SetNextPageId(GetNextPageId());
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    for (int i = 0; i < size; i++) {
        array[GetSize()+i] = items[i];
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

    // It is guaranteed the recipient is the immediate left neighbor of current node.
    // It is guaranteed the recipient is the child 0 of parent.

    // move element
    recipient->CopyLastFrom(array[0]);
    for (int i = 0; i < GetSize()-1; i++)
        array[i] = array[i+1];
    IncreaseSize(-1);

    // change the parent pointer
    const int node_parent_index = 1;
    Page* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
    assert(parent_page_data->ValueAt(0) == recipient->GetPageId());
    assert(parent_page_data->ValueAt(1) == GetPageId());
    parent_page_data->SetKeyAt(node_parent_index, KeyAt(0));
    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), true);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array[GetSize()] = item;
    IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

    // It is guaranteed recipient is the immediate right neighbor of current node.
    assert(GetNextPageId() == recipient->GetPageId());

    // move the element
    recipient->CopyFirstFrom(GetItem(GetSize()-1), parentIndex+1, buffer_pool_manager);
    IncreaseSize(-1);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

    // move the element
    for (int i = GetSize()-1; i >= 0; i--) {
        array[i+1] = array[i];
    }
    // insert the first element
    array[0] = item;
    IncreaseSize(1);

    // change the pointer in the parent
    Page* parent_page = buffer_pool_manager->FetchPage(GetPageId());
    auto parent_page_data = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
    parent_page_data->SetKeyAt(parentIndex, array[0].first);
    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);


}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RedirectParentIdOfChildPages(BPlusTreeLeafPage* neighbor,
                                                              BufferPoolManager* buffer_pool_manager) {}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId:" << GetPageId() << ",parentId:" << GetParentPageId() << ",nextId:" << GetNextPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    /*
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
     */
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
