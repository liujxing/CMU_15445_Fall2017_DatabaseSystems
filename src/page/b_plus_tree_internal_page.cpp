/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType)));

}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    assert(index >= 0 && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    assert(index >= 0 && index < GetMaxSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); i++) {
        if (array[i].second == value)
            return i;
    }
    return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(index >= 0 && index < GetSize());
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    if (comparator(key, KeyAt(1)) < 0) return ValueAt(0);
    if (comparator(key, KeyAt(GetSize()-1)) >= 0) return ValueAt(GetSize()-1);
    for (int i = 1; i < GetSize()-1; i++) {
        if (comparator(key, KeyAt(i)) >= 0 && comparator(key, KeyAt(i+1)) <= 0)
            return ValueAt(i);
    }
    return INVALID_PAGE_ID; // this line is never called
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    // This method is called when the root is split.
    // old_value is the pointer to the old root
    // new_key is the
    // new_value is the pointer to the new root

    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {

    array[0].second = old_value;
    array[1] = std::make_pair(new_key, new_value);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {

    for (int i = 0; i < GetSize(); i++) {
        if (ValueAt(i) == old_value) {
            for (int j = GetSize() - 1; j > i; j--) {
                array[j+1] = array[j];
            }
            array[i+1] = std::make_pair(new_key, new_value);
            SetSize(GetSize()+1);
            break;
        }
    }
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    //TODO: buffer_pool_manager is not used in here, perhaps to be used to set the parent_page_id of all child nodes

    // It is guaranteed that current size of node is equal to MaxSize + 1
    assert(GetSize() == GetMaxSize() + 1);

    // It is guaranteed the recipient is an empty page
    assert(recipient->GetSize() == 0);

    // move elements
    const int start_index = (GetSize()+1)/2;
    int recipient_index = 0;
    for (int i = start_index; i < GetSize(); i++) {
        recipient->array[recipient_index] = array[i];
        recipient_index++;
    }
    SetSize(start_index);
    recipient->SetSize(recipient_index);
    assert(GetSize() + recipient->GetSize() == GetMaxSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    assert(index >= 0 && index < GetSize());
    for (int i = index; i < GetSize()-1; i++) {
        array[i] = array[i+1];
    }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  return INVALID_PAGE_ID;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {

    // It is guaranteed the current page is immediately to the right of recipient page and share the same parent page.
    // It is guaranteed the current page and recipient page elements can fit in a single page.

    // move item from current node to recipient node
    for (int i = 0; i < GetSize(); i++) {
        recipient->array[recipient->GetSize()+i] = array[i];
    }
    // also need to copy the parent key into the key position
    Page* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent_page_data = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
    recipient->SetKeyAt(recipient->GetSize(), parent_page_data->KeyAt(index_in_parent));
    // set size of nodes
    recipient->IncreaseSize(GetSize());
    SetSize(0);
    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), false);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {


    // It is guaranteed the recipient is the immediate left neighbor of current node.
    // It is guaranteed the recipient is the child 0 of parent.

    // get the element of parent that separates this node and recipient
    const int node_parent_index = 1;
    Page* parent_page = buffer_pool_manager->FetchPage(GetPageId());
    auto parent_page_data = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
    const KeyType separating_key = parent_page_data->KeyAt(node_parent_index);

    // move element
    recipient->array[recipient->GetSize()] = array[0];
    recipient->SetSize(recipient->GetSize()+1);
    for (int i = 0; i < GetSize()-1; i++)
        array[i] = array[i+1];
    SetSize(GetSize()-1);

    // copy the separating key into recipient
    recipient->SetKeyAt(recipient->GetSize()-1, separating_key);

    // set parent key by the current first key of current node
    parent_page_data->SetKeyAt(node_parent_index, KeyAt(0));

    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {

    // It is guaranteed recipient is the immediate right neighbor of current node.

    // get the element of parent that separates this node and recipient
    const int recipient_parent_index = parent_index + 1;
    Page* parent_page = buffer_pool_manager->FetchPage(GetPageId());
    auto parent_page_data = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
    const KeyType separating_key = parent_page_data->KeyAt(recipient_parent_index);

    // move the element
    for (int i = recipient->GetSize()-1; i >= 0; i--) {
        recipient->array[i+1] = recipient->array[i];
    }
    recipient->array[0] = array[GetSize()-1];
    recipient->IncreaseSize(1);
    this->IncreaseSize(-1);

    // copy the separating key into recipient
    recipient->SetKeyAt(1, separating_key);

    // set parent key by the moved element key
    parent_page_data->array[recipient_parent_index].first = recipient->array[0].first;

    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
