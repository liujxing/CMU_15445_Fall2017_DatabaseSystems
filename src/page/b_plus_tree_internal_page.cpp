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
    SetSize(0); // TODO: the SetSize is 0 or 1?
    SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType)) - 1);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    if (index < 0) {
        std::stringstream stream;
        stream << "The index is " << index << "\n";
        throw std::invalid_argument(stream.str());
    }
    if (index >= GetSize()) {
        std::stringstream stream;
        stream << "The index is " << index << " and the size is " << GetSize() << ",PageID:" << GetPageId() << "\n";
        throw std::invalid_argument(stream.str());
    }
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
    assert(GetSize() > 0);
    if (comparator(key, KeyAt(1)) < 0) return ValueAt(0);
    if (comparator(key, KeyAt(GetSize()-1)) >= 0) return ValueAt(GetSize()-1);
    for (int i = 1; i < GetSize()-1; i++) {
        if (comparator(key, KeyAt(i)) >= 0 && comparator(key, KeyAt(i+1)) < 0)
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
    SetSize(2);
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
            IncreaseSize(1);
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

    // It is guaranteed that current size of node is equal to MaxSize + 1
    assert(GetSize() == GetMaxSize() + 1);

    // It is guaranteed the recipient is an empty page
    assert(recipient->GetSize() == 0);

    // move elements
    // TODO: the start index might not be textbook value
    const int start_index = (GetSize()+1)/2;
    recipient->CopyHalfFrom(&array[start_index], GetSize()-start_index, buffer_pool_manager);
    SetSize(start_index);
    assert(GetSize() + recipient->GetSize() == GetMaxSize() + 1);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    // Only works if current internal page is empty
    assert(GetSize() == 0);

    for (int i = 0; i < size; i++) {
        // copy the element
        array[i] = items[i];

        // redirect the parent page ID of copied pages
        // TODO: not sure whether need to lock the child pages
        Page* child_page = buffer_pool_manager->FetchPage(array[i].second);
        child_page->WLatch();
        auto child_page_data = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
        child_page_data->SetParentPageId(GetPageId());
        child_page->WUnlatch();
        buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
    }
    IncreaseSize(size);
}

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
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    const ValueType value = ValueAt(0);
    SetSize(0);
    return value;
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
    const int recipient_size_before_copy = recipient->GetSize();
    recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);

    // TODO: not sure whether should update the pointer in parent_page
    // also need to copy the parent key into the key position
    Page* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent_page_data = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
    recipient->SetKeyAt(recipient_size_before_copy, parent_page_data->KeyAt(index_in_parent));
    // set size of nodes
    SetSize(0);
    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), false);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {

    // TODO: not sure why buffer_pool_manager is needed here
    for (int i = 0; i < size; i++) {
        array[GetSize()+i] = items[i];
    }
    IncreaseSize(size);
}

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

    // TODO: completely different from other people's code

    // It is guaranteed the recipient is the immediate left neighbor of current node.
    // It is guaranteed the recipient is the child 0 of parent.

    // get the element of parent that separates this node and recipient
    const int node_parent_index = 1;
    Page* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent_page_data = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
    assert(parent_page_data->ValueAt(0) == recipient->GetPageId());
    assert(parent_page_data->ValueAt(1) == GetPageId());
    const KeyType separating_key = parent_page_data->KeyAt(node_parent_index);

    // move element
    recipient->CopyLastFrom(array[0], buffer_pool_manager);
    for (int i = 0; i < GetSize()-1; i++)
        array[i] = array[i+1];
    IncreaseSize(-1);

    // copy the separating key into recipient
    recipient->SetKeyAt(recipient->GetSize()-1, separating_key);

    // set parent key by the current first key of current node
    parent_page_data->SetKeyAt(node_parent_index, KeyAt(0));

    // set parentId of moved page
    const page_id_t moved_page_id = recipient->ValueAt(recipient->GetSize()-1);
    auto moved_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(moved_page_id)->GetData());
    moved_page->SetParentPageId(recipient->GetPageId());

    // unpin the pages
    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), true);
    buffer_pool_manager->UnpinPage(moved_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    // TODO: not sure why need buffer_pool_manager
    array[GetSize()] = pair;
    IncreaseSize(1);
}

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

    // set the ParentId of moved page
    const page_id_t moved_page_id = recipient->ValueAt(0);
    auto moved_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(moved_page_id)->GetData());
    moved_page->SetParentPageId(recipient->GetPageId());

    // unpin the pages
    buffer_pool_manager->UnpinPage(parent_page_data->GetPageId(), true);
    buffer_pool_manager->UnpinPage(moved_page->GetPageId(), true);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RedirectParentIdOfChildPages(BPlusTreeInternalPage* neighbor,
                                                                  BufferPoolManager* buffer_pool_manager) {
    for (int i = 0; i < GetSize(); i++) {
        ValueType child_page_id = ValueAt(i);
        auto child_page_data = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(child_page_id)->GetData());
        child_page_data->SetParentPageId(neighbor->GetPageId());
        buffer_pool_manager->UnpinPage(child_page_id, true);
    }
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    // TODO: completely different from other people's code
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr) {
        std::cout << "fetching PageId:" << array[i].second << "\n";
        throw Exception(EXCEPTION_TYPE_INDEX,
                        "all page are pinned while printing");
    }
    auto node =
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
