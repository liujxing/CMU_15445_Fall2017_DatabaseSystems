/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(
        B_PLUS_TREE_LEAF_PAGE_TYPE* start_leaf_page,
        BufferPoolManager* buffer_pool_manager,
        const int index_in_leaf_page
): current_leaf_page(start_leaf_page), buffer_pool_manager(buffer_pool_manager), index_in_leaf_page(index_in_leaf_page){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {return current_leaf_page == nullptr;}

INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*() {
    assert(current_leaf_page != nullptr);
    return current_leaf_page->GetItem(index_in_leaf_page);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++() {

    if (current_leaf_page == nullptr) return *this;

    // increase the index
    index_in_leaf_page++;

    // increase the leaf page
    if (index_in_leaf_page == current_leaf_page->GetSize()) {
        const page_id_t next_page_id = current_leaf_page->GetNextPageId();
        index_in_leaf_page = 0;
        buffer_pool_manager->UnpinPage(current_leaf_page->GetPageId(), false);

        if (next_page_id == INVALID_PAGE_ID) current_leaf_page = nullptr;
        else current_leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(buffer_pool_manager->FetchPage(next_page_id)->GetData());
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
