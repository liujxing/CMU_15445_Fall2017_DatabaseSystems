/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* start_leaf_page,
                BufferPoolManager* buffer_pool_manager, const int index_in_leaf_page);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
    // add your own private member variables here
    B_PLUS_TREE_LEAF_PAGE_TYPE* current_leaf_page;
    BufferPoolManager* buffer_pool_manager;
    int index_in_leaf_page = 0;


};

} // namespace cmudb
