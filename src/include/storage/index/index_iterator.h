//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/index/index.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator() = default;
  IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager);

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return leaf_ == itr.leaf_ && index_ == itr.index_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return !((*this) == itr); }

 private:
  // add your own private member variables here
  Page *page_;  // Point to current page
  LeafPage *leaf_;
  int index_;  // Specify current position in the leaf page
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
