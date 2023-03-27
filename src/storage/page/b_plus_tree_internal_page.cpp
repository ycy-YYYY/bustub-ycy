//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <sstream>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"
#include "type/value.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetLSN();
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Illegal index");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= GetSize()) {
    return;
  }
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(const KeyType &key, KeyComparator comparator) -> int {
  int insert_index = -1;
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = (right - left) / 2 + left;
    if (comparator(KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else if (comparator(array_[mid].first, key) > 0) {
      right = mid;
    } else {
      insert_index = mid;
      break;
    }
  }
  insert_index = insert_index < 0 ? left : insert_index;
  return insert_index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndex(const KeyType &key, KeyComparator comparator) -> int {
  return UpperBound(key, comparator) - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, KeyComparator comparator) -> int {
  auto it = std::upper_bound(array_ + 1, array_ + GetSize(), key,
                             [comparator](const auto &value, const auto &p) { return comparator(value, p.first) < 0; });
  return it - array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndex(const ValueType &value) -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &p) { return p.second == value; });
  int dis = std::distance(array_, it);
  BUSTUB_ASSERT(dis >= 0 && dis < GetSize(), "Index invalid");
  return dis;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Wrong index");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetChildPageId(const KeyType &key, KeyComparator comparator) -> page_id_t {
  int index = UpperBound(key, comparator) - 1;

  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Binary search err");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, page_id_t page_id, KeyComparator comparator) -> bool {
  if (GetSize() == GetMaxSize()) {
    return false;
  }
  int index = LowerBound(key, comparator);

  if (index < GetSize()) {
    std::move_backward(array_ + index, array_ + GetSize(), array_ + 1 + GetSize());
  }

  array_[index] = std::make_pair(key, page_id);
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFromPre(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *sibling,
                                                   BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                                                   int index, BufferPoolManager *buffer_pool_manager) {
  BUSTUB_ASSERT(sibling->GetSize() <= sibling->GetMaxSize(), "Page currupted");
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = std::move(sibling->array_[sibling->GetSize() - 1]);

  // Change moved child's parent id
  auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[0].second)->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  // Update parent key
  parent->SetKeyAt(index, array_[0].first);
  IncreaseSize(1);
  sibling->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BorrowFromNext(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *sibling,
                                                    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                                                    int index, BufferPoolManager *buffer_pool_manager) {
  BUSTUB_ASSERT(sibling->GetSize() <= sibling->GetMaxSize(), "Page currupted");
  array_[GetSize()] = std::move(sibling->array_[0]);
  std::move(sibling->array_ + 1, sibling->array_ + sibling->GetSize(), sibling->array_);
  // Change moved child's parent id

  auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[GetSize()].second)->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  // Update parent key
  parent->SetKeyAt(index + 1, sibling->array_[0].first);
  IncreaseSize(1);
  sibling->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeToPre(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *sibling,
                                                BufferPoolManager *buffer_pool_manager) {
  BUSTUB_ASSERT(sibling->GetSize() + GetSize() <= sibling->GetMaxSize(), "Error");
  // Move all the element to previous page
  std::move(array_, array_ + GetSize(), sibling->array_ + sibling->GetSize());

  // Update parent's link
  for (int i = sibling->GetSize(); i < sibling->GetSize() + GetSize(); i++) {
    auto *child =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(sibling->array_[i].second)->GetData());
    child->SetParentPageId(sibling->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }

  // Marked 0 size for delete page

  sibling->IncreaseSize(GetSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetItem(int index, const KeyType &key, const ValueType &value) {
  assert(index >= 0 && index < GetSize());
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstHalf(const std::vector<MappingType> &tempArray) {
  int size = (GetMaxSize() + 1) / 2;
  std::move(tempArray.begin(), tempArray.begin() + size, array_);
  SetSize(size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastHalf(const std::vector<MappingType> &tempArray,
                                                  BufferPoolManager *buffer_pool_manager) {
  int size = (GetMaxSize() + 1) / 2;
  std::move(tempArray.begin() + size, tempArray.end(), array_);
  SetSize(tempArray.end() - (tempArray.begin() + size));
  // For the new split node, we need to reset chilren node's parent id
  for (int i = 0; i < GetSize(); i++) {
    auto *child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[i].second)->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Illegal key");
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = std::move(array_[i + 1]);
  }
  array_[GetSize() - 1] = {};
  SetSize(GetSize() - 1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
