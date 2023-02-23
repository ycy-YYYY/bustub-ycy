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
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
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
  if (index < 0 || index >= GetSize()) {
    return {};
  }
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comparator) -> int {
  int insert_index = 0;
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = (right - left) / 2 + left;
    if (comparator(KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  insert_index = left;
  return insert_index;
}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return {};
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetChildPageId(const KeyType &key, KeyComparator comparator) -> page_id_t {
  int i = 1;
  for (; i < GetSize(); i++) {
    auto temp = array_[i];
    if (comparator(temp.first, key) > 0) {
      break;
    }
  }
  return array_[i - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, page_id_t page_id, KeyComparator comparator) -> bool {
  if (GetSize() == GetMaxSize()) {
    return false;
  }
  array_[GetSize()] = std::make_pair(key, page_id);
  IncreaseSize(1);
  std::sort(array_ + 1, array_ + GetSize(),
            [comparator](auto p1, auto p2) { return comparator(p1.first, p2.first) < 0; });
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *other) -> KeyType {
  size_t size = GetSize() - GetMinSize();
  std::copy(array_ + GetMinSize(), array_ + GetSize(), other->array_);
  SetSize(GetMinSize());
  other->SetSize(size);
  return other->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetItem(int index, const KeyType &key, const ValueType &value) {
  assert(index >= 0 && index < GetSize());
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstHalf(const std::vector<MappingType> &tempArray) {
  std::for_each(array_, array_ + GetMaxSize(), [](auto &p) { p = {}; });
  std::copy(tempArray.begin(), tempArray.begin() + GetMinSize(), array_);
  SetSize(GetMinSize());
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastHalf(const std::vector<MappingType> &tempArray) {
  std::copy(tempArray.begin() + GetMinSize(), tempArray.end() + GetMinSize(), array_);
  SetSize(tempArray.end() - (tempArray.begin() + GetMinSize()));
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
