//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iterator>
#include <sstream>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetLSN();
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index < 0 || index > GetSize()) {
    return {};
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, ValueType &value, KeyComparator comparator) -> bool {
  auto it =
      std::find_if(array_, array_ + GetSize(), [&key, comparator](auto p) { return comparator(p.first, key) == 0; });
  if (it == array_ + GetSize()) {
    return false;
  }
  value = it->second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> const MappingType & {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Index out of bound");
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator) -> bool {
  int left = 0;
  int right = GetSize();
  while (left < right) {
    int mid = (right + left) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      return false;
    }
    if (comparator(array_[mid].first, key) < 0) {
      left = mid + 1;
    }
    if (comparator(array_[mid].first, key) > 0) {
      right = mid;
    }
  }
  int index = left;

  // Try to insert to the end and sort it
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *other) -> KeyType {
  int min_size = GetMaxSize() / 2;
  size_t size = GetMaxSize() - min_size;
  std::copy(array_ + min_size, array_ + GetMaxSize(), other->array_);

  SetSize(min_size);
  other->SetSize(size);
  return other->array_[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comparator) -> int {
  auto it =
      std::find_if(array_, array_ + GetSize(), [&key, &comparator](auto &p) { return comparator(key, p.first) == 0; });
  BUSTUB_ASSERT(it != array_ + GetSize(), "Iterator error");
  return std::distance(array_, it);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FitIn(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *other) -> void {
  std::move(std::begin(other->array_), other->array_ + other->GetSize(), array_ + GetSize());
  IncreaseSize(other->GetSize());
  next_page_id_ = other->next_page_id_;
  assert(GetSize() < GetMaxSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) -> bool {
  int index = -1;
  int left = 0;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      index = mid;
      break;
    }
    if (comparator(array_[mid].first, key) < 0) {
      left = mid + 1;
    }
    if (comparator(array_[mid].first, key) > 0) {
      right = mid;
    }
  }
  if (index < 0) {
    return false;
  }

  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = std::move(array_[i + 1]);
  }
  SetSize(GetSize() - 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Redestribute(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *other) -> void {
  if (GetSize() < other->GetSize()) {
    int left = GetSize();
    int right = 0;
    int minsize = GetMinSize();
    while (other->GetSize() != minsize) {
      array_[left++] = std::move(other->array_[right++]);
      IncreaseSize(1);
    }
    std::move(other->array_ + right, other->array_ + other->GetSize(), other->array_);
    other->SetSize(other->GetSize() - right);
  } else {
    int len = GetSize() - GetMinSize();
    std::move_backward(other->array_, other->array_ + other->GetSize(), other->array_ + len);
    std::move(array_ + GetMinSize(), array_ + GetSize(), array_);
    SetSize(GetSize() - len);
    other->IncreaseSize(len);
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
