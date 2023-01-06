//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/types.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

#include "type/value.h"

#include "common/logger.h"

namespace bustub {
using std::pair;

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(1), bucket_size_(bucket_size), num_buckets_(2) {
  LOG_INFO("HashTable Initialized, global_depth:%u , bucket_size:%zu , num of buckets:%u", global_depth_, bucket_size,
           num_buckets_);
  for (size_t i = 0; i < 2; i++) {
    dir_.emplace_back(std::make_shared<Bucket>(bucket_size, global_depth_));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << GetGlobalDepth()) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  auto index = IndexOf(key);
  if (!dir_[index]) {
    value = {};
    return false;
  }
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t index = IndexOf(key);
  if (!dir_[index]) {
    return false;
  }
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // First find the entry index
  size_t index = IndexOf(key);
  // If the bucket do not exist, initialize it and re-insert it
  if (!dir_[index]) {
    IncrementNumberOfBuckets();
    dir_[index] = std::make_shared<Bucket>(bucket_size_, GetGlobalDepth());
    bool success = dir_[index]->Insert(key, value);
    if (success) {
      LOG_INFO("successfully insert key - value pair into bucket :%zu", index);
    }
    return;
  }
  // Try to insert it
  bool success = dir_[index]->Insert(key, value);
  if (success) {
    LOG_INFO("successfully insert key - value pair into bucket :%zu", index);
  }
  if (!success) {
    // Check whether the bucket is full
    if (dir_[index]->IsFull()) {
      // If global depth == local depth
      if (GetGlobalDepth() == dir_[index]->GetDepth()) {
        IncrementGlobalDepth();
      }
      // Split the current bucket and redistribute the pointer
      auto temp_ptr = dir_[index];
      temp_ptr->IncrementDepth();
      LOG_INFO("Redistribute bucket :%zu", index);
      RedistributeBucket(temp_ptr);
      // Re-insert the key-value pair
      Insert(key, value);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // Create a new bucket
  size_t bucket_depth = bucket->GetDepth();
  auto temp_ptr = std::make_shared<Bucket>(bucket_size_, bucket_depth);
  for (size_t i = 0; i < dir_.size(); ++i) {
    if (dir_[i] == bucket) {
      if (((i >> (bucket_depth-1)) & 1) != 0U) {
        dir_[i] = temp_ptr;
      }
    }
  } 
  // Get the original list
  auto &list = bucket->GetItems();
  // Store in temp list
  std::list<pair<K, V>> temp = list;
  for (auto it = list.begin(); it != list.end();) {
    it = list.erase(it);
  }
  for (auto &[key, value] : temp) {
    Insert(key, value);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncrementGlobalDepth() -> void {
  std::scoped_lock<std::mutex> lock(latch_);
  dir_.resize(2 * dir_.size());
  // Re-arrange the dir_ pointer
  for (size_t i = 0; i < dir_.size() / 2; ++i) {
    size_t increased_index = (1 << global_depth_) + i;
    dir_[increased_index] = dir_[i];
  }
  ++global_depth_;

  LOG_INFO("Increased global_depth , global_depth = %d", global_depth_);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncrementNumberOfBuckets() -> void {
  std::scoped_lock<std::mutex> lock(latch_);
  ++num_buckets_;
  LOG_INFO("Increased buckets num , now buckets num = %d", num_buckets_);
}



//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  bool finded = false;
  latch_.RLock();
  for (auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      finded = true;
      break;
    }
  }
  latch_.RUnlock();
  return finded;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  bool finded = false;
  latch_.WLock();
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      list_.erase(it);
      finded = true;
      break;
    }
    it++;
  }
  latch_.WUnlock();
  // The given key do not exsit, return false
  return finded;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 1) If it is full
  if (IsFull()) {
    return false;
  }
  latch_.WLock();
  for (auto &pair : list_) {
    if (pair.first == key && pair.second != value) {
      pair.second = value;
      latch_.WUnlock();
      return true;
    }
  }
  list_.emplace_back(key, value);
  latch_.WUnlock();
  return true;
}



template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
