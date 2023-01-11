//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cassert>
#include <cstddef>
#include <list>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> scoped_lock(latch_);
  frame_id_t temp_id = 0;
  size_t maxtime = 0;
  for (auto id : evictable_id_) {
    auto kth_time = GetKthTime(id);
    if (kth_time > maxtime) {
      maxtime = kth_time;
      temp_id = id;
    }
  }
  if (maxtime == 0) {
    return false;
  }
  evictable_id_.extract(temp_id);
  frames_.extract(temp_id);
  *frame_id = temp_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // First check if frame_id is valid
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "Frame_id is invalid");
  // Find out if frame_id has not been seen before
  auto it = frames_.find(frame_id);
  ++current_timestamp_;
  if (it == frames_.end()) {
    frames_.emplace(frame_id, std::list<size_t>{current_timestamp_});
  } else {
    auto &timestamps = it->second;
    timestamps.emplace_front(current_timestamp_);
    if (timestamps.size() > k_) {
      timestamps.pop_back();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // First check if frame_id is valid
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "Frame_id is invalid");
  // If frame_id do not exist in frames, imediately return
  if (frames_.count(frame_id) == 0) {
    return;
  }
  // Set one frame to non-evictable
  if (evictable_id_.count(frame_id) != 0 && !set_evictable) {
    evictable_id_.extract(frame_id);
  }
  // Set one frame to evictable
  if (evictable_id_.count(frame_id) == 0 && set_evictable) {
    evictable_id_.emplace(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> scoped_lock(latch_);
  auto frames_it = frames_.find(frame_id);
  if (frames_it == frames_.end()) {
    return;
  }
  auto evictable_it = evictable_id_.find(frame_id);
  BUSTUB_ASSERT(evictable_it != evictable_id_.end(), "Remove error, the frame_id is invalid or not evictvale");
  evictable_id_.erase(evictable_it);
  frames_.erase(frames_it);
}

auto LRUKReplacer::Size() -> size_t { return evictable_id_.size(); }

// ******************************private******************************************

auto LRUKReplacer::GetKthTime(frame_id_t frame_id) -> size_t {
  auto it = frames_.find(frame_id);
  assert(it != nullptr);
  auto list = it->second;
  auto earliest = list.back();
  size_t kth_time = 0;
  // if size < 3
  if (list.size() < k_) {
    kth_time = 0x7ffffff - earliest + current_timestamp_;
  } else {
    kth_time = current_timestamp_ - earliest;
  }
  return kth_time;
}

}  // namespace bustub
