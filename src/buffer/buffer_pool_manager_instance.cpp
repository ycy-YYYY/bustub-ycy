//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <algorithm>
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/table_page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  // 1) Find the free frame
  frame_id_t frame_id = 0;
  auto success = FindFreeFrame(frame_id);
  if (!success) {
    return nullptr;
  }
  // 2) Allocate new page
  *page_id = AllocatePage();

  // 3) Create a new page on the frame
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  // 4) Record Access the page, and pin the page
  AccessPage(frame_id);
  // 5) Update Map in page_table
  page_table_->Insert(*page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1) First  fetch from the buffer pool
  frame_id_t frame_id = 0;
  auto finded = page_table_->Find(page_id, frame_id);
  if (finded) {
    AccessPage(frame_id);
    return &pages_[frame_id];
  }
  // 2) Find free frame
  finded = FindFreeFrame(frame_id);
  if (!finded) {
    return nullptr;
  }
  // 3) Fetch the page from disk
  FetchPageFromDisk(page_id, frame_id);
  // 4) Record Access the page, and pin the page
  AccessPage(frame_id);
  // 5) Update Map in page_table
  page_table_->Insert(page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id = 0;
  auto success = page_table_->Find(page_id, frame_id);
  // Not in the buffer pool
  if (!success) {
    return false;
  }

  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  // !important
  // is_dirty flag only indicate that current thread had modified or not, only write page to disk can set the flag to
  // false
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  // If pin_count reach to zero, set evictable to true
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id = 0;
  auto founded = page_table_->Find(page_id, frame_id);
  if (!founded) {
    return false;
  }
  // Write page to the disk
  WritePageToDisk(frame_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    WritePageToDisk(i);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  frame_id_t frame_id = 0;
  auto founded = page_table_->Find(page_id, frame_id);
  // The page do not exist in the buffer pool
  if (!founded) {
    return true;
  }
  // Some thread are referencing the page
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }
  // reset pages memory and meta-data
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  // Delete key-value pair from hashmap
  page_table_->Remove(page_id);
  // Free the frame
  replacer_->Remove(frame_id);
  // Add to the freelist
  free_list_.emplace_front(frame_id);

  // Deallocate it from disk
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::FetchPageFromDisk(page_id_t page_id, frame_id_t frame_id) -> void {
  Page *page = &pages_[frame_id];
  // read data from disk
  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  BUSTUB_ASSERT(page->pin_count_ == 0, "The page from freelist pin count != 0");
  page->is_dirty_ = false;
}

auto BufferPoolManagerInstance::WritePageToDisk(frame_id_t frame_id) -> void {
  pages_[frame_id].WLatch();
  BUSTUB_ASSERT(pages_[frame_id].page_id_ != INVALID_PAGE_ID, "Write pages_[frame_id] to disk within illegal page_id");
  // Write data to disk
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  // Reset dirty flag
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();
}
auto BufferPoolManagerInstance::FindFreeFrame(frame_id_t &frame_id) -> bool {
  // First find free frame from freeList
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // Then try to evict from LRU-replacer
  if (replacer_->Evict(&frame_id)) {
    BUSTUB_ASSERT(pages_[frame_id].GetPageId() != INVALID_PAGE_ID, "Page Id illegal");
    // If evition success, check if the old page are dirty
    if (pages_[frame_id].IsDirty()) {
      // If dirty, write it to disk
      WritePageToDisk(frame_id);
      pages_[frame_id].is_dirty_ = false;
    }
    // Remove key-value pair in the map
    page_table_->Remove(pages_[frame_id].GetPageId());
    return true;
  }
  return false;
}

}  // namespace bustub
