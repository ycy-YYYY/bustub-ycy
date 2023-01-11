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
#include <mutex>

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
  // 1) Check if freelist has element
  frame_id_t frame_id = 0;
  auto success = GetFrameIdFromFreeList(&frame_id);
  if (success) {
    // Allocate new page
    *page_id = AllocatePage();
    // Insert new page from disk
    auto page = FetchPageFromDisk(*page_id, frame_id);
    // Map new page id
    page_table_->Insert(*page_id, frame_id);
    // Access the pagepr
    AccessPage(frame_id, page);
    return page;
  }
  // If there are any evictable pages
  if (replacer_->Size() != 0) {
    // Allocate new page id
    *page_id = AllocatePage();
    // Evit the old page through lru-k
    success = EvictPageLRU(frame_id);
    // Fetch the new page from disk
    auto page = FetchPageFromDisk(*page_id, frame_id);
    // Insert pageid -> frameid to hashmap
    page_table_->Insert(*page_id, frame_id);
    // Access the page
    AccessPage(frame_id, page);
    return page;
  }
  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1) First  fetch from the buffer pool
  frame_id_t frame_id = 0;
  auto success = page_table_->Find(page_id, frame_id);
  if (success) {
    AccessPage(frame_id, &pages_[frame_id]);
    return &pages_[frame_id];
  }
  // 2) Find frame from free list, and fetch it from disk
  success = GetFrameIdFromFreeList(&frame_id);
  if (success) {
    auto page = FetchPageFromDisk(page_id, frame_id);
    // Insert to hashmap
    page_table_->Insert(page_id, frame_id);
    // AccessPage
    AccessPage(frame_id, page);
    return page;
  }
  // 3) Evict from lru-k
  if (replacer_->Size() > 0) {
    // Evict an old page and free the frame
    success = EvictPageLRU(frame_id);
    // Fetch the new page from the disk
    auto page = FetchPageFromDisk(page_id, frame_id);
    if (success) {
      // Insert to hashmap
      page_table_->Insert(page_id, frame_id);
      // Access the page
      AccessPage(frame_id, page);
      return page;
    }
  }

  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id = 0;
  auto success = page_table_->Find(page_id, frame_id);
  if (success) {
    auto page = &pages_[frame_id];
    page->WLatch();
    if (page->pin_count_ != 0) {
      page->is_dirty_ = is_dirty;
      // If pin_count reach to zero, set evictable to true
      if (--page->pin_count_ == 0) {
        replacer_->SetEvictable(frame_id, true);
      }
      page->RUnlatch();
      return true;
    }
    page->WUnlatch();
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id = 0;
  auto founded = page_table_->Find(page_id, frame_id);
  if (!founded) {
    return false;
  }
  // Write page to the disk
  WritePageToDisk(&pages_[frame_id]);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    WritePageToDisk(&pages_[i]);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock (latch_);
  frame_id_t frame_id = 0;
  auto founded = page_table_->Find(page_id, frame_id);
  // The page do not exist in the buffer pool
  if (!founded) {
    return true;
  }
  pages_[frame_id].WLatch();
  // Some thread are referencing the page
  if (pages_[frame_id].GetPinCount() != 0) {
    pages_[frame_id].WUnlatch();
    return false;
  }
  // reset pages memory and meta-data
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].WUnlatch();
  // Delete key-value pair from hashmap
  page_table_->Remove(page_id);
  // Free the frame
  free_list_.emplace_back(frame_id);
  // Deallocate it from disk
  DeallocatePage(page_id);
  return true;
 }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::AccessPage(frame_id_t frame_id, Page *page) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(pool_size_), "The frame_id is greater than buffer pool size");
  std::scoped_lock<std::mutex> lock(latch_);
  // Update pin count
  page->WLatch();
  ++(page->pin_count_);
  page->WUnlatch();
  // Access record
  replacer_->RecordAccess(frame_id);
  // Set un-evictable
  replacer_->SetEvictable(frame_id, false);
}


auto BufferPoolManagerInstance::EvictPageLRU(frame_id_t &frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // Get the frame id
  auto success = replacer_->Evict(&frame_id);
  if (!success) {
    return false;
  }
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(pool_size_), "The frame_id is greater than buffer pool size");
  // Remove the old page
  WritePageToDisk(&pages_[frame_id]);
  page_table_->Remove(pages_[frame_id].GetPageId());
  return true;
}

auto BufferPoolManagerInstance::GetFrameIdFromFreeList(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (free_list_.empty()) {
    return false;
  }
  *frame_id = free_list_.front();
  free_list_.pop_front();
  return true;
}
auto BufferPoolManagerInstance::FetchPageFromDisk(page_id_t page_id, frame_id_t frame_id) -> Page * {
  Page *page = &pages_[frame_id];
  // read data from disk
  page->WLatch();
  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  page->WUnlatch();
  return page;
}

auto BufferPoolManagerInstance::WritePageToDisk(Page *page) -> void {
  page->WLatch();
  BUSTUB_ASSERT(page->page_id_ != INVALID_PAGE_ID, "Write page to disk within illegal page_id");
  // Write data to disk
  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  // Reset dirty flag
  page->is_dirty_ = false;
  page->WUnlatch();
}
}  // namespace bustub
