/**
 * index_iterator.cpp
 */

#include "storage/index/index_iterator.h"
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), index_(index), buffer_pool_manager_(buffer_pool_manager) {}
// NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (page_id_ == 0 && index_ == 0) {
    // If defalut constructor was called
    return true;
  }

  Page *page = buffer_pool_manager_->FetchPage(page_id_);
  page->RLatch();
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  bool res = leaf->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf->GetSize();
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id_, false);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  Page *page = buffer_pool_manager_->FetchPage(page_id_);
  page->RLatch();
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  auto &item = leaf->GetItem(index_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id_, false);
  return item;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // 1) Index at range
  // 2) Index out of bound, fetch next page
  // 3) reach the end

  Page *page = buffer_pool_manager_->FetchPage(page_id_);
  page->RLatch();
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page_id_t cur_page_id = page_id_;
  if (index_ < leaf->GetSize() - 1) {
    index_++;
  } else {
    page_id_t next_page_id = leaf->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) {
      // update index to new page
      page_id_ = next_page_id;
      index_ = 0;
    } else {
      // reach to the end
      index_++;
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page_id, false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
