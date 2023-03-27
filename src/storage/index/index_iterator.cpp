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
INDEXITERATOR_TYPE::IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager)
    : page_(page), index_(index), buffer_pool_manager_(buffer_pool_manager) {
  BUSTUB_ASSERT(page != nullptr, "Page is null");
  leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ == nullptr) {
    page_ = nullptr;
    buffer_pool_manager_ = nullptr;
    return;
  }

  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);

  page_ = nullptr;
  buffer_pool_manager_ = nullptr;
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(leaf_ != nullptr, "Dereferencing end iterator");
  BUSTUB_ASSERT(index_ != leaf_->GetSize(), "Dereference end iter");
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // 1) Index at range
  // 2) Index out of bound, fetch next page
  // 3) reach the end
  BUSTUB_ASSERT(index_ <= leaf_->GetSize(), "Size out of bound");
  if (index_ < leaf_->GetSize() - 1) {
    index_++;
    return *this;
  }

  page_id_t next_page_id = leaf_->GetNextPageId();
  if (next_page_id != INVALID_PAGE_ID) {
    // Fetch next page
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    BUSTUB_ASSERT(next_page != nullptr, "Fetched a illegal page");
    next_page->RLatch();

    // release the current page
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);

    page_ = next_page;
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
    index_ = 0;
    return *this;
  }

  index_++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
