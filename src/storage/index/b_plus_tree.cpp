#include <bits/types/FILE.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <exception>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"
#include "type/type.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  Page *page = SearchLeaf(key, comparator_);
  if (page == nullptr) {
    return false;
  }

  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value{};
  bool finded = leaf->GetValue(key, value, comparator_);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (!finded) {
    return false;
  }
  result->emplace_back(value);

  // unlock the leaf page and unpin it in the buffer pool
  // LOG_DEBUG("Page: %d Runlock", page->GetPageId());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 1) Fetch a new page from buffer pool
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  assert(root_page != nullptr);
  // 2) init the root page, insert key-value pair and update root id

  // LOG_DEBUG("Page :%d Wlock", root_page_id_);
  auto root = reinterpret_cast<LeafPage *>(root_page->GetData());
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId(1);
  root->Insert(key, value, comparator_);

  // LOG_DEBUG("Page :%d Wulock", root_page_id_);
  // 3) Unpin the page
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *page = SearchLeaf(key, comparator_, SearchType::INSERT, transaction);
  // If the current tree is empty
  if (page == nullptr) {
    StartNewTree(key, value);
    RootUnlock();
    return true;
  }

  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
  bool success = leaf->Insert(key, value, comparator_);

  if (success && leaf->GetSize() == leaf->GetMaxSize()) {
    Split(leaf, transaction);
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), success);
  UnlockPages(transaction);

  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  Page *leaf_page = SearchLeaf(key, comparator_, SearchType::DELETE, transaction);
  if (leaf_page == nullptr) {
    RootUnlock();
    return;
  }

  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page);

  bool success = leaf->Remove(key, comparator_);
  bool dirty = true;
  if (!success) {
    dirty = false;
  }
  if (leaf->GetSize() < leaf->GetMinSize()) {
    MergeOrDistribute(leaf, *transaction->GetPageSet(), transaction);
  }

  UnlockPages(transaction);
  DeletePages(transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), dirty);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeOrDistribute(LeafPage *leaf, std::deque<Page *> path, Transaction *transaction) {
  // If current page is root page
  // means the page is empty, add it to delete pageset
  if (leaf->IsRootPage()) {
    BUSTUB_ASSERT(leaf->GetSize() == 0, "Size error");
    transaction->AddIntoDeletedPageSet(leaf->GetPageId());

    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(root_page_id_);
  } else {
    BUSTUB_ASSERT(!path.empty(), "Tranction size reach 0");
    Page *parent_page = path.back();
    path.pop_back();
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // make the right side neighbour to exchange
    int index = parent->LookUp(leaf->GetPageId());
    assert(index >= 0);
    int bound_index = 0;
    Page *sibling = nullptr;
    LeafPage *left = nullptr;
    LeafPage *right = nullptr;

    if (index == parent->GetSize() - 1) {
      sibling = buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1));
      sibling->WLatch();
      left = reinterpret_cast<LeafPage *>(sibling->GetData());
      right = leaf;
      bound_index = index;
    } else {
      left = leaf;

      sibling = buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1));
      sibling->WLatch();

      right = reinterpret_cast<LeafPage *>(sibling->GetData());
      bound_index = index + 1;
    }
    assert(left != nullptr);
    assert(right != nullptr);

    // merge or redistribute
    if (left->GetSize() + right->GetSize() <= left->GetMaxSize()) {
      // merge left with right node
      KeyType key_tobe_delete = right->KeyAt(0);
      left->FitIn(right);
      // Set next pageid
      left->SetNextPageId(right->GetNextPageId());

      transaction->AddIntoDeletedPageSet(right->GetPageId());

      RemoveEntry(key_tobe_delete, parent, path, transaction);

    } else {
      // redistribute key-value pairs
      leaf->Redestribute(right);
      parent->SetKeyAt(bound_index, right->KeyAt(0));
    }
    // Flush the dirty flag of parent page

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->FetchPage(parent_page->GetPageId());
    // Release sibing page
    sibling->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(const KeyType &key, InternalPage *internal, std::deque<Page *> &path,
                                 Transaction *transaction) {
  assert(internal->GetSize() >= internal->GetMinSize());
  internal->Remove(key, comparator_);

  if (internal->GetSize() < internal->GetMinSize()) {
    if (internal->IsRootPage()) {
      assert(internal->GetSize() == 1);
      // make the only child page to be the root page
      auto *child = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(0))->GetData());

      // Update root page id
      child->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = child->GetPageId();
      UpdateRootPageId(root_page_id_);

      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      transaction->AddIntoDeletedPageSet(internal->GetPageId());

    } else {
      Page *parent_page = path.back();
      path.pop_back();
      auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
      // make the right side neighbour to exchange
      Page *sibling = nullptr;
      int index = parent->LookUp(internal->GetPageId());
      int bound_index = 0;
      InternalPage *left = nullptr;
      InternalPage *right = nullptr;
      if (index == parent->GetSize() - 1) {
        sibling = buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1));
        sibling->WLatch();
        left = reinterpret_cast<InternalPage *>(sibling->GetData());
        right = internal;
        bound_index = index;
      } else {
        left = internal;
        sibling = buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1));
        sibling->WLatch();
        right = reinterpret_cast<InternalPage *>(sibling->GetData());
        bound_index = index + 1;
      }
      assert(left != nullptr);
      assert(right != nullptr);

      // Either merge or redistribute need to set right's first key
      right->SetKeyAt(0, parent->KeyAt(bound_index));
      if (left->GetSize() + right->GetSize() <= leaf_max_size_) {
        // First set right node's first key

        left->FitIn(right, buffer_pool_manager_);

        transaction->AddIntoDeletedPageSet(right->GetPageId());

        RemoveEntry(parent->KeyAt(bound_index), parent, path, transaction);
      } else {
        left->Redistribute(right, buffer_pool_manager_);
        // Set Parent node key at bound index to be right->KeyAt(0)
        parent->SetKeyAt(bound_index, right->KeyAt(0));
      }

      sibling->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);

      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->FetchPage(parent_page->GetPageId());
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeaf(const KeyType &key, KeyComparator comparator, SearchType type, Transaction *transaction)
    -> Page * {
  RootLock();
  if (IsEmpty()) {
    return nullptr;
  }

  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->WLatch();

  assert(page != nullptr);
  while (true) {
    auto *current_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (IsSafety(current_node, type)) {
      // Check if the root still locked

      UnlockPages(transaction);
    }
    if (current_node->IsLeafPage()) {
      break;
    }
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->GetChildPageId(key, comparator);

    // Fetch and lock child page
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    child_page->WLatch();
    // Add current page to the tranction
    transaction->AddIntoPageSet(page);
    page = child_page;
  }
  // return pointer to leaf page
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeaf(const KeyType &key, KeyComparator comparator) -> Page * {
  RootLock();
  if (IsEmpty()) {
    RootUnlock();
    return nullptr;
  }
  auto page = buffer_pool_manager_->FetchPage(GetRootPageId());
  page->RLatch();
  RootUnlock();
  assert(page != nullptr);
  while (!reinterpret_cast<InternalPage *>(page->GetData())->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->GetChildPageId(key, comparator);

    // Fetch and lock child page
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    child_page->RLatch();

    assert(child_page->GetPinCount() == 1);

    // Unlock and unpin parent page
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    page = child_page;
  }
  return page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeftMostChild() -> Page * {
  RootLock();
  if (IsEmpty()) {
    RootUnlock();
    return nullptr;
  }
  auto page = buffer_pool_manager_->FetchPage(GetRootPageId());
  page->RLatch();
  RootUnlock();
  assert(page != nullptr);
  while (!reinterpret_cast<InternalPage *>(page->GetData())->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->ValueAt(0);

    // Fetch and lock child page
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    child_page->RLatch();

    assert(child_page->GetPinCount() == 1);

    // Unlock and unpin parent page
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child_page;
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  // Unlock all the pages in the transaction and if it contains root page, unlock root_lock too
  auto lock_set = transaction->GetPageSet();
  RootUnlock();
  for (auto p : *lock_set) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
  }
  lock_set->clear();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(BPlusTreePage *oldChildPage, BPlusTreePage *newChildPage, const KeyType &key,
                                    std::deque<Page *> path) -> void {
  // If parent page id is illegel, fetch a new page and update root id
  if (oldChildPage->IsRootPage()) {
    Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(root_page_id_ != 0);
    assert(root_page != nullptr);

    auto *root = reinterpret_cast<InternalPage *>(root_page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    UpdateRootPageId();

    // When ever fetch a new root page, update the links to them
    oldChildPage->SetParentPageId(root_page_id_);
    newChildPage->SetParentPageId(root_page_id_);

    // Insert two key-value pairs in to it
    root->SetSize(2);
    root->SetItem(0, {}, oldChildPage->GetPageId());
    root->SetItem(1, key, newChildPage->GetPageId());

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    // unpin root page
    return;
  }

  auto *page = reinterpret_cast<InternalPage *>(path.back());
  path.pop_back();

  if (page->GetSize() == page->GetMaxSize()) {
    int insert_index = page->LookUp(key, comparator_);
    auto temp_array = page->GetAllItem();
    temp_array.resize(temp_array.size() + 1);
    std::move_backward(temp_array.begin() + insert_index, temp_array.end() - 1, temp_array.end());
    temp_array[insert_index] = std::make_pair(key, newChildPage->GetPageId());
    page_id_t new_internal_id = 0;

    Page *new_internal_page = buffer_pool_manager_->NewPage(&new_internal_id);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    assert(new_internal != nullptr);
    new_internal->Init(new_internal_id, page->GetParentPageId(), internal_max_size_);
    page->CopyFirstHalf(temp_array);
    new_internal->CopyLastHalf(temp_array, buffer_pool_manager_);

    if (insert_index >= page->GetMinSize()) {
      newChildPage->SetParentPageId(new_internal_id);
    }

    InsertToParent(page, new_internal, new_internal->KeyAt(0), path);

    buffer_pool_manager_->UnpinPage(new_internal_id, true);
  } else {
    page->Insert(key, newChildPage->GetPageId(), comparator_);
  }

  // Flush the dirty bit
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->FetchPage(page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(LeafPage *leaf, Transaction *tranction) {
  page_id_t new_leaf_id = 0;
  auto *new_leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_leaf_id)->GetData());

  assert(new_leaf != nullptr);
  new_leaf->Init(new_leaf_id, leaf->GetParentPageId(), leaf_max_size_);
  leaf->Merge(new_leaf);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_id);

  InsertToParent(leaf, new_leaf, new_leaf->KeyAt(0), *tranction->GetPageSet());

  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafety(BPlusTreePage *page, SearchType type) -> bool {
  bool res = false;
  if (type == SearchType::INSERT) {
    if (page->IsLeafPage()) {
      res = page->GetSize() < page->GetMaxSize() - 1;
    } else {
      res = page->GetSize() < page->GetMaxSize();
    }
  }
  if (type == SearchType::DELETE) {
    if (page->GetSize() > page->GetMinSize()) {
      res = true;
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction *transaction) {
  auto deleted_set = transaction->GetDeletedPageSet();
  for (page_id_t page_id : *deleted_set) {
    buffer_pool_manager_->DeletePage(page_id);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Page *page = LeftMostChild();
  return INDEXITERATOR_TYPE(page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *page = SearchLeaf(key, comparator_);
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf->LookUp(key, comparator_);
  return INDEXITERATOR_TYPE(page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
