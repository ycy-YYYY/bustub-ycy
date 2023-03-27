//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) { is_end_ = false; }

  /**
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept
      : key_char_(other_trie_node.key_char_), is_end_(other_trie_node.is_end_) {
    for (auto &pair : other_trie_node.children_) {
      children_.emplace(pair.first, std::move(pair.second));
    }
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  auto HasChild(char key_char) const -> bool { return children_.count(key_char) != 0; }

  /**
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  auto HasChildren() const -> bool { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  auto IsEndNode() const -> bool { return is_end_; }

  /**
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  auto GetKeyChar() const -> char { return key_char_; }

  /**
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  auto InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) -> std::unique_ptr<TrieNode> * {
    // 1) if children already contains key_char
    if (children_.count(key_char) != 0U) {
      return nullptr;
    }
    // 2) if key_char is different than unique_ptr's key_char
    if (key_char != child->key_char_) {
      return nullptr;
    }
    children_.emplace(key_char, std::move(child));
    return &children_.find(key_char)->second;
  }

  /**
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  auto GetChildNode(char key_char) -> std::unique_ptr<TrieNode> * {
    auto it = children_.find(key_char);
    if (it == children_.end()) {
      return nullptr;
    }
    return &it->second;
  }

  /**
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    auto it = children_.find(key_char);
    if (it == children_.end()) {
      return;
    }
    // Free the child node
    it->second.reset();
    children_.erase(it);
  }

  /**
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { this->is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)), value_(value) { is_end_ = true; }

  /**
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) { is_end_ = true; }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  auto GetValue() const -> T { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

  /**
   *
   * @brief Helper method for insert key-value pair into th
   * @param index Current index
   * @param parent Pointer points to parent node
   * @return True if insertion succeeds, false if key already exists
   */
  template <typename T>
  auto Insert(const std::string &key, T value, size_t index, std::unique_ptr<TrieNode> *cur) -> bool {
    auto child = (*cur)->GetChildNode(key[index]);
    // If reach the end
    if (index == key.size() - 1) {
      // Contains that child
      if (child) {
        // The key already exist
        if ((*child)->IsEndNode()) {
          return false;
        }
        // The key do not exist, transform TrieNode to TrieNode with value
        (*child) = std::make_unique<TrieNodeWithValue<T>>(std::move(**child), value);
        return true;
      }
      // Do not contains that child, create a new end child
      (*cur)->InsertChildNode(key[index], std::make_unique<TrieNodeWithValue<T>>(key[index], value));
      return true;
    }

    // If do not have that child, create a new node
    if (!child) {
      (*cur)->InsertChildNode(key[index], std::make_unique<TrieNode>(key[index]));
      child = (*cur)->GetChildNode(key[index]);
    }
    auto res = Insert(key, value, index + 1, child);
    return res;
  }

  auto Remove(const std::string &key, size_t index, std::unique_ptr<TrieNode> *cur) -> bool {
    if (index >= key.size()) {
      return false;
    }
    auto child = (*cur)->GetChildNode(key[index]);
    // First check if cur node have that child
    if (child == nullptr) {
      return false;
    }
    // If reach trie 's end
    if (index == key.size() - 1) {
      if (!(*child)->IsEndNode()) {
        return false;
      }
      // Remove the end node
      // Set isend -> false
      (*child)->SetEndNode(false);
      // Transform TrieNodeWithValue to TrieNode
      (*child) = std::make_unique<TrieNode>(std::move(**child));
      if (!(*child)->HasChildren()) {
        (*cur)->RemoveChildNode(key[index]);
      }
      return true;
    }
    auto res = Remove(key, index + 1, child);
    // If successfully deleted, check if child has no children and child is not end node
    if (res) {
      if (!(*child)->HasChildren() && !(*child)->IsEndNode()) {
        (*cur)->RemoveChildNode(key[index]);
      }
    }
    return res;
  }

 public:
  /**
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() : root_(std::make_unique<TrieNode>('\0')) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  auto Insert(const std::string &key, T value) -> bool {
    latch_.RLock();
    if (key.empty()) {
      latch_.RUnlock();
      return false;
    }
    latch_.RUnlock();
    latch_.WLock();
    auto res = Insert<T>(key, value, 0, &root_);
    latch_.WUnlock();
    return res;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  auto Remove(const std::string &key) -> bool {
    latch_.WLock();
    if (key.empty()) {
      latch_.WUnlock();
      return false;
    }
    auto res = Remove(key, 0, &root_);
    latch_.WUnlock();
    return res;
  }

  /**
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  auto GetValue(const std::string &key, bool *success) -> T {
    latch_.RLock();
    // 1) If key is empty
    *success = false;
    if (key.empty()) {
      latch_.RUnlock();
      return {};
    }
    size_t index = 0;
    auto cur_ptr = &root_;
    while (index < key.size()) {
      auto child = (*cur_ptr)->GetChildNode(key[index]);
      if (child == nullptr) {
        break;
      }
      cur_ptr = child;
      index++;
    }
    // 2) If trie do not contains key
    if (index != key.size()) {
      latch_.RUnlock();
      return {};
    }
    // 3) If node is not an end node (been removed)
    if (!(*cur_ptr)->IsEndNode()) {
      latch_.RUnlock();
      return {};
    }
    // 4) If node hold  not type T value
    auto ptr = dynamic_cast<TrieNodeWithValue<T> *>((*cur_ptr).get());
    if (ptr == nullptr) {
      latch_.RUnlock();
      return {};
    }
    // Returm the value
    *success = true;
    latch_.RUnlock();
    return ptr->GetValue();
  }
};
}  // namespace bustub
