/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <string>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
  // Remove from empty table
  EXPECT_FALSE(table->Remove(1));
  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  EXPECT_EQ(3, table->GetNumBuckets());
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
  // Find the key after it's been removed
  EXPECT_FALSE(table->Find(8, result));
  EXPECT_EQ(result, std::string());
  EXPECT_FALSE(table->Remove(8));
  EXPECT_FALSE(table->Find(20, result));
  EXPECT_EQ(result, std::string());
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 5;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      if ((tid / 2) != 0) {
        threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
      } else {
        threads.emplace_back([tid, &table]() {
          table->Insert(tid, tid);
          table->Remove(tid);
        });
      }
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    // EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      if (i / 2 != 0) {
        EXPECT_TRUE(table->Find(i, val));
        EXPECT_EQ(i, val);
      } else {
        EXPECT_FALSE(table->Find(i, val));
      }
    }
  }
}

}  // namespace bustub
