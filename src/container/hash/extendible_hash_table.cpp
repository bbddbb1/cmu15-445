//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  dir_page->SetPageId(directory_page_id_);
  page_id_t first_bucket_page_id;
  buffer_pool_manager_->NewPage(&first_bucket_page_id);
  dir_page->SetBucketPageId(0, first_bucket_page_id);
  buffer_pool_manager_->UnpinPage(first_bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  // LOG_INFO("%d", KeyToDirectoryIndex(key, dir_page));
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  reinterpret_cast<Page *>(dir_page)->RLatch();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool flag = bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  table_latch_.RUnlock();
  reinterpret_cast<Page *>(dir_page)->RUnlatch();
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  reinterpret_cast<Page *>(dir_page)->RLatch();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  // LOG_INFO("insert to bucket %d page %d", KeyToDirectoryIndex(key, dir_page), bucket_page_id);
  // bucket full
  if (bucket_page->IsFull()) {
    LOG_INFO("bucket %d is full try to split", KeyToDirectoryIndex(key, dir_page));
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    reinterpret_cast<Page *>(dir_page)->RUnlatch();
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    return SplitInsert(transaction, key, value);
  }
  if (!bucket_page->Insert(key, value, comparator_)) {
    LOG_INFO("same key value pair");
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    reinterpret_cast<Page *>(dir_page)->RUnlatch();
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.RUnlock();
    return false;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.RUnlock();
  reinterpret_cast<Page *>(dir_page)->RUnlatch();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  reinterpret_cast<Page *>(dir_page)->WLatch();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t global_depth = dir_page->GetGlobalDepth();
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);
  // dir_page->PrintDirectory();
  if (local_depth == global_depth) {
    dir_page->IncrGlobalDepth();
    // redir bucketpageid
    LOG_INFO("increse global depth now %d", global_depth);
    for (size_t i = 0; i < dir_page->Size() / 2; i++) {
      // LOG_INFO("set bucket %d to page %d local depth %d", dir_page->GetImageIndex(i), dir_page->GetBucketPageId(i),
      //          dir_page->GetLocalDepth(i));
      dir_page->SetBucketPageId(dir_page->GetImageIndex(i), dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(dir_page->GetImageIndex(i), dir_page->GetLocalDepth(i));
    }
  }
  page_id_t new_page_id;
  uint32_t new_bucket_id = dir_page->GetImageIndex(bucket_index);
  Page *new_pg = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_pg == nullptr) {
    LOG_ERROR("buffer pool overflow");
    // buffer_pool_manager_->UnpinPage(new_page_id, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    table_latch_.WUnlock();
    return false;
  }
  // split bucket
  LOG_INFO("set new bucket id %d to page %d", new_bucket_id, new_page_id);
  dir_page->SetBucketPageId(new_bucket_id, new_page_id);
  dir_page->IncrLocalDepth(bucket_index);
  dir_page->IncrLocalDepth(new_bucket_id);
  // bool flag;
  // dir_page->PrintDirectory();
  //  split items
  HASH_TABLE_BUCKET_TYPE *page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(page)->WLatch();
  HASH_TABLE_BUCKET_TYPE *new_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_pg->GetData());
  new_pg->WLatch();
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    ValueType value1 = page->ValueAt(i);
    KeyType key1 = page->KeyAt(i);
    uint32_t id = KeyToDirectoryIndex(key1, dir_page);
    if (id == new_bucket_id) {
      page->RemoveAt(i);
      new_page->Insert(key1, value1, comparator_);
    }
  }
  // LOG_INFO("try to reinsert");
  // new_page->PrintBucket();
  // page->PrintBucket();
  page_id_t id = KeyToPageId(key, dir_page);
  if (id == new_page_id) {
    new_page->Insert(key, value, comparator_);
  } else if (id == bucket_page_id) {
    page->Insert(key, value, comparator_);
  } else {
    table_latch_.WUnlock();
    LOG_ERROR("error split");
    return false;
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  table_latch_.WUnlock();
  reinterpret_cast<Page *>(dir_page)->WUnlatch();
  new_pg->WUnlatch();
  reinterpret_cast<Page *>(page)->WUnlatch();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  reinterpret_cast<Page *>(dir_page)->RLatch();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  if (!bucket_page->Remove(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    reinterpret_cast<Page *>(dir_page)->RUnlatch();
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    return false;
  }
  if (bucket_page->IsEmpty()) {
    LOG_INFO("bucket %d is empty try to merge", KeyToDirectoryIndex(key, dir_page));
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    table_latch_.RUnlock();
    reinterpret_cast<Page *>(dir_page)->RUnlatch();
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    Merge(transaction, key, value);
    return true;
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.RUnlock();
  reinterpret_cast<Page *>(dir_page)->RUnlatch();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  reinterpret_cast<Page *>(dir_page)->WLatch();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t image_bucket_id = dir_page->GetImageIndex(bucket_id);
  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_id);
  dir_page->PrintDirectory();
  if (!dir_page->DoMerge(bucket_id, image_bucket_id)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(image_bucket_page_id, false);
    table_latch_.WUnlock();
    reinterpret_cast<Page *>(dir_page)->WUnlatch();
    return;
  }
  LOG_DEBUG("successful merge bucket %d to image %d", bucket_id, image_bucket_id);
  dir_page->PrintDirectory();
  // shrink
SHRINK:
  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
    LOG_INFO("shrink to global depth %d", dir_page->GetGlobalDepth());
    for (size_t i = 0; i < dir_page->Size(); i++) {
      page_id_t page_id = dir_page->GetBucketPageId(i);
      HASH_TABLE_BUCKET_TYPE *page = FetchBucketPage(page_id);
      reinterpret_cast<Page *>(page)->RLatch();
      if (page->IsEmpty()) {
        dir_page->DoMerge(i, dir_page->GetImageIndex(i));
      }
      buffer_pool_manager_->UnpinPage(page_id, false);
      reinterpret_cast<Page *>(page)->RUnlatch();
    }
    dir_page->PrintDirectory();
    goto SHRINK;
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
  reinterpret_cast<Page *>(dir_page)->WUnlatch();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
