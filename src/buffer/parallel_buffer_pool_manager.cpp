//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size), starting_index_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  this->instances_ = new BufferPoolManager *[num_instances];
  for (size_t i = 0; i < num_instances; i++) {
    this->instances_[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete this->instances_[i];
  }
  delete[] this->instances_;
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t instance_id = page_id % this->num_instances_;
  return this->instances_[instance_id];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *instance = this->GetBufferPoolManager(page_id);
  return instance->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *instance = this->GetBufferPoolManager(page_id);
  return instance->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *instance = this->GetBufferPoolManager(page_id);
  return instance->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> guard(this->latch_);
  for (size_t i = 0; i < this->num_instances_; i++) {
    BufferPoolManager *instance = this->GetBufferPoolManager(this->starting_index_);
    Page *page = instance->NewPage(page_id);
    this->starting_index_ = (this->starting_index_ + 1) % this->num_instances_;
    if (page != nullptr) {
      // LOG_DEBUG("create new page id %d in instance id %d", *page_id, this->starting_index_ - 1);
      return page;
    }
  }
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *instance = this->GetBufferPoolManager(page_id);
  if (instance == nullptr) {
    return true;
  }

  return instance->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < this->num_instances_; i++) {
    BufferPoolManager *instance = this->GetBufferPoolManager(i);
    instance->FlushAllPages();
  }
}

}  // namespace bustub
