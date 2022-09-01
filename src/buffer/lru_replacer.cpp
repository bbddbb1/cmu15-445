//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  this->max_pages_ = num_pages;
  this->lru_list_ = std::list<frame_id_t>{};
  this->lru_map_ = std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator>{};
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  this->latch_.lock();
  if (this->lru_map_.empty()) {
    this->latch_.unlock();
    return false;
  }
  *frame_id = this->lru_list_.back();
  this->lru_list_.pop_back();
  this->lru_map_.erase(*frame_id);
  this->latch_.unlock();
  return true;
}
void LRUReplacer::Pin(frame_id_t frame_id) {
  this->latch_.lock();
  if (this->lru_map_.count(frame_id) != 0) {
    this->lru_list_.erase(this->lru_map_[frame_id]);
    this->lru_map_.erase(frame_id);
  }
  this->latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  this->latch_.lock();
  if (this->lru_map_.count(frame_id) != 0) {
    this->latch_.unlock();
    return;
  }
  while (this->lru_list_.size() >= this->max_pages_) {
    this->lru_map_.erase(this->lru_list_.back());
    this->lru_list_.pop_back();
  }

  this->lru_list_.push_front(frame_id);
  this->lru_map_[frame_id] = this->lru_list_.begin();
  this->latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return this->lru_list_.size(); }

}  // namespace bustub
