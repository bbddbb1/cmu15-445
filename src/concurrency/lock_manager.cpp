//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"
namespace bustub {

auto LockManager::GetLock(Transaction *txn, LockRequestQueue &lock_request_queue, LockMode mode) -> bool {
  auto &request_queue = lock_request_queue.request_queue_;
  txn_id_t txn_id = txn->GetTransactionId();
  if (request_queue.front().txn_id_ == txn_id) {
    return true;
  }
  bool abort = false;
  bool success = true;
  for (auto iter = request_queue.begin(); iter->txn_id_ != txn_id; ++iter) {
    if ((mode == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::EXCLUSIVE)) {
      if(iter->txn_id_ < txn_id){
        auto *young = TransactionManager::GetTransaction(iter->txn_id_);
        if (young->GetState() != TransactionState::ABORTED) {
          young->SetState(TransactionState::ABORTED);
          abort = true;
        }
      }else{
        success = false;
      }
    }
  }
  if (abort) {
    lock_request_queue.cv_.notify_all();
  }
  return success;
}

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED || txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> u_latch(latch_);
  auto &queue = lock_table_[rid].request_queue_;
  queue.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  while (!GetLock(txn, lock_table_[rid], LockMode::SHARED)) {
    lock_table_[rid].cv_.wait(u_latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }
  for (auto &iter : queue) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      break;
    }
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> u_latch(latch_);
  auto &queue = lock_table_[rid].request_queue_;
  queue.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  while (!GetLock(txn, lock_table_[rid], LockMode::EXCLUSIVE)) {
    lock_table_[rid].cv_.wait(u_latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  for (auto &iter : queue) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      break;
    }
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsSharedLocked(rid)) {
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> u_latch(latch_);
  auto &queue = lock_table_[rid].request_queue_;
  //another transaction is already waiting to upgrade their lock
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID)
  {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  while(!GetLock(txn, lock_table_[rid], LockMode::EXCLUSIVE)){
    lock_table_[rid].cv_.wait(u_latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }    
  }

  for (auto &iter : queue) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      iter.granted_ = true;
      iter.lock_mode_ = LockMode::EXCLUSIVE;
      break;
    }
  }
  lock_table_[rid].upgrading_ = INVALID_TXN_ID;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  if(!txn->IsExclusiveLocked(rid) && !txn->IsSharedLocked(rid)){
    return false;
  }

  if(txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> u_latch(latch_);
  auto &queue = lock_table_[rid].request_queue_;
  for (auto iter = queue.begin(); iter != queue.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      queue.erase(iter);
      lock_table_[rid].cv_.notify_all();
      break;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
