// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <time.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "base/common.h"
#include "cache/addr_cache.h"
#include "connection/meta_manager.h"
#include "connection/qp_manager.h"
#include "dtx/doorbell.h"
#include "dtx/structs.h"
#include "util/debug.h"
#include "util/hash.h"
#include "util/json_config.h"

/* One-sided RDMA-enabled distributed transaction processing */
class DTX {
 public:
  /************ Interfaces for applications ************/
  void TxBegin(tx_id_t txid);

  void AddToReadOnlySet(DataItemPtr item);

  void AddToReadWriteSet(DataItemPtr item);

  bool TxExe(coro_yield_t& yield, bool fail_abort = true);

  bool TxCommit(coro_yield_t& yield);
  long long get_clock_sys_time_us();

  /*****************************************************/

 public:
  void TxAbortReadOnly();

  void TxAbortReadWrite();

  void RemoveLastROItem();

 public:
  DTX(MetaManager* meta_man, QPManager* qp_man, t_id_t tid, coro_id_t coroid,
      CoroutineScheduler* sched, RDMABufferAllocator* rdma_buffer_allocator,
      LogOffsetAllocator* log_offset_allocator, int lease, AddrCache* addr_buf);
  ~DTX() { Clean(); }

 public:
  // size_t GetAddrCacheSize() { return addr_cache->TotalAddrSize(); }

 private:
  bool Validate(coro_yield_t& yield);  // RDMA read value versions
  bool RWLock(coro_yield_t& yield);
  bool Drtm(coro_yield_t& yield);

  bool DrtmCheckCas(coro_yield_t& yield);

  bool Dlmr(coro_yield_t& yield);
  bool DlmrCheck(coro_yield_t& yield);

  bool ExeRW(coro_yield_t& yield);

  bool cas_lease_expired(uint64_t lease);

  //////////// check
  bool CheckReadRO(coro_yield_t& yield);
  bool CheckCASRO(std::vector<CasRead>& pending_cas_ro,
                  std::vector<HashRead>& pending_hash_ro,
                  std::list<HashRead>& pending_next_hash_ro,
                  coro_yield_t& yield);
  bool CheckCASRead(std::vector<CasRead>& pending_cas_ro);
  bool CheckHashRO(std::vector<HashRead>& pending_hash_ro,
                   std::list<HashRead>& pending_next_hash_ro);
  bool CheckDirectRO(std::vector<DirectRead>& pending_direct_ro);
  bool CheckNextHashRO(std::list<HashRead>& pending_next_hash_ro);
  bool CheckValidate(std::vector<ValidateRead>& pending_validate);

  // bool CoalescentCommit(coro_yield_t& yield);

  void Abort();

  bool RDMAWriteRoundTrip(RCQP* qp, char* wt_data, uint64_t remote_offset,
                          size_t size);  // RDMA write wrapper

  bool RDMAReadRoundTrip(RCQP* qp, char* rd_data, uint64_t remote_offset,
                         size_t size);  // RDMA read wrapper

  void ParallelUndoLog();

  void Clean();  // Clean data sets after commit/abort

 public:
  tx_id_t tx_id;  // Transaction ID

  t_id_t t_id;  // Thread ID

  coro_id_t coro_id;  // Coroutine ID

  // For statistics
  std::vector<uint64_t> lock_durations;  // us
  long long start_time;

  std::vector<uint64_t> invisible_durations;  // us

  std::vector<uint64_t> invisible_reread;  // times

  size_t hit_local_cache_times;

  size_t miss_local_cache_times;

  MetaManager* global_meta_man;  // Global metadata manager
  int rw_ratio;

 private:
  int lease;
  CoroutineScheduler* coro_sched;  // Thread local coroutine scheduler

  QPManager* thread_qp_man;  // Thread local qp connection manager. Each
                             // transaction thread has one

  RDMABufferAllocator*
      thread_rdma_buffer_alloc;  // Thread local RDMA buffer allocator

  LogOffsetAllocator* thread_remote_log_offset_alloc;  // Thread local remote
                                                       // log offset generator

  TXStatus tx_status;

  std::vector<DataSetItem> read_only_set;

  std::vector<DataSetItem> read_write_set;

  std::vector<size_t> locked_rw_set;  // For release lock during abort

  AddrCache* addr_cache;
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  std::list<HashRead> pending_next_hash_ro;
  std::vector<CasRead>& pending_cas_ro;

  // For validate the version for insertion
  // std::vector<OldVersionForInsert> old_version_for_insert;

  struct pair_hash {
    inline std::size_t operator()(
        const std::pair<node_id_t, offset_t>& v) const {
      return v.first * 31 + v.second;
    }
  };

  // Avoid inserting to the same slot in one transaction
  std::unordered_set<std::pair<node_id_t, offset_t>, pair_hash> inserted_pos;
};

/*************************************************************
 ************************************************************
 *********** Implementations of interfaces in DTX ***********
 ************************************************************
 **************************************************************/

ALWAYS_INLINE
void DTX::TxBegin(tx_id_t txid) {
  Clean();  // Clean the last transaction states
  tx_id = txid;
  pending_direct_ro.clear();
  pending_hash_ro.clear();

  pending_next_hash_ro.clear();
  pending_cas_ro.clear();
  start_time = 0;
}

ALWAYS_INLINE
void DTX::AddToReadOnlySet(DataItemPtr item) {
  DataSetItem data_set_item{.item_ptr = std::move(item),
                            .is_fetched = false,
                            .is_logged = false,
                            .read_which_node = 0,
                            .bkt_idx = -1};
  read_only_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
long long DTX::get_clock_sys_time_us() {
  struct timespec tp;

  clock_gettime(CLOCK_MONOTONIC, &tp);
  return (long long)(tp.tv_sec * 1000000 + tp.tv_nsec / 1000);
}

ALWAYS_INLINE
void DTX::AddToReadWriteSet(DataItemPtr item) {
  DataSetItem data_set_item{.item_ptr = std::move(item),
                            .is_fetched = false,
                            .is_logged = false,
                            .read_which_node = -1,
                            .bkt_idx = -1};
  read_write_set.emplace_back(data_set_item);
}

ALWAYS_INLINE
void DTX::TxAbortReadOnly() {
  // Application actively aborts the tx
  // User abort tx in the middle time of tx exe
  assert(read_write_set.empty());
  read_only_set.clear();
}

ALWAYS_INLINE
void DTX::TxAbortReadWrite() { Abort(); }

ALWAYS_INLINE
void DTX::RemoveLastROItem() { read_only_set.pop_back(); }

// RDMA write `wt_data' with size `size' to remote
ALWAYS_INLINE
bool DTX::RDMAWriteRoundTrip(RCQP* qp, char* wt_data, uint64_t remote_offset,
                             size_t size) {
  // ***ONLY FOR DEBUG***
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, 0);
  if (rc != SUCC) {
    TLOG(ERROR, t_id) << "client: post write fail. rc=" << rc;
    return false;
  }
  // wait finish
  sleep(1);
  // ibv_wc wc{};
  // rc = qp->poll_till_completion(wc, no_timeout);
  // if (rc != SUCC) {
  //   TLOG(ERROR, t_id) << "client: poll write fail. rc=" << rc;
  // }
  return true;
}

// RDMA read value with size `size` from remote mr at offset `remote_offset` to
// rd_data
ALWAYS_INLINE
bool DTX::RDMAReadRoundTrip(RCQP* qp, char* rd_data, uint64_t remote_offset,
                            size_t size) {
  // ***ONLY FOR DEBUG***
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, 0);
  if (rc != SUCC) {
    TLOG(ERROR, t_id) << "client: post read fail. rc=" << rc;
    return false;
  }
  // wait finish
  usleep(20);
  // ibv_wc wc{};
  // rc = qp->poll_till_completion(wc, no_timeout);
  // // then get the results, stored in the local_buffer
  // if (rc != SUCC) {
  //   TLOG(ERROR, t_id) << "client: poll read fail. rc=" << rc;
  //   return false;
  // }
  return true;
}

ALWAYS_INLINE
void DTX::Clean() {
  read_only_set.clear();
  read_write_set.clear();
  // not_eager_locked_rw_set.clear();
  locked_rw_set.clear();
  // old_version_for_insert.clear();
  inserted_pos.clear();
}
