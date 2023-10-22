// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

DTX::DTX(MetaManager* meta_man, QPManager* qp_man, t_id_t _tid,
         coro_id_t coroid, CoroutineScheduler* sched,
         RDMABufferAllocator* rdma_buffer_allocator,
         LogOffsetAllocator* remote_log_offset_allocator, int _lease,
         AddrCache* addr_buf) {
  // Transaction setup
  tx_id = 0;
  t_id = _tid;
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  thread_qp_man = qp_man;
  lease = _lease;
  thread_rdma_buffer_alloc = rdma_buffer_allocator;
  tx_status = TXStatus::TX_INIT;

  thread_remote_log_offset_alloc = remote_log_offset_allocator;
  addr_cache = addr_buf;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;
}

bool DTX::OCC(coro_yield_t& yield) { return true; }

bool DTX::RWLock(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    item.read_which_node = remote_node_id;
    auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      pending_direct_ro.emplace_back(DirectRead{.qp = qp,
                                                .item = &item,
                                                .buf = data_buf,
                                                .remote_node = remote_node_id});
      if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
        return false;
      }
    } else {
      // hash read
      HashMeta meta =
          global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      pending_hash_ro.emplace_back(HashRead{.qp = qp,
                                            .item = &item,
                                            .buf = local_hash_node,
                                            .remote_node = remote_node_id,
                                            .meta = meta});
      if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off,
                                sizeof(HashNode))) {
        return false;
      }
    }
  }
  for (auto& item : read_write_set) {
    // cas lock
  }
  if (start_time == 0) {
    start_time = get_clock_sys_time_us();
  }
  coro_sched->Yield(yield, coro_id);
  // auto end_time = get_clock_sys_time_us();
  // auto cost = end_time - start_time;
  // if (cost > 5) {
  //   RDMA_LOG(INFO) << "rdma get time = " << cost;
  // }
  // Receive data
  std::list<HashRead> pending_next_hash_ro;
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro,
                         pending_next_hash_ro, yield);
  return res;
}

bool DTX::Drtm(coro_yield_t& yield) {
  std::vector<CasRead> pending_cas_rw;
  std::vector<HashRead> pending_hash_ro;
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      pending_cas_rw.emplace_back(CasRead{.qp = qp,
                                          .item = &item,
                                          .cas_buf = cas_buf,
                                          .data_buf = data_buf,
                                          .primary_node_id = remote_node_id});
      // test without doorbell
      auto expired_time = start_time + lease;
      auto swap = expired_time << 1;
      if (!coro_sched->RDMACAS(coro_id, qp, cas_buf,
                               it->GetRemoteLockAddr(offset), 0, swap)) {
        return false;
      }
      if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
        return false;
      }

      // test with doorbell
    } else {
      // hash read
      HashMeta meta =
          global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      pending_hash_ro.emplace_back(HashRead{.qp = qp,
                                            .item = &item,
                                            .buf = local_hash_node,
                                            .remote_node = remote_node_id,
                                            .meta = meta});
      if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off,
                                sizeof(HashNode))) {
        return false;
      }
    }
  }

  for (auto& item : read_write_set) {
    // cas lock
  }

  coro_sched->Yield(yield, coro_id);
  // Receive data
  std::list<HashRead> pending_next_hash_ro;
  auto res =
      CheckCASRO(pending_cas_rw, pending_hash_ro, pending_next_hash_ro, yield);

  return true;
}

bool DTX::Dlmr(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  std::vector<uint64_t> pending_lock;
  std::vector<HashRead> pending_hash_ro;
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      pending_direct_ro.emplace_back(DirectRead{.qp = qp,
                                                .item = &item,
                                                .buf = data_buf,
                                                .remote_node = remote_node_id});
      if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
        return false;
      }
    } else {
      // hash read
      HashMeta meta =
          global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      pending_hash_ro.emplace_back(HashRead{.qp = qp,
                                            .item = &item,
                                            .buf = local_hash_node,
                                            .remote_node = remote_node_id,
                                            .meta = meta});
      if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off,
                                sizeof(HashNode))) {
        return false;
      }
    }
  }

  for (auto& item : read_write_set) {
    // cas lock
  }

  coro_sched->Yield(yield, coro_id);
  // Receive data
  std::list<HashRead> pending_next_hash_ro;
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro,
                         pending_next_hash_ro, yield);

  return true;
}

bool DTX::Validate(coro_yield_t& yield) {
  // The transaction is read-write, and all the written data have
  // been locked before
  // validate the reads
  std::vector<ValidateRead> pending_validate;

  // For read-only items, we only need to read their versions
  for (auto& set_it : read_only_set) {
    auto it = set_it.item_ptr;
    // RDMA_LOG(INFO) << "validate key = " << it->key;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(set_it.read_which_node);
    char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
    pending_validate.push_back(ValidateRead{.qp = qp,
                                            .item = &set_it,
                                            .cas_buf = nullptr,
                                            .version_buf = version_buf,
                                            .has_lock_in_validate = false});
    if (!coro_sched->RDMARead(coro_id, qp, version_buf,
                              it->GetRemoteVersionAddr(), sizeof(version_t))) {
      return false;
    }
  }
  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

bool DTX::CheckValidate(std::vector<ValidateRead>& pending_validate) {
  for (auto& re : pending_validate) {
    auto it = re.item->item_ptr;
    // Compare version
    if (it->version != *((version_t*)re.version_buf)) {
      // RDMA_LOG(DBG) << "MY VERSION " << it->version;
      // RDMA_LOG(DBG) << "version_buf " << *((version_t*)re.version_buf);
      return false;
    }
  }
  return true;
}

void DTX::ParallelUndoLog() {
  // Write the old data from read write set
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be
      // recorded
      log_size += DataItemSize;
    }
  }
  char* written_log_buf = thread_rdma_buffer_alloc->Alloc(log_size);

  offset_t cur = 0;
  *((tx_id_t*)(written_log_buf + cur)) = tx_id;
  cur += sizeof(tx_id);
  *((t_id_t*)(written_log_buf + cur)) = t_id;
  cur += sizeof(t_id);

  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      memcpy(written_log_buf + cur, (char*)(set_it.item_ptr.get()),
             DataItemSize);
      cur += DataItemSize;
      set_it.is_logged = true;
    }
  }

  // Write undo logs to all memory nodes
  // for (int i = 0; i < global_meta_man->remote_nodes.size(); i++) {
  //   offset_t log_offset =
  //       thread_remote_log_offset_alloc->GetNextLogOffset(i, log_size);
  //   RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(i);
  //   coro_sched->RDMALog(coro_id, tx_id, qp, written_log_buf, log_offset,
  //                       log_size);
  // }
}

void DTX::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if
  // no hardware failure occurs
  char* unlock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
  *((lock_t*)unlock_buf) = 0;
  for (auto& index : locked_rw_set) {
    auto& it = read_write_set[index].item_ptr;
    node_id_t primary_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    RCQP* primary_qp =
        thread_qp_man->GetRemoteDataQPWithNodeID(primary_node_id);
    auto rc = primary_qp->post_send(IBV_WR_RDMA_WRITE, unlock_buf,
                                    sizeof(lock_t), it->GetRemoteLockAddr(), 0);
    if (rc != SUCC) {
      RDMA_LOG(FATAL) << "Thread " << t_id << " , Coroutine " << coro_id
                      << " unlock fails during abortion";
    }
  }
  tx_status = TXStatus::TX_ABORT;
}