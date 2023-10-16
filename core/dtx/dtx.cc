// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/dtx.h"

DTX::DTX(MetaManager* meta_man, QPManager* qp_man, t_id_t tid, coro_id_t coroid,
         CoroutineScheduler* sched, RDMABufferAllocator* rdma_buffer_allocator,
         LogOffsetAllocator* remote_log_offset_allocator, int lease) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;
  coro_id = coroid;
  coro_sched = sched;
  global_meta_man = meta_man;
  thread_qp_man = qp_man;
  lease = lease;
  thread_rdma_buffer_alloc = rdma_buffer_allocator;
  tx_status = TXStatus::TX_INIT;

  thread_remote_log_offset_alloc = remote_log_offset_allocator;
  // addr_cache = addr_buf;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;
}

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
  // std::list<InvisibleRead> pending_invisible_ro;
  // std::list<HashRead> pending_next_hash_ro;
  // // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check
  // read
  // // ro";
  // auto res = CheckReadRO(pending_direct_ro, pending_hash_ro,
  //                        pending_invisible_ro, pending_next_hash_ro, yield);
  // return res;
  return true;
}

bool DTX::Drtm(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    // auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    // if (!coro_sched->RDMACAS(coro_id, qp, data_buf, offset, DataItemSize)) {
    //   return false;
    // }
  }
  for (auto& item : read_write_set) {
    // cas lock
  }
  return true;
}

bool DTX::Dlmr(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    // auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    // if (!coro_sched->RDMAFAA(coro_id, qp, data_buf, offset, 1)) {
    //   return false;
    // }
  }
  for (auto& item : read_write_set) {
    // cas lock
  }
  return true;
}

bool DTX::Validate(coro_yield_t& yield) {
  // The transaction is read-write, and all the written data have
  // been locked before
  // validate the reads
  std::vector<ValidateRead> pending_validate;

  if (!IssueRemoteValidate(pending_validate)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  auto res = CheckValidate(pending_validate);
  return res;
}

void DTX::ParallelUndoLog() {
  // Write the old data from read write set
  size_t log_size = sizeof(tx_id) + sizeof(t_id);
  for (auto& set_it : read_write_set) {
    if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
      // For the newly inserted data, the old data are not needed to be recorded
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
  // In general, the transaction will not abort during committing replicas if no
  // hardware failure occurs
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