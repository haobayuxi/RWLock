#include "dtx.h"

bool DTX::ReadWrite(coro_yield_t& yield) {
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;

  // For read-write data from primary
  std::vector<CasRead> pending_cas_rw;
  std::vector<DirectRead> pending_direct_rw;
  std::vector<HashRead> pending_hash_rw;
  //   std::vector<InsertOffRead> pending_insert_off_rw;

  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;
  std::vector<CasRead> pending_next_cas_rw;
  if (start_time == 0) {
    start_time = get_clock_sys_time_us();
  }
  //   std::list<InsertOffRead> pending_next_off_rw;
  // may contain read only set
  if (!IssueReadRO(pending_direct_ro, pending_hash_ro)) return false;
  if (!IssueReadLock(pending_cas_rw, pending_hash_rw)) return false;
  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);
  auto res = CheckReadRORW(pending_direct_ro, pending_hash_ro, pending_hash_rw,
                           pending_cas_rw, pending_next_cas_rw,
                           pending_next_hash_ro, pending_next_hash_rw, yield);
  if (res) {
    ParallelUndoLog();
    IssueLock();
  }
  return res;
}

bool DTX::IssueLock() {
  auto size = read_write_set.size();
  for (size_t i = 0; i < size; i++) {
    auto it = read_write_set[i].item_ptr;
    auto remote_node_id = read_write_set[i].read_which_node;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    auto offset = it->remote_offset;
    char* lock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    *(lock_t*)lock_buf = tx_id;
    if (!coro_sched->RDMAWrite(coro_id, qp, lock_buf, offset, sizeof(lock_t))) {
      return false;
    }
  }
  return true;
}

bool DTX::IssueReadLock(std::vector<CasRead>& pending_cas_rw,
                        std::vector<HashRead>& pending_hash_rw) {
  // For read-write set, we need to read and lock them
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto it = read_write_set[i].item_ptr;
    auto remote_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    read_write_set[i].read_which_node = remote_node_id;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    // Addr cached in local
    if (offset != NOT_FOUND) {
      // hit_local_cache_times++;
      it->remote_offset = offset;
      //   locked_rw_set.emplace_back(i);
      // After getting address, use CAS + READ
      char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      pending_cas_rw.emplace_back(CasRead{.qp = qp,
                                          .item = &read_write_set[i],
                                          .cas_buf = cas_buf,
                                          .data_buf = data_buf,
                                          .primary_node_id = remote_node_id,
                                          .op = OP::Write});
      if (!coro_sched->RDMACAS(coro_id, qp, cas_buf,
                               it->GetRemoteLockAddr(offset), 0, tx_id)) {
        return false;
      }
      if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
        return false;
      }
    } else {
      // Only read
      const HashMeta& meta =
          global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      //   if (it->user_insert) {
      //     pending_insert_off_rw.emplace_back(
      //         InsertOffRead{.qp = qp,
      //                       .item = &read_write_set[i],
      //                       .buf = local_hash_node,
      //                       .remote_node = remote_node_id,
      //                       .meta = meta,
      //                       .node_off = node_off});
      //   } else {
      pending_hash_rw.emplace_back(HashRead{.qp = qp,
                                            .item = &read_write_set[i],
                                            .buf = local_hash_node,
                                            .remote_node = remote_node_id,
                                            .meta = meta,
                                            .op = OP::Write});
      //   }
      if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off,
                                sizeof(HashNode))) {
        return false;
      }
    }
  }
  return true;
}