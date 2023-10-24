#include "dtx.h"

int read_only_lease = 1000;   // us
int read_write_leaase = 400;  // us

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
      auto expired_time = start_time + read_only_lease;
      auto swap = expired_time << 1;
      if (!coro_sched->RDMACAS(coro_id, qp, cas_buf,
                               it->GetRemoteLockAddr(offset), 0, swap)) {
        return false;
      }
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
  auto res =
      CheckCASRO(pending_cas_rw, pending_hash_ro, pending_next_hash_ro, yield);

  return true;
}

// bool DTX::DrtmCheckHashRead(coro_yield_t& yield) {
//   // check if wlocked
// }

bool DTX::DrtmCheckCas(coro_yield_t& yield) {
  // check cas
  std::vector<CasRead> pending_next_cas;
  for (auto& res : pending_cas_ro) {
    auto lock = (uint64_t)res.cas_buf;
    auto lease = lock >> 1;
    if (!cas_lease_expired(lease)) {
      // lease expired, retry to get read lock

      // pending_next_cas.emplace_back()
    }
    if (lock % 2 == 1) {
      // write locked
      return false;
    }
  }
  return true;
}

bool DTX::cas_lease_expired(uint64_t lease) {
  auto now = get_clock_sys_time_us();
  if (lease > now) {
    return true;
  } else {
    return false;
  }
}