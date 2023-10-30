#include "dtx.h"

bool DTX::ReadOnly(coro_yield_t& yield) {
  //   bool read_only = read_write_set.empty();
  if (start_time == 0) {
    start_time = get_clock_sys_time_us();
  }
  std::vector<DirectRead> pending_direct_ro;
  std::vector<HashRead> pending_hash_ro;
  if (!IssueReadRO(pending_direct_ro, pending_hash_ro)) return false;

  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);

  // Receive data
  std::list<HashRead> pending_next_hash_ro;
  // RDMA_LOG(DBG) << "coro: " << coro_id << " tx_id: " << tx_id << " check read
  // ro";
  auto res = CheckReadRO(pending_direct_ro, pending_hash_ro,
                         pending_next_hash_ro, yield);
  return res;
}

bool DTX::IssueReadRO(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_ro) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    item.read_which_node = remote_node_id;
    auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    // auto offset = it->key;
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      pending_direct_ro.emplace_back(DirectRead{.qp = qp,
                                                .item = &item,
                                                .buf = data_buf,
                                                .remote_node = remote_node_id});
      if (unlikely(!coro_sched->RDMARead(coro_id, qp, data_buf, offset,
                                         DataItemSize))) {
        return false;
      }
    } else {
      // hash read
      // RDMA_LOG(INFO) << "hash read";
      HashMeta meta =
          global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char* local_hash_node = thread_rdma_buffer_alloc->Alloc(sizeof(HashNode));
      pending_hash.emplace_back(HashRead{.qp = qp,
                                         .item = &item,
                                         .buf = local_hash_node,
                                         .remote_node = remote_node_id,
                                         .meta = meta,
                                         .op = OP::Read});
      if (!coro_sched->RDMARead(coro_id, qp, local_hash_node, node_off,
                                sizeof(HashNode))) {
        return false;
      }
    }
  }
  return true;
}
