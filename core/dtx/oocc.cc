#include "dtx.h"

bool DTX::OOCC(coro_yield_t& yield) {
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
  auto res = CheckReadRO(yield);
  return res;
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
