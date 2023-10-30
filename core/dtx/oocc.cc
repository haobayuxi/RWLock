#include "dtx.h"

int expire[100] = {0};

bool DTX::CasWriteLockAndRead(coro_yield_t& yield) {
  // may contain read only set
  if (read_only_set.size() != 0) {
  }
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
      pending_cas.emplace_back(CasRead{.qp = qp,
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
      pending_hash.emplace_back(HashRead{.qp = qp,
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

// bool DTX::OOCCCheck(coro_yield_t& yield) {
//   if (!CheckDirectRO()) return false;
//   if (!CheckHash()) return false;

//   // During results checking, we may re-read data due to invisibility and
//   hash
//   // collisions
//   while (unlikely(!pending_next_hash.empty() || !pending_cas.empty())) {
//     RDMA_LOG(INFO) << "while fail";
//     coro_sched->Yield(yield, coro_id);
//     if (!CheckCAS()) return false;
//     if (!CheckNextHash()) return false;
//   }

//   return true;
// }

bool DTX::CheckCAS() {
  // check if w locked
  for (auto& res : pending_cas) {
    // auto* it = res.item->item_ptr.get();
    res.item->is_fetched = true;
    if (res.op == OP::Write) {
      auto cas = (uint64_t)*res.cas_buf;
      if (cas != tx_id) return false;
    }
  }
  //   pending_cas.clear();
  return true;
}

////////
/////////
/////////validate

bool DTX::Validate(coro_yield_t& yield) {
  // The transaction is read-write, and all the written data have
  // been locked before
  // validate the reads
  std::vector<ValidateRead> pending_validate;

  // For read-only items, we only need to read their versions
  for (auto& set_it : read_only_set) {
    auto it = set_it.item_ptr;
    // RDMA_LOG(INFO) << "validate key = " << it->key << "node "
    //                << it->remote_offset;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(set_it.read_which_node);
    char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
    pending_validate.push_back(ValidateRead{.qp = qp,
                                            .item = &set_it,
                                            .cas_buf = nullptr,
                                            .version_buf = version_buf,
                                            .has_lock_in_validate = false});
    if (!coro_sched->RDMARead(coro_id, qp, version_buf,
                              it->GetRemoteVersionAddr(), sizeof(version_t))) {
      RDMA_LOG(INFO) << "read version fail";
      return false;
    }
  }
  // Yield to other coroutines when waiting for network replies
  coro_sched->Yield(yield, coro_id);
  auto res = CheckValidate(pending_validate);
  if (!res) {
    RDMA_LOG(INFO) << "check version fail";
  }
  return res;
}

bool DTX::CheckValidate(std::vector<ValidateRead>& pending_validate) {
  for (auto& re : pending_validate) {
    auto it = re.item->item_ptr;
    // Compare version
    if (it->version != *((version_t*)re.version_buf)) {
      RDMA_LOG(INFO) << it->version << "  " << *((version_t*)re.version_buf);
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

  offset_t log_offset =
      thread_remote_log_offset_alloc->GetNextLogOffset(0, log_size);
  RCQP* qp = thread_qp_man->GetRemoteLogQPWithNodeID(0);
  coro_sched->RDMALog(coro_id, tx_id, qp, written_log_buf, log_offset,
                      log_size);
}
