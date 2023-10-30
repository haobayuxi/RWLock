#include "dtx.h"

int expire[100] = {0};

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

// bool DTX::CheckCAS() {
//   // check if w locked
//   for (auto& res : pending_cas) {
//     // auto* it = res.item->item_ptr.get();
//     res.item->is_fetched = true;
//     if (res.op == OP::Write) {
//       auto cas = (uint64_t)*res.cas_buf;
//       if (cas != tx_id) return false;
//     }
//   }
//   //   pending_cas.clear();
//   return true;
// }

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
