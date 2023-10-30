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
  write_ratio = 0;

  hit_local_cache_times = 0;
  miss_local_cache_times = 0;
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

bool DTX::TxExe(coro_yield_t& yield, bool fail_abort) {
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;
  if (read_write_set.empty() && read_only_set.empty()) {
    RDMA_LOG(INFO) << "wrong";
    return true;
  }

  if (global_meta_man->txn_system == DTX_SYS::RWLock ||
      global_meta_man->txn_system == DTX_SYS::OCC) {
    // Run our system
    if (ReadOnly(yield)) {
      return true;
    } else {
      goto ABORT;
    }
  } else if (global_meta_man->txn_system == DTX_SYS::DrTMH) {
    // if (Drtm(yield)) {
    //   return true;
    // } else {
    //   goto ABORT;
    // }
  } else if (global_meta_man->txn_system == DTX_SYS::DLMR) {
    // get read lock

    // get write lock
  } else {
    RDMA_LOG(FATAL) << "NOT SUPPORT SYSTEM ID: " << global_meta_man->txn_system;
  }

  return true;

ABORT:
  if (fail_abort) Abort();
  return false;
}

bool DTX::TxCommit(coro_yield_t& yield) {
  bool commit_stat;

  /*!
    RWLock's commit protocol
    */
  // RDMA_LOG(INFO) << "tx commit" << global_meta_man->txn_system;
  if (global_meta_man->txn_system == DTX_SYS::RWLock) {
    // check lease
    auto end_time = get_clock_sys_time_us();

    if ((end_time - start_time) > lease) {
      // RDMA_LOG(INFO) << "rwlock commit" << end_time - start_time << "lease "
      //                << lease;
      if (!Validate(yield)) {
        goto ABORT;
      }
    }

    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      // check log ack
      while (!coro_sched->CheckLogAck(coro_id)) {
        ;  // wait
      }
      // wait until lease pass
      end_time = get_clock_sys_time_us();
      while ((end_time - wlock_start_time) < lease) {
        end_time = get_clock_sys_time_us();
      }
      // write data and unlock
      commit_data();
      coro_sched->Yield(yield, coro_id);
    }
  } else if (global_meta_man->txn_system == DTX_SYS::OCC) {
    /*
      OCC commit protocol
    */
    // RDMA_LOG(INFO) << "occ commit";
    if (!Validate(yield)) {
      goto ABORT;
    }
    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      while (!coro_sched->CheckLogAck(coro_id)) {
        ;  // wait
      }
      // write data and unlock
      commit_data();
      coro_sched->Yield(yield, coro_id);
    }

  } else if (global_meta_man->txn_system == DTX_SYS::DrTMH) {
    // check lease
    // RDMA_LOG(INFO) << "drtm commit";
    if (!Validate(yield)) {
      goto ABORT;
    }

    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      // Write log

      // write data and unlock
    }
  } else if (global_meta_man->txn_system == DTX_SYS::DLMR) {
    // RDMA_LOG(INFO) << "dlmr commit";
    if (!read_write_set.empty()) {
      // Write log

      // write data and unlock
    }
    // unlock read lock and write lock
  }

  return true;
ABORT:
  Abort();
  return false;
}

bool DTX::commit_data() {
  for (size_t i = 0; i < read_write_set.size(); i++) {
    auto it = read_write_set[i].item_ptr;
    auto remote_node_id = global_meta_man->GetPrimaryNodeID(it->table_id);
    read_write_set[i].read_which_node = remote_node_id;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    auto offset = it->remote_offset;
    locked_rw_set.emplace_back(i);
    // After getting address, use doorbell CAS + READ
    char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    *(lock_t*)cas_buf = 0;
    memcpy(data_buf, (char*)it, sizeof(DataItem));
    // pending_cas.emplace_back(CasRead{.qp = qp,
    //                                  .item = &read_write_set[i],
    //                                  .cas_buf = cas_buf,
    //                                  .data_buf = data_buf,
    //                                  .primary_node_id = remote_node_id});
    if (!coro_sched->RDMAWrite(coro_id, qp, data_buf, offset, DataItemSize)) {
      return false;
    }
    if (!coro_sched->RDMACAS(coro_id, qp, cas_buf,
                             it->GetRemoteLockAddr(offset), 0, tx_id)) {
      return false;
    }
  }
  return true;
}

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
