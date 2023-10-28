// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <list>

#include "base/common.h"
#include "rlib/rdma_ctrl.hpp"
#include "scheduler/coroutine.h"

using namespace rdmaio;

// Scheduling coroutines. Each txn thread only has ONE scheduler
class CoroutineScheduler {
 public:
  // The coro_num includes all the coroutines
  CoroutineScheduler(t_id_t thread_id, coro_id_t coro_num) {
    t_id = thread_id;
    pending_counts = new int[coro_num];
    pending_log_counts = new int[coro_num];
    for (coro_id_t c = 0; c < coro_num; c++) {
      pending_counts[c] = 0;
      pending_log_counts[c] = 0;
    }
    coro_array = new Coroutine[coro_num];
  }
  ~CoroutineScheduler() {
    if (pending_counts) delete[] pending_counts;
    if (pending_log_counts) delete[] pending_log_counts;
    if (coro_array) delete[] coro_array;
  }

  // For RDMA requests
  void AddPendingQP(coro_id_t coro_id, RCQP* qp);

  void AddPendingLogQP(coro_id_t coro_id, RCQP* qp);

  bool RDMABatch(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr,
                 ibv_send_wr** bad_sr_addr, int doorbell_num);

  bool RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr,
                     ibv_send_wr** bad_sr_addr, int doorbell_num);

  bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data,
                 uint64_t remote_offset, size_t size);

  bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data,
                 uint64_t remote_offset, size_t size, MemoryAttr& local_mr,
                 MemoryAttr& remote_mr);

  bool RDMAWriteSync(coro_id_t coro_id, RCQP* qp, char* wt_data,
                     uint64_t remote_offset, size_t size);

  bool RDMAWriteSync(coro_id_t coro_id, RCQP* qp, char* wt_data,
                     uint64_t remote_offset, size_t size, MemoryAttr& local_mr,
                     MemoryAttr& remote_mr);

  bool RDMALog(coro_id_t coro_id, tx_id_t tx_id, RCQP* qp, char* wt_data,
               uint64_t remote_offset, size_t size);

  bool RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data,
                uint64_t remote_offset, size_t size);

  bool RDMAReadInv(coro_id_t coro_id, RCQP* qp, char* rd_data,
                   uint64_t remote_offset, size_t size);

  bool RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data,
                    uint64_t remote_offset, size_t size);

  bool RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf,
               uint64_t remote_offset, uint64_t compare, uint64_t swap);
  bool RDMAFAA(coro_id_t coro_id, RCQP* qp, char* local_buf,
               uint64_t remote_offset, uint64_t add_value);

  // For polling
  void PollCompletion();  // There is a coroutine polling ACKs

  void PollRegularCompletion();

  void PollLogCompletion();

  bool CheckLogAck(coro_id_t c_id);

  // Link coroutines in a loop manner
  void LoopLinkCoroutine(coro_id_t coro_num);

  // For coroutine yield, used by transactions
  void Yield(coro_yield_t& yield, coro_id_t cid);

  // Append this coroutine to the tail of the yield-able coroutine list
  // Used by coroutine 0
  void AppendCoroutine(Coroutine* coro);

  // Start this coroutine. Used by coroutine 0
  void RunCoroutine(coro_yield_t& yield, Coroutine* coro);

 public:
  Coroutine* coro_array;

  Coroutine* coro_head;

  Coroutine* coro_tail;

 private:
  t_id_t t_id;

  std::list<RCQP*> pending_qps;

  std::list<RCQP*> pending_log_qps;

  // number of pending qps (i.e., the ack has not received) per coroutine
  int* pending_counts;

  // number of pending log qps (i.e., the ack has not received) per coroutine
  int* pending_log_counts;
};

ALWAYS_INLINE
void CoroutineScheduler::AddPendingQP(coro_id_t coro_id, RCQP* qp) {
  pending_qps.push_back(qp);
  pending_counts[coro_id] += 1;
}

ALWAYS_INLINE
void CoroutineScheduler::AddPendingLogQP(coro_id_t coro_id, RCQP* qp) {
  pending_log_qps.push_back(qp);
  pending_log_counts[coro_id] += 1;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMABatch(coro_id_t coro_id, RCQP* qp,
                                   ibv_send_wr* send_sr,
                                   ibv_send_wr** bad_sr_addr,
                                   int doorbell_num) {
  send_sr[doorbell_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post batch fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMABatchSync(coro_id_t coro_id, RCQP* qp,
                                       ibv_send_wr* send_sr,
                                       ibv_send_wr** bad_sr_addr,
                                       int doorbell_num) {
  send_sr[doorbell_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post batch fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll batch fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data,
                                   uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset,
                          IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWriteSync(coro_id_t coro_id, RCQP* qp,
                                       char* wt_data, uint64_t remote_offset,
                                       size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset,
                          IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data,
                                   uint64_t remote_offset, size_t size,
                                   MemoryAttr& local_mr,
                                   MemoryAttr& remote_mr) {
  auto rc =
      qp->post_send_to_mr(local_mr, remote_mr, IBV_WR_RDMA_WRITE, wt_data, size,
                          remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWriteSync(coro_id_t coro_id, RCQP* qp,
                                       char* wt_data, uint64_t remote_offset,
                                       size_t size, MemoryAttr& local_mr,
                                       MemoryAttr& remote_mr) {
  auto rc =
      qp->post_send_to_mr(local_mr, remote_mr, IBV_WR_RDMA_WRITE, wt_data, size,
                          remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc
                    << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMALog(coro_id_t coro_id, tx_id_t tx_id, RCQP* qp,
                                 char* wt_data, uint64_t remote_offset,
                                 size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset,
                          IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(FATAL) << "client: post log fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id << ", txid = " << tx_id;
    return false;
  }
  AddPendingLogQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data,
                                  uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset,
                          IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(INFO) << "client: post read fail. rc=" << rc << ", tid = " << t_id
                   << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadInv(coro_id_t coro_id, RCQP* qp, char* rd_data,
                                     uint64_t remote_offset, size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, 0, 0);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadSync(coro_id_t coro_id, RCQP* qp,
                                      char* rd_data, uint64_t remote_offset,
                                      size_t size) {
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset,
                          IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf,
                                 uint64_t remote_offset, uint64_t compare,
                                 uint64_t swap) {
  auto rc = qp->post_cas(local_buf, remote_offset, compare, swap,
                         IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAFAA(coro_id_t coro_id, RCQP* qp, char* local_buf,
                                 uint64_t remote_offset, uint64_t add_value) {
  auto rc = qp->post_faa(local_buf, remote_offset, add_value, IBV_SEND_SIGNALED,
                         coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id
                    << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

// Link coroutines in a loop manner
ALWAYS_INLINE
void CoroutineScheduler::LoopLinkCoroutine(coro_id_t coro_num) {
  // The coroutines are maintained in an array,
  // but linked via pointers for efficient yield scheduling
  for (uint i = 0; i < coro_num; ++i) {
    coro_array[i].prev_coro = coro_array + i - 1;
    coro_array[i].next_coro = coro_array + i + 1;
  }
  coro_head = &(coro_array[0]);
  coro_tail = &(coro_array[coro_num - 1]);
  coro_array[0].prev_coro = coro_tail;
  coro_array[coro_num - 1].next_coro = coro_head;
}

// For coroutine yield, used by transactions
ALWAYS_INLINE
void CoroutineScheduler::Yield(coro_yield_t& yield, coro_id_t cid) {
  if (unlikely(pending_counts[cid] == 0)) {
    return;
  }
  // 1. Remove this coroutine from the yield-able coroutine list
  Coroutine* coro = &coro_array[cid];
  assert(coro->is_wait_poll == false);
  Coroutine* next = coro->next_coro;
  coro->prev_coro->next_coro = next;
  next->prev_coro = coro->prev_coro;
  if (coro_tail == coro) coro_tail = coro->prev_coro;
  coro->is_wait_poll = true;
  // 2. Yield to the next coroutine
  // RDMA_LOG(INFO) << "coro: " << cid << " yields to coro " << next->coro_id;
  RunCoroutine(yield, next);
}

// Start this coroutine. Used by coroutine 0 and Yield()
ALWAYS_INLINE
void CoroutineScheduler::RunCoroutine(coro_yield_t& yield, Coroutine* coro) {
  // RDMA_LOG(DBG) << "yield to coro: " << coro->coro_id;
  coro->is_wait_poll = false;
  yield(coro->func);
}

// Append this coroutine to the tail of the yield-able coroutine list. Used by
// coroutine 0
ALWAYS_INLINE
void CoroutineScheduler::AppendCoroutine(Coroutine* coro) {
  if (!coro->is_wait_poll) return;
  Coroutine* prev = coro_tail;
  prev->next_coro = coro;
  coro_tail = coro;
  coro_tail->next_coro = coro_head;
  coro_tail->prev_coro = prev;
}
ALWAYS_INLINE
void CoroutineScheduler::PollRegularCompletion() {
  for (auto it = pending_qps.begin(); it != pending_qps.end();) {
    RCQP* qp = *it;
    struct ibv_wc wc;
    auto poll_result = qp->poll_send_completion(wc);  // The qp polls its own wc
    if (poll_result == 0) {
      RDMA_LOG(INFO) << "not recv";
      sleep(1);
      it++;
      continue;
    }
    RDMA_LOG(INFO) << " recv";
    if (unlikely(wc.status != IBV_WC_SUCCESS)) {
      RDMA_LOG(INFO) << "Bad completion status: " << wc.status << " with error "
                     << ibv_wc_status_str(wc.status) << ";@ node "
                     << qp->idx_.node_id;
      if (wc.status != IBV_WC_RETRY_EXC_ERR) {
        RDMA_LOG(EMPH) << "completion status != IBV_WC_RETRY_EXC_ERR. abort()";
        abort();
      } else {
        it++;
        continue;
      }
    }
    auto coro_id = wc.wr_id;
    RDMA_LOG(INFO) << "coro id= " << coro_id;
    if (coro_id == 0) continue;
    assert(pending_counts[coro_id] > 0);
    pending_counts[coro_id] -= 1;
    if (pending_counts[coro_id] == 0) {
      AppendCoroutine(&coro_array[coro_id]);
    }
    it = pending_qps.erase(it);
  }
}
ALWAYS_INLINE
void CoroutineScheduler::PollLogCompletion() {
  for (auto it = pending_log_qps.begin(); it != pending_log_qps.end();) {
    RCQP* qp = *it;
    struct ibv_wc wc;
    auto poll_result = qp->poll_send_completion(wc);
    if (poll_result == 0) {
      it++;
      continue;
    }
    if (unlikely(wc.status != IBV_WC_SUCCESS)) {
      RDMA_LOG(EMPH) << "Bad completion status: " << wc.status << " with error "
                     << ibv_wc_status_str(wc.status) << ";@ node "
                     << qp->idx_.node_id;
      if (wc.status != IBV_WC_RETRY_EXC_ERR) {
        RDMA_LOG(EMPH) << "completion status != IBV_WC_RETRY_EXC_ERR. abort()";
        abort();
      } else {
        it++;
        continue;
      }
    }
    auto coro_id = wc.wr_id;
    if (coro_id == 0) continue;
    assert(pending_log_counts[coro_id] > 0);
    pending_log_counts[coro_id] -= 1;
    it = pending_log_qps.erase(it);
  }
}
ALWAYS_INLINE
void CoroutineScheduler::PollCompletion() {
  PollRegularCompletion();
  PollLogCompletion();
}
ALWAYS_INLINE
bool CoroutineScheduler::CheckLogAck(coro_id_t c_id) {
  if (pending_log_counts[c_id] == 0) {
    return true;
  }
  PollLogCompletion();
  return pending_log_counts[c_id] == 0;
}