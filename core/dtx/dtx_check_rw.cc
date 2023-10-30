#include "dtx.h"

bool DTX::CheckReadRORW(std::vector<DirectRead>& pending_direct_ro,
                        std::vector<HashRead>& pending_hash_ro,
                        std::vector<HashRead>& pending_hash_rw,
                        std::vector<CasRead>& pending_cas_rw,
                        std::vector<CasRead>& pending_next_cas_rw,
                        std::list<HashRead>& pending_next_hash_ro,
                        std::list<HashRead>& pending_next_hash_rw,
                        coro_yield_t& yield) {
  // check read-only results
  if (!CheckDirectRO(pending_direct_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_next_hash_ro)) return false;
  // The reason to use separate CheckHashRO and CheckHashRW: We need to compare
  // txid with the fetched id in read-write txn check read-write results
  if (!CheckCasRW(pending_cas_rw)) return false;
  if (!CheckHashRW(pending_hash_rw, pending_next_cas_rw, pending_next_hash_rw))
    return false;
  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (!pending_next_hash_ro.empty() || !pending_next_hash_rw.empty() ||
         !pending_next_cas_rw.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckCasRW(pending_next_cas_rw)) return false;
    // Recheck read-only replies
    if (!CheckNextHashRO(pending_next_hash_ro)) return false;

    // Recheck read-write replies
    if (!CheckNextHashRW(pending_next_hash_rw)) return false;
  }
  return true;
}

bool DTX::CheckCasRW(std::vector<CasRead>& pending_cas_rw) {
  for (auto& re : pending_cas_rw) {
    if (*((lock_t*)re.cas_buf) != 0) {
      return false;
    }
  }
  RDMA_LOG(INFO) << "cas read check";
  pending_cas_rw.clear();
  return true;
}

bool DTX::CheckHashRW(std::vector<HashRead>& pending_hash_rw,
                      std::vector<CasRead>& pending_next_cas_rw,
                      std::list<HashRead>& pending_next_hash_rw) {
  // Check results from hash read
  //   return true;
  for (auto& res : pending_hash_rw) {
    auto* local_hash_node = (HashNode*)res.buf;
    auto* it = res.item->item_ptr.get();
    bool find = false;

    for (auto& item : local_hash_node->data_items) {
      if (item.valid && item.key == it->key && item.table_id == it->table_id) {
        *it = item;
        addr_cache->Insert(res.remote_node, it->table_id, it->key,
                           it->remote_offset);
        res.item->is_fetched = true;
        find = true;
        RDMA_LOG(INFO) << "find key" << it->key;
        break;
      }
    }
    if (likely(find)) {
      if (unlikely(it->lock != 0)) {
        return false;
      } else {
        // After getting address, use CAS + READ
        char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
        char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
        pending_next_cas_rw.emplace_back(
            CasRead{.qp = res.qp,
                    .item = res.item,
                    .cas_buf = cas_buf,
                    .data_buf = data_buf,
                    .primary_node_id = res.remote_node});
        RDMA_LOG(INFO) << "cas read";
        if (!coro_sched->RDMACAS(coro_id, res.qp, cas_buf,
                                 it->GetRemoteLockAddr(it->remote_offset), 0,
                                 tx_id)) {
          return false;
        }
        if (!coro_sched->RDMARead(coro_id, res.qp, data_buf, it->remote_offset,
                                  DataItemSize)) {
          return false;
        }
      }
    } else {
      if (local_hash_node->next == nullptr) return false;
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_rw.emplace_back(HashRead{.qp = res.qp,
                                                 .item = res.item,
                                                 .buf = res.buf,
                                                 .remote_node = res.remote_node,
                                                 .meta = res.meta,
                                                 .op = res.op});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                sizeof(HashNode)))
        return false;
    }
  }
  return true;
}

bool DTX::CheckNextHashRW(std::list<HashRead>& pending_next_hash_rw) {
  return true;
}