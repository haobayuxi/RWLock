

#include "dtx.h"

bool DTX::CheckReadRO(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_ro,
                      std::list<HashRead>& pending_next_hash_ro,
                      coro_yield_t& yield) {
  if (!CheckDirectRO(pending_direct_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_next_hash_ro)) return false;

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (!pending_next_hash_ro.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckNextHashRO(pending_next_hash_ro)) return false;
  }

  return true;
}

ALWAYS_INLINE
bool DTX::CheckDirectRO(std::vector<DirectRead>& pending_direct_ro) {
  // check if the tuple has been wlocked
  int len = pending_direct_ro.size();
  for (int i = 0; i < len; i++) {
    // auto* it = res.item->item_ptr.get();
    // pending_direct_ro[i].item->is_fetched = true;
    auto* lock = (lock_t*)pending_direct_ro[i].buf;
    if (*lock != 0) {
      RDMA_LOG(INFO) << "lock";
      return false;
    }
  }
  //   pending_direct_ro.clear();
  return true;
}

bool DTX::CheckHashRO(std::vector<HashRead>& pending_hash_ro,
                      std::list<HashRead>& pending_next_hash_ro) {
  // Check results from hash read
  //   return true;
  for (auto& res : pending_hash_ro) {
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
        // RDMA_LOG(INFO) << "find key" << it->key;
        break;
      }
    }
    if (likely(find)) {
      if (unlikely(it->lock == W_LOCKED)) {
        return false;
      }
      //   else {
      //     // if write, cas read to lock and get the data
      //     if (res.op == OP::Write) {
      //       // After getting address, use CAS + READ
      //       RDMA_LOG(INFO) << "hash read wirt";
      //       char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
      //       char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
      //       pending_cas.emplace_back(CasRead{.qp = res.qp,
      //                                        .item = res.item,
      //                                        .cas_buf = cas_buf,
      //                                        .data_buf = data_buf,
      //                                        .primary_node_id =
      //                                        res.remote_node});
      //       if (!coro_sched->RDMACAS(coro_id, res.qp, cas_buf,
      //                                it->GetRemoteLockAddr(it->remote_offset),
      //                                0, tx_id)) {
      //         return false;
      //       }
      //       if (!coro_sched->RDMARead(coro_id, res.qp, data_buf,
      //                                 it->remote_offset, DataItemSize)) {
      //         return false;
      //       }
      //     }
      //   }
    } else {
      if (local_hash_node->next == nullptr) return false;
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_ro.emplace_back(HashRead{.qp = res.qp,
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
  //   pending_hash.clear();
  return true;
}

bool DTX::CheckNextHashRO(std::list<HashRead>& pending_next_hash_ro) {
  for (auto iter = pending_next_hash_ro.begin();
       iter != pending_next_hash_ro.end(); iter++) {
    auto res = *iter;
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
        break;
      }
    }
    if (likely(find)) {
      if (unlikely(it->lock == W_LOCKED)) {
        return false;
      }
      iter = pending_next_hash_ro.erase(iter);
    } else {
      if (local_hash_node->next == nullptr) return false;
      // Not found, we need to re-read the next bucket
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_ro.emplace_back(HashRead{.qp = res.qp,
                                                 .item = res.item,
                                                 .buf = res.buf,
                                                 .remote_node = res.remote_node,
                                                 .meta = res.meta});
      if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                sizeof(HashNode)))
        return false;
    }
  }
  return true;
}
