

#include "dtx.h"

bool DTX::CheckDirectRO(std::vector<DirectRead>& pending_direct_ro) {
  // check if the tuple has been wlocked
  for (auto& res : pending_direct_ro) {
    // auto* it = res.item->item_ptr.get();
    auto* fetched_item = (DataItem*)res.buf;
    if (fetched_item->lock == W_LOCKED) return false;
  }

  return true;
}

bool DTX::CheckHashRO(std::vector<HashRead>& pending_hash_ro,
                      std::list<HashRead>& pending_next_hash_ro) {
  // Check results from hash read
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
        break;
      }
    }
    if (likely(find)) {
      if (unlikely(it->lock == W_LOCKED)) {
        return false;
      } else {
        if (local_hash_node->next == nullptr) return false;
        // Not found, we need to re-read the next bucket
        auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                        res.meta.base_off;
        pending_next_hash_ro.emplace_back(
            HashRead{.qp = res.qp,
                     .item = res.item,
                     .buf = res.buf,
                     .remote_node = res.remote_node,
                     .meta = res.meta});
        if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                  sizeof(HashNode)))
          return false;
      }
    }
  }
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
      } else {
        if (local_hash_node->next == nullptr) return false;
        // Not found, we need to re-read the next bucket
        auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                        res.meta.base_off;
        pending_next_hash_ro.emplace_back(
            HashRead{.qp = res.qp,
                     .item = res.item,
                     .buf = res.buf,
                     .remote_node = res.remote_node,
                     .meta = res.meta});
        if (!coro_sched->RDMARead(coro_id, res.qp, res.buf, node_off,
                                  sizeof(HashNode)))
          return false;
      }
    }
  }
  return true;
}

bool DTX::CheckReadRO(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_ro,
                      std::list<HashRead>& pending_next_hash_ro,
                      coro_yield_t& yield) {
  return true;
  if (!CheckDirectRO(pending_direct_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_next_hash_ro)) return false;

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (unlikely(!pending_next_hash_ro.empty())) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckNextHashRO(pending_next_hash_ro)) return false;
  }

  return true;
}

bool DTX::CheckCASRO(std::vector<CasRead>& pending_cas_ro,
                     std::vector<HashRead>& pending_hash_ro,
                     std::list<HashRead>& pending_next_hash_ro,
                     coro_yield_t& yield) {
  if (!CheckCASRead(pending_cas_ro)) return false;
  if (!CheckHashRO(pending_hash_ro, pending_next_hash_ro)) return false;

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (!pending_next_hash_ro.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckNextHashRO(pending_next_hash_ro)) return false;
  }

  return true;
}

bool DTX::CheckCASRead(std::vector<CasRead>& pending_cas_ro) {
  std::vector<CasRead> pending_next_cas;
  for (auto& res : pending_cas_ro) {
    auto lock = (uint64_t)res.cas_buf;
    auto lease = lock >> 1;
    if (!cas_lease_expired(lease)) {
      // lease expired
      //   pending_next_cas.emplace_back()
    }
    if (lock % 2 == 1) return false;
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