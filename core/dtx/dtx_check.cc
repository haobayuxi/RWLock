

#include "dtx.h"

bool DTX::CheckDirectRO(std::vector<DirectRead>& pending_direct_ro) {
  // check if the tuple has been wlocked
  for (auto& res : pending_direct_ro) {
    auto* it = res.item->item_ptr.get();
    auto* fetched_item = (DataItem*)res.buf;
    if (fetched_item->lock == W_LOCKED) return false;
  }

  return true;
}

bool DTX::CheckHashRO(std::vector<HashRead>& pending_hash_ro,
                      std::list<HashRead>& pending_next_hash_ro) {
  return true;
}

bool DTX::CheckNextHashRO(std::list<HashRead>& pending_next_hash_ro) {
  return true;
}

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