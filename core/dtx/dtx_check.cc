

#include "dtx.h"

bool DTX::CheckReadRO(std::vector<DirectRead>& pending_direct_ro,
                      std::vector<HashRead>& pending_hash_ro,
                      std::list<InvisibleRead>& pending_invisible_ro,
                      std::list<HashRead>& pending_next_hash_ro,
                      coro_yield_t& yield) {
  if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro,
                     pending_next_hash_ro))
    return false;
  if (!CheckHashRO(pending_hash_ro, pending_invisible_ro, pending_next_hash_ro))
    return false;

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  // if (!lease_expired) {
  while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckInvisibleRO(pending_invisible_ro)) return false;
    if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
      return false;
  }
  // }

  return true;
}