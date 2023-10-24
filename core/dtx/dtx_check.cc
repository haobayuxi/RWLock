

#include "dtx.h"

bool DTX::CheckCASRO(std::vector<CasRead>& pending_cas_ro,
                     std::vector<HashRead>& pending_hash_ro,
                     std::list<HashRead>& pending_next_hash_ro,
                     coro_yield_t& yield) {
  if (!CheckCASRead(pending_cas_ro)) return false;
  // if (!CheckHashRO(pending_hash_ro, pending_next_hash_ro)) return false;

  // During results checking, we may re-read data due to invisibility and hash
  // collisions
  while (!pending_next_hash_ro.empty()) {
    coro_sched->Yield(yield, coro_id);
    if (!CheckNextHashRO(pending_next_hash_ro)) return false;
  }

  return true;
}

bool DTX::CheckCASRead(std::vector<CasRead>& pending_cas_ro) { return true; }
