
#include "rwbench.h"

bool rwbench::read_lock(int addr) {
  if (type == RWLOCK_TYPE::Lease) {
  } else if (type == RWLOCK_TYPE::OCC) {
    // validate
  }
  return true;
}
bool rwbench::read_unlock(int addr, int version, long long end_time) {
  if (type == RWLOCK_TYPE::Lease) {
    // check lease
    if (likely((end_time - start_time) <= lease)) {
      return true;
    }
    // validate
    if (unlikely(version != lease_rwlock_data[addr].version)) {
      return false;
    }
  } else if (type == RWLOCK_TYPE::OCC) {
    // validate
    if (unlikely(version != occ_data[addr].version)) {
      return false;
    }
  }
  return true;
}
bool rwbench::write_lock(int addr) {
  // cas
  bool cmp = 0;
  //   if (type == RWLOCK_TYPE::Lease) {
  //     if (lease_rwlock_data[addr].wlock.compare_exchange_strong(false, true))
  //     {
  //       return true;
  //     }
  //   } else if (type == RWLOCK_TYPE::OCC) {
  //     occ_data[addr].wlock.compare_exchange_strong();
  //   }
  return true;
}
bool rwbench::write_unlock(int addr) {
  // cas
  //   if (type == RWLOCK_TYPE::Lease) {
  //     lease_rwlock_data[addr].wlock.compare_exchange_stron();
  //   } else if (type == RWLOCK_TYPE::OCC) {
  //     occ_data[addr].wlock.compare_exchange_strong();
  //   }
  return true;
}

void rwbench::run() {
  //
  auto addr1 = FastRand(&seed);
  auto addr2 = FastRand(&seed);
  auto readonly = true;

  if (readonly) {
    // read only

  } else {
    // write
  }
}