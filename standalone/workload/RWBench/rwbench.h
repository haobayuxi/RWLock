
#include <atomic>
#include <iostream>

#include "../common.h"

struct lease_rwlock_node {
  std::atomic<bool> wlock;
  int version;
  int data;
};

struct occ_node {
  std::atomic<bool> wlock;
  int version;
  int data;
};

occ_node occ_data[64];
lease_rwlock_node lease_rwlock_data[64];

class rwbench {
 public:
  rwbench(int thread_id, int type, int lease) {
    seed = 0xdeadbeef + thread_id;
    type = type;
    lease = lease;
  }
  void start();
  void run();
  bool readonly();
  bool readwrite();
  bool read_lock(int addr);
  bool read_unlock(int addr, int version, long long end_time);
  bool write_lock(int addr);
  bool write_unlock(int addr);

 private:
  uint64_t seed;
  int type;
  long long start_time;
  int lease;
};

bool rwbench::readonly() {
  // random two addr

  return true;
}

bool rwbench::readwrite() {
  // random two addr
  return true;
}

void rwbench::start() { start_time = get_clock_sys_time_ns(); }