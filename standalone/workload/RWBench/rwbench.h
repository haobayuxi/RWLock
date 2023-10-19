
#include <atomic>
#include <iostream>

#include "../common.h"

struct lease_rwlock_node {
  std::atomic<bool> wlock;
  int data;
};

struct occ_node {
  int version;
  int data;
};

class rwbench {
 public:
  bool readonly();
  bool readwrite();

 private:
  occ_node occ_data[64];
  lease_rwlock_node lease_rwlock_data[64];
  int type;
};

bool rwbench::readonly() {
  // random two addr

  return true;
}

bool rwbench::readwrite() {
  // random two addr
  return true;
}