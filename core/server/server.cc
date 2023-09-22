
#include "server.h"

#include <stdio.h>

int rlock[10] = {0};

int64_t rwlock_test() {}

int64_t lock_unlock() {}

int main(int argc, char* argv[]) {
  auto times = 10000000;
  auto start = rdtsc_s();
  // start
  for (auto i = 0; i < 10; i++) {
    printf("%d ", rlock[i]);
  }
  // end
  auto end = rdtsc_e();
  printf("test %ld\n", end - start);
}