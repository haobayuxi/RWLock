
#include "server.h"

#include <stdio.h>
#include <time.h>

int rlock[10] = {0};

long long get_clock_sys_time_us() {
  struct timespec tp;
  long long time_us = 0;

  clock_gettime(CLOCK_MONOTONIC, &tp);
  time_us = (long long)tp.tv_sec * 1000000 + tp.tv_nsec / 1000;

  return time_us;
}

int64_t rwlock_test() {}

int64_t lock_unlock() {}

int main(int argc, char* argv[]) {
  auto times = 10000000;
  auto start = get_clock_sys_time_us();
  // start
  for (auto i = 0; i < 10; i++) {
    // printf("%d ", rlock[i]);
  }
  // end
  auto end = get_clock_sys_time_us();
  printf("test %ld\n", end - start);
}