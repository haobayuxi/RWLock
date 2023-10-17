
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#define ALWAYS_INLINE inline __attribute__((always_inline))

class Server {
 public:
  void gen_threads(int thread_num);

 private:
  int thread_num;
};

ALWAYS_INLINE
long long get_clock_sys_time_us() {
  struct timespec tp;
  long long time_us = 0;

  clock_gettime(CLOCK_MONOTONIC, &tp);
  time_us = (long long)tp.tv_sec * 1000000 + tp.tv_nsec / 1000;

  return time_us;
}