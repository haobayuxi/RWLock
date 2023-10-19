

enum RWLOCK_TYPE : int {
  Lease = 0,
  OCC = 1,
  TwoPLSF = 2,
};

enum BENCH_TYPE : int {
  RwBench = 0,
  BPlusTree = 1,
};

#define ALWAYS_INLINE inline __attribute__((always_inline))

ALWAYS_INLINE
long long get_clock_sys_time_us() {
  struct timespec tp;
  long long time_us = 0;

  clock_gettime(CLOCK_MONOTONIC, &tp);
  time_us = (long long)tp.tv_sec * 1000000 + tp.tv_nsec / 1000;

  return time_us;
}

ALWAYS_INLINE
long long get_clock_sys_time_ns() {
  struct timespec tp;
  clock_gettime(CLOCK_MONOTONIC, &tp);
  return tp.tv_nsec;
}

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)

// Generate random number for workload testing
static ALWAYS_INLINE uint32_t FastRand(uint64_t* seed) {
  *seed = *seed * 1103515245 + 12345;
  return (uint32_t)(*seed >> 32);
}