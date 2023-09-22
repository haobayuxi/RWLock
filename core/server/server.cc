
#include "server.h"

#include <stdio.h>

int main(int argc, char* argv[]) {
  auto times = 10000000;
  auto start = rdtsc_s();
  // start
  for (auto i = 0; i < times; i++) {
  }
  // end
  auto end = rdtsc_e();
  printf("test %ld\n", end - start);
}