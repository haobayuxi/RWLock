
#include "server.h"

#include <intrin.h>
#include <stdio.h>

#pragma intrinsic(__rdtsc)

int main(int argc, char* argv[]) {
  auto times = 10000000;
  // start
  for (auto i = 0; i < times; i++) {
  }
  // end
  auto t = rtdsc();
  printf("test %ld\n", t);
}